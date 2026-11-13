"""FastAPI 入口：项目选择、DICOM 批次上传、批次列表、整批 ZIP 下载与项目销毁。"""
from __future__ import annotations

import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import db, destroy
from .config import settings
from .dicom import DicomValidationError
from .ids import ID_KINDS
from .pipeline import (
    UploadLimitError,
    UploadedFile,
    process_batch,
    recover_on_startup,
    zip_path,
)

WEB_DIR = Path(__file__).parent / "web"

# SQLite 连接不能跨线程共享；处理线程各自取连接（check_same_thread=False
# 配合“每线程一连接”的使用方式，连接对象不会并发使用）。
_init_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    with _init_lock:
        db.init_db()
        recover_on_startup()
        # 进程重启后续办未完成的销毁（DESTROYING 墓碑 -> 重启销毁线程）
        destroy.resume_on_startup()
    yield


app = FastAPI(
    title="科研影像 DICOM 统一标识入库服务",
    version="1.1.0",
    description="按研究项目隔离的 DICOM UID/PatientID 持久重映射、整批 ZIP 下载与项目销毁",
    lifespan=lifespan,
)


def _get_project_or_404(conn, project_id: str):
    project = db.get_project(conn, project_id)
    if project is None:
        raise HTTPException(status_code=404, detail=f"研究项目不存在: {project_id}")
    return project


def _ensure_not_destroying(conn, project_id: str) -> None:
    if db.is_project_destroying(conn, project_id):
        raise HTTPException(
            status_code=410,
            detail="项目已受理销毁：批次与映射不再可见，上传与下载已关闭",
        )


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/projects")
def get_projects():
    conn = db.connect()
    try:
        return [dict(row) for row in db.list_projects(conn)]
    finally:
        conn.close()


@app.post("/api/projects", status_code=201)
def post_project(name: str = Form(...)):
    name = name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="项目名称不能为空")
    conn = db.connect()
    try:
        row = db.create_project(conn, name)
        if db.is_project_destroying(conn, row["id"]):
            raise HTTPException(
                status_code=409,
                detail="同名项目正在销毁，暂不能复用该名称，请稍后再试",
            )
        return dict(row)
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 项目销毁
# --------------------------------------------------------------------------- #
@app.post("/api/projects/{project_id}/destroy", status_code=202)
def post_destroy(project_id: str):
    """受理项目销毁（幂等）。受理即拒绝新上传/下载并隐藏批次与映射；
    实际删除由后台线程推进，请求断连不影响，重复请求跟随同一次销毁。"""
    with destroy.permit_lock:
        conn = db.connect()
        try:
            existing = db.get_destruction(conn, project_id)
            if existing is None:
                project = db.get_project(conn, project_id)
                if project is None:
                    raise HTTPException(
                        status_code=404, detail=f"研究项目不存在: {project_id}"
                    )
                state = destroy.accept(project_id, project["name"])
            else:
                state = dict(existing)
        finally:
            conn.close()
    # 确保销毁线程在运行（仅销毁中需要；首次受理、失败重试或异常退出后都会被重新拉起）
    if state["status"] == "DESTROYING":
        destroy.ensure_worker(project_id)
    state = destroy.get_state(project_id) or state
    return JSONResponse(status_code=202, content=state)


@app.get("/api/projects/{project_id}/destroy")
def get_destroy(project_id: str):
    state = destroy.get_state(project_id)
    if state is None:
        raise HTTPException(status_code=404, detail="该项目没有销毁记录")
    return state


@app.get("/api/destructions")
def get_destructions():
    return destroy.list_states()


# --------------------------------------------------------------------------- #
# 批次上传
# --------------------------------------------------------------------------- #
@app.post("/api/batches")
async def create_batch(
    project_id: str = Form(...),
    files: list[UploadFile] = File(..., description="最多 20 份、合计 ≤16 MiB 的未压缩 DICOM"),
):
    conn = db.connect()
    try:
        _get_project_or_404(conn, project_id)
        _ensure_not_destroying(conn, project_id)  # 早失败：避免无谓读取上传体
    finally:
        conn.close()

    # 先在请求层做总量限制，避免超大上传耗尽内存
    payload: list[UploadedFile] = []
    total = 0
    for upload in files:
        content = await upload.read()
        total += len(content)
        if total > settings.max_total_bytes:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"批次合计超过 {settings.max_total_bytes // (1024 * 1024)} MiB 上限，"
                    "整批已拒绝，未写入任何数据"
                ),
            )
        payload.append(UploadedFile(filename=upload.filename or "unnamed.dcm", content=content))

    if len(payload) > settings.max_files:
        raise HTTPException(
            status_code=413,
            detail=f"单批最多 {settings.max_files} 份文件，本次为 {len(payload)} 份",
        )

    # 许可闸门：与“受理销毁”互斥——要么本批次被放行并计入在途转换
    # （销毁线程会等待它退出），要么看到销毁墓碑并被拒绝。
    conn = db.connect()
    try:
        with destroy.permit_lock:
            _ensure_not_destroying(conn, project_id)
            destroy.tracker.begin_conversion(project_id)
    finally:
        conn.close()

    try:
        # 转换为 CPU/文件密集型同步流水线，放到线程池执行，避免阻塞事件循环，
        # 保证多批次 HTTP 请求能真正并发进入保留协议。
        batch_id = await run_in_threadpool(process_batch, project_id, payload)
    except UploadLimitError as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except DicomValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except db.ProjectDestroyedError as exc:
        # 转换期间项目被销毁：保留/发布闸门已拦截，数据已回滚清理
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:  # 防御：任何意外错误都不允许部分落库
        raise HTTPException(status_code=500, detail=f"批次处理失败且已回滚: {exc}") from exc
    finally:
        destroy.tracker.end_conversion(project_id)

    conn = db.connect()
    try:
        row = db.get_batch(conn, batch_id)
        return JSONResponse(status_code=201, content=dict(row))
    finally:
        conn.close()


@app.get("/api/batches")
def get_batches(project_id: str | None = None):
    conn = db.connect()
    try:
        if project_id:
            _get_project_or_404(conn, project_id)
            _ensure_not_destroying(conn, project_id)
        rows = db.list_batches(conn, project_id)
        return [dict(row) for row in rows]
    finally:
        conn.close()


@app.get("/api/batches/{batch_id}")
def get_batch(batch_id: str):
    conn = db.connect()
    try:
        row = db.get_batch(conn, batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        _ensure_not_destroying(conn, row["project_id"])
        result = dict(row)
        result["files"] = [dict(r) for r in db.list_batch_files(conn, batch_id)]
        return result
    finally:
        conn.close()


def _stream_zip(path: Path):
    """分块流式发送 ZIP。

    可选演示钩子 DICOM_REMAP_SLOW_DOWNLOAD_CHUNK_SECONDS 人为放慢下载，
    用于演示“慢下载期间销毁：已获准下载可完成，销毁线程等待其关闭”。
    """
    chunk = max(4096, settings.download_chunk_bytes)
    delay = settings.slow_download_chunk_seconds
    with open(path, "rb") as fh:
        while True:
            data = fh.read(chunk)
            if not data:
                break
            if delay > 0:
                time.sleep(delay)
            yield data


class TrackedStreamingResponse(StreamingResponse):
    """在 ``__call__`` 的 finally 中归还在途下载计数。

    无论正常发完、客户端断连（ClientDisconnect）还是任务被取消，
    ASGI 调用结束都会经过这里，计数释放不依赖生成器 GC 终结。
    """

    def __init__(self, *args, project_id: str, **kwargs):
        super().__init__(*args, **kwargs)
        self._project_id = project_id

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            destroy.tracker.end_download(self._project_id)


@app.get("/api/batches/{batch_id}/download")
def download_batch(batch_id: str):
    conn = db.connect()
    try:
        row = db.get_batch(conn, batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        if row["status"] != "PUBLISHED":
            raise HTTPException(status_code=409, detail=f"批次状态为 {row['status']}，不可下载")
        project_id = row["project_id"]
        # 许可闸门：与“受理销毁”互斥。获准的下载计入在途，销毁线程
        # 会等它关闭后才删除 ZIP；受理后的新下载一律拒绝。
        with destroy.permit_lock:
            if db.is_project_destroying(conn, project_id):
                raise HTTPException(
                    status_code=410, detail="项目已受理销毁，下载已关闭"
                )
            destroy.tracker.begin_download(project_id)
    finally:
        conn.close()

    path = zip_path(batch_id)
    try:
        if not path.is_file():
            # 已发布但文件缺失属于存储事故，明确报错而不是返回空包
            raise HTTPException(status_code=410, detail="发布的 ZIP 文件缺失，请联系管理员")
        headers = {
            "Content-Disposition": f'attachment; filename="remapped-{batch_id}.zip"',
            "Content-Length": str(path.stat().st_size),
        }
        return TrackedStreamingResponse(
            _stream_zip(path),
            project_id=project_id,
            media_type="application/zip",
            headers=headers,
        )
    except Exception:
        # 响应未成功构造：在此归还在途计数（成功返回后由响应对象负责）
        destroy.tracker.end_download(project_id)
        raise


@app.get("/api/mappings")
def get_mappings(project_id: str | None = None):
    conn = db.connect()
    try:
        if project_id:
            _get_project_or_404(conn, project_id)
            _ensure_not_destroying(conn, project_id)
        rows = db.list_mappings(conn, project_id)
        return [dict(row) for row in rows]
    finally:
        conn.close()


@app.get("/api/meta")
def get_meta():
    return {
        "max_files": settings.max_files,
        "max_total_bytes": settings.max_total_bytes,
        "max_total_mib": settings.max_total_bytes / (1024 * 1024),
        "id_kinds": ID_KINDS,
    }


# 静态前端（挂载在最后，避免覆盖 /api 路由）
app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
