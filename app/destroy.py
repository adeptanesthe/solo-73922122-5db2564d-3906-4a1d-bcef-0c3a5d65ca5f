"""项目销毁：受理、在途操作静默等待、物理删除与崩溃恢复。

语义（单 Web 进程模型）：
  * **受理**：``POST /api/projects/{id}/destroy`` 在 :data:`permit_lock` 内登记
    DESTROYING 墓碑并立即返回；销毁由独立后台线程推进，请求断连不影响。
    重复请求（含已 DESTROYED）跟随同一次销毁，幂等返回当前状态。
  * **闸门**：受理后新上传/新下载被拒绝，批次与映射对外不可见；
    受理前开始的转换在发布事务内被拦截（不得发布），已获准的下载可完成。
  * **静默等待**：销毁线程等待该项目在途转换/下载计数归零（条件变量，
    不持任何 SQLite 事务/写锁），随后才删除文件与数据行。
  * **删除**：先删文件（ZIP + 暂存，不持锁），再用一条短事务删除
    批次/映射/项目行并标记 DESTROYED——崩溃于两步之间时，重启按墓碑
    重放整个删除流程（幂等），绝不提前报完成、不留迟到产物。
  * **失败重试**：删除抛错则保持 DESTROYING、记录错误并周期性重试；
    进程退出后重启由 :func:`resume_on_startup` 自动续完。
"""
from __future__ import annotations

import shutil
import threading
import time
from pathlib import Path

from . import db
from .config import settings
from .pipeline import cleanup_staging, zip_path

#: “受理销毁”与“放行新上传/下载”的互斥锁：保证在途计数与墓碑登记无竞态。
permit_lock = threading.Lock()


class ActivityTracker:
    """按项目跟踪在途转换与在途下载数量（单进程内存计数）。

    计数增减与 :func:`accept` 共用 :data:`permit_lock` 定序：
    受理销毁之前被放行的操作一定已计入，受理之后不会再有新操作被放行，
    因此销毁线程只需等待计数归零即可安全删除文件。
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._conversions: dict[str, int] = {}
        self._downloads: dict[str, int] = {}

    @staticmethod
    def _dec(table: dict[str, int], project_id: str) -> None:
        left = table.get(project_id, 0) - 1
        if left <= 0:
            table.pop(project_id, None)
        else:
            table[project_id] = left

    def begin_conversion(self, project_id: str) -> None:
        with self._cond:
            self._conversions[project_id] = self._conversions.get(project_id, 0) + 1

    def end_conversion(self, project_id: str) -> None:
        with self._cond:
            self._dec(self._conversions, project_id)
            self._cond.notify_all()

    def begin_download(self, project_id: str) -> None:
        with self._cond:
            self._downloads[project_id] = self._downloads.get(project_id, 0) + 1

    def end_download(self, project_id: str) -> None:
        with self._cond:
            self._dec(self._downloads, project_id)
            self._cond.notify_all()

    def counts(self, project_id: str) -> tuple[int, int]:
        with self._cond:
            return (
                self._conversions.get(project_id, 0),
                self._downloads.get(project_id, 0),
            )

    def await_quiescent(self, project_id: str) -> None:
        """等待该项目在途转换与下载全部退出。纯内存等待，不持 SQLite 锁。"""
        with self._cond:
            while (
                self._conversions.get(project_id, 0)
                or self._downloads.get(project_id, 0)
            ):
                self._cond.wait(timeout=0.5)


#: 全局在途操作追踪器（路由与销毁线程共享）
tracker = ActivityTracker()

_workers: dict[str, threading.Thread] = {}
_workers_lock = threading.Lock()


def accept(project_id: str, project_name: str) -> dict:
    """受理销毁（幂等）。须在 :data:`permit_lock` 内调用。"""
    conn = db.connect()
    try:
        row = db.accept_destruction(conn, project_id, project_name)
        return dict(row)
    finally:
        conn.close()


def get_state(project_id: str) -> dict | None:
    """销毁进度：墓碑记录 + 实时在途计数。"""
    conn = db.connect()
    try:
        row = db.get_destruction(conn, project_id)
        if row is None:
            return None
        state = dict(row)
    finally:
        conn.close()
    conversions, downloads = tracker.counts(project_id)
    state["pending_conversions"] = conversions
    state["pending_downloads"] = downloads
    return state


def list_states() -> list[dict]:
    conn = db.connect()
    try:
        rows = [dict(r) for r in db.list_destructions(conn)]
    finally:
        conn.close()
    for row in rows:
        conversions, downloads = tracker.counts(row["project_id"])
        row["pending_conversions"] = conversions
        row["pending_downloads"] = downloads
    return rows


def ensure_worker(project_id: str) -> None:
    """确保该项目的销毁线程在运行（重复调用安全）。"""
    with _workers_lock:
        worker = _workers.get(project_id)
        if worker is not None and worker.is_alive():
            return
        worker = threading.Thread(
            target=_destroy_worker,
            args=(project_id,),
            daemon=True,
            name=f"destroy-{project_id[:8]}",
        )
        _workers[project_id] = worker
        worker.start()


def resume_on_startup() -> list[str]:
    """进程重启后续办：为所有 DESTROYING 记录重启销毁线程。

    已 DESTROYED 的项目无需处理（数据已删净，种子守卫保证不重建）。
    """
    conn = db.connect()
    try:
        active = db.list_active_destructions(conn)
    finally:
        conn.close()
    for row in active:
        ensure_worker(row["project_id"])
    return [r["project_id"] for r in active]


def _remove_tree(path: Path) -> None:
    """删除目录树；不存在视为成功，其余错误抛出以触发重试。"""
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def _delete_project_files(project_id: str) -> None:
    """删除项目全部 ZIP 与暂存目录。不持任何数据库事务/写锁。"""
    conn = db.connect()
    try:
        batch_ids = db.batch_ids_for_project(conn, project_id)
    finally:
        conn.close()
    for batch_id in batch_ids:
        zip_path(batch_id).unlink(missing_ok=True)
        _remove_tree(settings.staging_dir / batch_id)


def _finish(project_id: str) -> None:
    conn = db.connect()
    try:
        db.finish_destruction(conn, project_id)
    finally:
        conn.close()


def _read_status(project_id: str) -> str | None:
    conn = db.connect()
    try:
        row = db.get_destruction(conn, project_id)
        return row["status"] if row else None
    finally:
        conn.close()


def _destroy_worker(project_id: str) -> None:
    """销毁线程：等静默 -> 删文件 -> 删行并标记完成；失败记录并重试。"""
    while True:
        try:
            # 已完成（或记录不存在）时立即退出，绝不做无谓重试
            if _read_status(project_id) != "DESTROYING":
                return
            # 1) 等在途转换退出、在途下载关闭（纯内存等待，不持 SQLite 锁）
            tracker.await_quiescent(project_id)
            # 2) 删除项目全部 ZIP 与暂存（文件操作，不持 SQLite 写锁）
            _delete_project_files(project_id)
            # 3) 短事务删除全部行并标记 DESTROYED（此后才允许报完成）
            _finish(project_id)
            return
        except Exception as exc:  # 删除失败：保持 DESTROYING，可重试
            try:
                conn = db.connect()
                try:
                    db.note_destroy_failure(conn, project_id, str(exc))
                finally:
                    conn.close()
            except Exception:
                pass  # 记录失败本身出错不致命，下一轮重试再记
            time.sleep(settings.destroy_retry_seconds)
