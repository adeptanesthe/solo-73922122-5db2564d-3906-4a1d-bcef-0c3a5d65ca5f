"""项目销毁：受理闸门、在途等待、物理删除、幂等、失败重试与重启续办。"""
from __future__ import annotations

import io
import threading
import time
import uuid
import zipfile

import pydicom
import pytest

from tests.conftest import upload


def _await_destroyed(client, project_id: str, timeout: float = 15.0) -> dict:
    """轮询销毁进度直到 DESTROYED；超时失败。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(f"/api/projects/{project_id}/destroy")
        assert resp.status_code == 200
        state = resp.json()
        if state["status"] == "DESTROYED":
            return state
        assert state["status"] == "DESTROYING"
        time.sleep(0.1)
    raise AssertionError("销毁未在时限内完成（不得提前报完成，也不得卡死）")


def test_destroy_full_flow(client, project_id, sample_dicom, tmp_path, monkeypatch):
    from app import destroy
    from app.config import settings

    projects = client.get("/api/projects").json()
    other = [p for p in projects if p["id"] != project_id][0]

    f = sample_dicom(tmp_path / "f.dcm")
    batch = upload(client, project_id, [f]).json()
    assert batch["status"] == "PUBLISHED"
    other_batch = upload(client, other["id"], [sample_dicom(tmp_path / "o.dcm")]).json()
    zip_file = settings.published_dir / f"{batch['id']}.zip"
    assert zip_file.is_file()

    # 让物理删除阻塞在事件上，使“受理后、删除前”的闸门断言确定化
    gate = threading.Event()
    real_delete = destroy._delete_project_files

    def blocked_delete(pid):
        assert gate.wait(timeout=10)
        return real_delete(pid)

    monkeypatch.setattr(destroy, "_delete_project_files", blocked_delete)

    # ---- 受理 ----
    resp = client.post(f"/api/projects/{project_id}/destroy")
    assert resp.status_code == 202
    assert resp.json()["status"] == "DESTROYING"

    # ---- 受理后立即生效的闸门（删除尚未完成，状态仍为 DESTROYING） ----
    r = upload(client, project_id, [sample_dicom(tmp_path / "g.dcm")])
    assert r.status_code in (409, 410), "受理后新上传必须被拒绝"
    r = client.get(f"/api/batches/{batch['id']}/download")
    assert r.status_code == 410, "受理后新下载必须被拒绝"
    assert client.get("/api/batches", params={"project_id": project_id}).status_code == 410
    assert client.get(f"/api/batches/{batch['id']}").status_code == 410
    assert client.get("/api/mappings", params={"project_id": project_id}).status_code == 410
    # 未过滤的全局列表同样不再可见
    assert all(b["project_id"] != project_id for b in client.get("/api/batches").json())
    assert all(m["project_id"] != project_id for m in client.get("/api/mappings").json())
    # 其他项目照常使用
    assert client.get("/api/batches", params={"project_id": other["id"]}).status_code == 200
    assert client.get(f"/api/batches/{other_batch['id']}/download").status_code == 200
    assert client.get("/api/mappings", params={"project_id": other["id"]}).json() != []

    # ---- 放行删除，等待完成 ----
    gate.set()
    state = _await_destroyed(client, project_id)
    assert state["finished_at"]

    # ---- 物理删除：ZIP、暂存、批次、映射、项目行 ----
    assert not zip_file.exists()
    assert not (settings.staging_dir / batch["id"]).exists()
    assert all(p["id"] != project_id for p in client.get("/api/projects").json())
    # 仅保留项目 ID 与销毁状态（墓碑）
    tomb = client.get(f"/api/projects/{project_id}/destroy").json()
    assert tomb["status"] == "DESTROYED"
    listed = client.get("/api/destructions").json()
    assert any(d["project_id"] == project_id and d["status"] == "DESTROYED" for d in listed)


def test_destroy_repeat_requests_follow_same_destruction(client, project_id, sample_dicom, tmp_path):
    upload(client, project_id, [sample_dicom(tmp_path / "f.dcm")])
    r1 = client.post(f"/api/projects/{project_id}/destroy")
    r2 = client.post(f"/api/projects/{project_id}/destroy")
    assert r1.status_code == 202 and r2.status_code == 202
    # 重复请求跟随同一次销毁：同一条墓碑记录
    assert r1.json()["requested_at"] == r2.json()["requested_at"]

    state = _await_destroyed(client, project_id)
    # 完成后再请求仍跟随同一次销毁，不会重新发起
    r3 = client.post(f"/api/projects/{project_id}/destroy")
    assert r3.status_code == 202
    assert r3.json()["status"] == "DESTROYED"
    assert r3.json()["requested_at"] == state["requested_at"]

    # 无记录的项目
    ghost = str(uuid.uuid4())
    assert client.post(f"/api/projects/{ghost}/destroy").status_code == 404
    assert client.get(f"/api/projects/{ghost}/destroy").status_code == 404


def test_destroy_during_slow_conversion_not_published(client, project_id, sample_dicom, tmp_path):
    """慢转换期间受理销毁：在途转换不得发布，退出后数据被删净。"""
    from app import config, destroy
    from app.config import settings

    object.__setattr__(config.settings, "slow_convert_seconds", 0.6)
    try:
        f1 = sample_dicom(tmp_path / "f1.dcm")
        ds1 = pydicom.dcmread(str(f1))
        f2 = sample_dicom(
            tmp_path / "f2.dcm",
            series_uid=f"{ds1.StudyInstanceUID}.2",
            sop_uid=f"{ds1.StudyInstanceUID}.2.1",
        )
        result = {}

        def uploader():
            result["resp"] = upload(client, project_id, [f1, f2])

        t = threading.Thread(target=uploader)
        t.start()
        # 等转换真正在途（计数由路由在许可闸门内登记）
        deadline = time.monotonic() + 5
        while destroy.tracker.counts(project_id)[0] < 1:
            assert time.monotonic() < deadline, "转换未进入在途状态"
            time.sleep(0.05)

        resp = client.post(f"/api/projects/{project_id}/destroy")
        assert resp.status_code == 202
        t.join(timeout=15)
        assert not t.is_alive()

        # 在途转换被发布闸门拦截：不得发布
        assert result["resp"].status_code == 409
        state = _await_destroyed(client, project_id)
        assert state["pending_conversions"] == 0

        # 不留迟到产物：无 ZIP、无暂存、无批次、无映射
        assert list(settings.published_dir.iterdir()) == []
        assert list(settings.staging_dir.iterdir()) == []
        from app import db
        conn = db.connect()
        try:
            assert conn.execute("SELECT COUNT(*) c FROM batches").fetchone()["c"] == 0
            assert conn.execute("SELECT COUNT(*) c FROM id_mappings").fetchone()["c"] == 0
        finally:
            conn.close()
    finally:
        object.__setattr__(config.settings, "slow_convert_seconds", 0.0)


def test_destroy_waits_for_inflight_download(client, project_id, sample_dicom, tmp_path):
    """慢下载期间受理销毁：已获准下载完整完成，关闭后销毁才删文件。"""
    from app import config, destroy
    from app.config import settings

    f = sample_dicom(tmp_path / "f.dcm")
    batch = upload(client, project_id, [f]).json()
    zip_file = settings.published_dir / f"{batch['id']}.zip"

    object.__setattr__(config.settings, "slow_download_chunk_seconds", 0.5)
    try:
        received = {}

        def reader():
            with client.stream("GET", f"/api/batches/{batch['id']}/download") as resp:
                received["status"] = resp.status_code
                received["body"] = b"".join(resp.iter_bytes())

        t = threading.Thread(target=reader)
        t.start()
        # 等下载被放行并计入在途
        deadline = time.monotonic() + 5
        while destroy.tracker.counts(project_id)[1] < 1:
            assert time.monotonic() < deadline, "下载未进入在途状态"
            time.sleep(0.05)

        resp = client.post(f"/api/projects/{project_id}/destroy")
        assert resp.status_code == 202
        # 下载仍在进行时，销毁不得完成（不能提前报完成）
        state = client.get(f"/api/projects/{project_id}/destroy").json()
        assert state["status"] == "DESTROYING"
        assert state["pending_downloads"] >= 1

        t.join(timeout=15)
        assert not t.is_alive()
        # 已获准的下载完整完成
        assert received["status"] == 200
        with zipfile.ZipFile(io.BytesIO(received["body"])) as zf:
            assert zf.namelist() == ["f.dcm"]

        # 下载关闭后销毁推进至完成，ZIP 被删除
        _await_destroyed(client, project_id)
        assert not zip_file.exists()
    finally:
        object.__setattr__(config.settings, "slow_download_chunk_seconds", 0.0)


def test_download_disconnect_releases_tracker(client, project_id, sample_dicom, tmp_path):
    """客户端断连：在途下载计数必须释放（销毁不会被泄漏的计数卡死）。"""
    from app import config, destroy

    batch = upload(client, project_id, [sample_dicom(tmp_path / "f.dcm")]).json()
    object.__setattr__(config.settings, "slow_download_chunk_seconds", 1.0)
    try:
        with client.stream("GET", f"/api/batches/{batch['id']}/download") as resp:
            assert resp.status_code == 200
            # 不读取正文，直接断开
        deadline = time.monotonic() + 8
        while destroy.tracker.counts(project_id)[1] > 0:
            assert time.monotonic() < deadline, "下载断连后在途计数未释放"
            time.sleep(0.05)
    finally:
        object.__setattr__(config.settings, "slow_download_chunk_seconds", 0.0)


def test_destroy_resume_after_restart_and_seed_not_rebuilt(client, project_id, sample_dicom, tmp_path):
    """进程退出后重启自动续完；已销毁项目（含种子项目）不得被重建。"""
    from app import db, destroy
    from app.config import settings
    from app.pipeline import recover_on_startup

    f = sample_dicom(tmp_path / "f.dcm")
    batch = upload(client, project_id, [f]).json()
    zip_file = settings.published_dir / f"{batch['id']}.zip"

    conn = db.connect()
    project = dict(db.get_project(conn, project_id))
    conn.close()

    # 模拟“受理后进程立即退出”：只写墓碑，不启动销毁线程
    conn = db.connect()
    db.accept_destruction(conn, project_id, project["name"])
    conn.close()

    # 重启恢复：自动续完
    resumed = destroy.resume_on_startup()
    assert project_id in resumed
    _await_destroyed(client, project_id)
    assert not zip_file.exists()

    # 再次“重启”：建库种子守卫 + 启动恢复均不得重建/复发已销毁项目
    db.init_db()
    recover_on_startup()
    conn = db.connect()
    try:
        assert db.get_project(conn, project_id) is None
        names = [r["name"] for r in db.list_projects(conn)]
        assert project["name"] not in names, "已销毁的种子项目重启后不得重建"
        tomb = db.get_destruction(conn, project_id)
        assert tomb["status"] == "DESTROYED"
    finally:
        conn.close()


def test_destroy_delete_failure_keeps_destroying_and_retries(
    client, project_id, sample_dicom, tmp_path, monkeypatch
):
    """删除失败保持 DESTROYING 并记录错误，自动重试后完成。"""
    from app import destroy
    from app.config import settings

    batch = upload(client, project_id, [sample_dicom(tmp_path / "f.dcm")]).json()
    zip_file = settings.published_dir / f"{batch['id']}.zip"

    original = destroy._delete_project_files
    calls = {"n": 0}

    def flaky(pid):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("模拟磁盘故障")
        return original(pid)

    monkeypatch.setattr(destroy, "_delete_project_files", flaky)

    assert client.post(f"/api/projects/{project_id}/destroy").status_code == 202

    # 第一次删除失败：仍为 DESTROYING，记录错误与尝试次数
    deadline = time.monotonic() + 5
    state = None
    while time.monotonic() < deadline:
        state = client.get(f"/api/projects/{project_id}/destroy").json()
        if state["attempts"] >= 1:
            break
        time.sleep(0.05)
    assert state["attempts"] >= 1
    assert state["status"] == "DESTROYING"
    assert "模拟磁盘故障" in (state["last_error"] or "")

    # 自动重试后完成，文件删除
    _await_destroyed(client, project_id)
    assert not zip_file.exists()


def test_destroyed_project_name_reusable_after_completion(client, project_id, sample_dicom, tmp_path):
    projects = client.get("/api/projects").json()
    name = [p for p in projects if p["id"] == project_id][0]["name"]
    upload(client, project_id, [sample_dicom(tmp_path / "f.dcm")])
    client.post(f"/api/projects/{project_id}/destroy")
    _await_destroyed(client, project_id)
    # 同名可创建全新项目（新 ID），与已销毁项目互不影响
    resp = client.post("/api/projects", data={"name": name})
    assert resp.status_code == 201
    assert resp.json()["id"] != project_id
    # 新项目可正常入库
    assert upload(client, resp.json()["id"], [sample_dicom(tmp_path / "g.dcm")]).status_code == 201
