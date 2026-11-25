"""验证 115 只秒传代理、失败延迟重试及插件生命周期边界。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.plugins.u115rapidtransfer import U115RapidTransfer
from app.plugins.u115rapidtransfer.rapid_proxy import calc_sha1, calc_slice_sha1_upper
from app.schemas.event import StorageOperSelectionEventData
from app.schemas.file import FileItem
from app.schemas.types import StorageSchema


@pytest.fixture
def plugin_data_store():
    """提供内存级插件持久化存储替身。"""
    return {}


@pytest.fixture
def plugin(monkeypatch, plugin_data_store):
    """构造启用状态的 U115RapidTransfer 实例并隔离宿主依赖。"""
    from app.plugins import u115rapidtransfer as plugin_mod

    monkeypatch.setattr(
        plugin_mod.scheduler_sdk,
        "add_plugin_once_job",
        MagicMock(return_value=True),
    )
    monkeypatch.setattr(
        plugin_mod.scheduler_sdk,
        "remove_plugin_once_job",
        MagicMock(),
    )

    instance = U115RapidTransfer()
    instance.save_data = lambda key, value, plugin_id=None: plugin_data_store.__setitem__(key, value)
    instance.get_data = lambda key=None, plugin_id=None: plugin_data_store.get(key)
    instance.update_config = MagicMock(return_value=True)
    instance.post_message = MagicMock()
    instance.init_plugin(
        {
            "enabled": True,
            "notify": True,
            "only_media": True,
            "min_size_mb": 0,
            "retry_delay": 300,
            "max_retries": 3,
            "cron": "*/5 * * * *",
        }
    )
    return instance


def _make_delegate(init_responses: list[dict], info_resp: dict | None = None):
    """构造只响应 /open/upload/init 与 /open/folder/get_info 的 115 替身。"""
    delegate = MagicMock()
    delegate.schema = StorageSchema.U115
    delegate._check_session = MagicMock()
    delegate.get_item = MagicMock(return_value=None)
    delegate.upload = MagicMock(side_effect=AssertionError("不应该回退调用底层普通 upload"))

    calls = []

    def request_api(method, endpoint, result_key=None, **kwargs):
        calls.append((method, endpoint, kwargs))
        if endpoint == "/open/upload/init":
            return init_responses.pop(0)
        if endpoint == "/open/folder/get_info":
            return info_resp
        return None

    delegate._request_api = request_api
    delegate.api_calls = calls
    return delegate


def test_rapid_upload_success_with_sign_check(plugin, tmp_path, monkeypatch):
    """秒传遇到二次校验 701 后命中 status=2，不应触发普通上传。"""
    from app.plugins.u115rapidtransfer import rapid_proxy

    monkeypatch.setattr(rapid_proxy.time, "sleep", lambda _sec: None)

    media_file = tmp_path / "Movie.2026.mkv"
    content = b"0123456789abcdef" * 1024
    media_file.write_bytes(content)

    expected_sha1 = hashlib.sha1(content).hexdigest()
    expected_slice = hashlib.sha1(content[10:21]).hexdigest().upper()

    delegate = _make_delegate(
        init_responses=[
            {
                "state": True,
                "data": {
                    "code": 701,
                    "sign_check": "10-20",
                    "pick_code": "pk1",
                    "sign_key": "sk1",
                },
            },
            {
                "state": True,
                "data": {
                    "status": 2,
                    "file_id": "998877",
                },
            },
        ],
        info_resp={
            "file_id": "998877",
            "file_name": "Movie.2026.mkv",
            "file_category": "1",
            "pick_code": "pk_ok",
            "size_byte": len(content),
            "utime": 1700000000,
        },
    )
    plugin._create_u115_delegate = lambda: delegate

    event = SimpleNamespace(event_data=StorageOperSelectionEventData(storage="u115"))
    plugin.on_storage_oper_selection(event)
    proxy = event.event_data.storage_oper
    assert proxy is not None

    target_dir = FileItem(storage="u115", fileid="100", path="/Movies/Test/", type="dir", name="Test")
    result = proxy.upload(target_dir, media_file, "Movie.2026.mkv")

    assert result is not None
    assert result.fileid == "998877"
    assert result.size == len(content)
    assert delegate.upload.call_count == 0
    assert len(delegate.api_calls) == 3
    assert delegate.api_calls[0][2]["data"]["fileid"] == expected_sha1
    assert delegate.api_calls[1][2]["data"]["sign_val"] == expected_slice
    assert calc_sha1(media_file) == expected_sha1
    assert calc_slice_sha1_upper(media_file, 10, 20) == expected_slice


def test_rapid_upload_miss_blocks_oss_upload_and_schedules_retry(plugin, tmp_path, monkeypatch):
    """秒传未命中时中止普通上传，并在 TransferFailed 后安排延迟重试。"""
    from app.plugins import u115rapidtransfer as plugin_mod

    add_job_mock = MagicMock(return_value=True)
    monkeypatch.setattr(plugin_mod.scheduler_sdk, "add_plugin_once_job", add_job_mock)
    monkeypatch.setattr(
        plugin,
        "_resolve_transfer_history_id",
        MagicMock(return_value=42),
    )

    media_file = tmp_path / "Show.S01E01.mkv"
    media_file.write_bytes(b"video-bytes")

    delegate = _make_delegate(
        init_responses=[{"state": True, "data": {"status": 1}}]
    )
    plugin._create_u115_delegate = lambda: delegate

    sel_event = SimpleNamespace(event_data=StorageOperSelectionEventData(storage="u115"))
    plugin.on_storage_oper_selection(sel_event)
    proxy = sel_event.event_data.storage_oper

    target_dir = FileItem(storage="u115", fileid="200", path="/TV/Show/", type="dir", name="Show")
    assert proxy.upload(target_dir, media_file) is None
    assert delegate.upload.call_count == 0

    fail_event = SimpleNamespace(
        event_data=SimpleNamespace(
            fileitem=FileItem(storage="local", path=media_file.as_posix(), type="file", name=media_file.name),
            transferinfo=SimpleNamespace(target_storage="u115", message="上传 u115 失败"),
            mediainfo=SimpleNamespace(title_year="Show (2026)"),
            transfer_history_id=42,
        )
    )
    plugin.on_transfer_failed(fail_event)

    pending = plugin._load_pending_retries()
    norm_path = media_file.as_posix()
    assert norm_path in pending
    assert pending[norm_path]["attempt"] == 1
    assert pending[norm_path]["history_id"] == 42
    assert add_job_mock.call_count == 1
    assert add_job_mock.call_args.kwargs["delay_seconds"] == 300


def test_non_media_file_bypasses_rapid_only_when_only_media_enabled(plugin, tmp_path):
    """开启仅限制媒体文件时，字幕等非媒体文件允许走底层普通上传。"""
    sub_file = tmp_path / "Movie.2026.srt"
    sub_file.write_text("1\n00:00:01,000 --> 00:00:02,000\nhello\n", encoding="utf-8")

    expected = FileItem(storage="u115", fileid="555", path="/Movies/Movie.2026.srt", type="file", name=sub_file.name)
    delegate = MagicMock()
    delegate.schema = StorageSchema.U115
    delegate.upload = MagicMock(return_value=expected)
    plugin._create_u115_delegate = lambda: delegate

    sel_event = SimpleNamespace(event_data=StorageOperSelectionEventData(storage="u115"))
    plugin.on_storage_oper_selection(sel_event)
    proxy = sel_event.event_data.storage_oper

    target_dir = FileItem(storage="u115", fileid="100", path="/Movies/", type="dir", name="Movies")
    res = proxy.upload(target_dir, sub_file)
    assert res == expected
    assert delegate.upload.call_count == 1


def test_retry_execution_and_max_retries_exhausted(plugin, tmp_path, monkeypatch):
    """验证重试到期后调用 TransferChain 重试，并在达到上限后停止重试。"""
    from app.plugins import u115rapidtransfer as plugin_mod

    media_file = tmp_path / "Retry.2026.mkv"
    media_file.write_bytes(b"data")
    norm_path = media_file.as_posix()

    fake_history = SimpleNamespace(id=88, status=False)
    fake_oper = MagicMock()
    fake_oper.get_by_src.return_value = fake_history
    monkeypatch.setattr(plugin_mod, "TransferHistoryOper", lambda: fake_oper)

    fake_chain = MagicMock()
    fake_chain.redo_transfer_history.return_value = (True, "")
    monkeypatch.setattr(plugin_mod, "TransferChain", lambda: fake_chain)

    plugin.save_data(
        "pending_retries",
        {
            norm_path: {
                "src_path": norm_path,
                "raw_src_path": str(media_file),
                "src_storage": "local",
                "target_path": "/Movies/Retry.2026.mkv",
                "history_id": 88,
                "attempt": 3,
                "max_retries": 3,
                "next_retry_at": 0,
                "last_error": "秒传未命中",
                "title": "Retry (2026)",
            }
        },
    )

    assert plugin.check_pending_retries() == 1
    fake_chain.redo_transfer_history.assert_called_once_with(88)

    plugin._on_rapid_failure(media_file, "/Movies/Retry.2026.mkv", "秒传未命中")
    plugin.on_transfer_failed(
        SimpleNamespace(
            event_data=SimpleNamespace(
                fileitem=FileItem(storage="local", path=str(media_file), type="file", name=media_file.name),
                transferinfo=SimpleNamespace(target_storage="u115", message="失败"),
                mediainfo=SimpleNamespace(title_year="Retry (2026)"),
                transfer_history_id=88,
            )
        )
    )
    assert norm_path not in plugin._load_pending_retries()
    assert plugin._load_stats()["retry_exhausted"] == 1


def test_stop_service_cancels_scheduled_jobs(plugin, monkeypatch):
    """停用插件应清理所有已登记的一次性调度任务并标记停止。"""
    from app.plugins import u115rapidtransfer as plugin_mod

    remove_mock = MagicMock()
    monkeypatch.setattr(plugin_mod.scheduler_sdk, "remove_plugin_once_job", remove_mock)

    plugin._scheduled_job_ids.add("retry_abc123")
    plugin.stop_service()

    assert plugin.get_state() is False
    assert plugin._stop_event.is_set() is True
    assert not plugin._scheduled_job_ids
    removed_ids = {call.args[1] for call in remove_mock.call_args_list}
    assert {"retry_abc123", "scan_pending_once"} <= removed_ids


def test_form_page_and_api_responses(plugin):
    """配置表单、详情页和 API 均返回符合 V3 规范的结构。"""
    form, defaults = plugin.get_form()
    assert form and defaults["only_media"] is True
    assert defaults["retry_delay"] == 600
    assert defaults["max_retries"] == 5

    page = plugin.get_page()
    assert isinstance(page, list) and len(page) >= 1

    status_resp = plugin.api_get_status()
    assert status_resp.success is True
    assert status_resp.data.enabled is True
