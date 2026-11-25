"""115 网盘只秒传存储代理，拦截普通分片上传并保留其它存储能力。"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any, Callable, Optional, cast

from app.modules.filemanager.storages import StorageBase
from app.schemas.file import FileItem, StorageUsage
from app.schemas.types import StorageSchema
from app.sdk.logging import logger


def calc_file_digests(
    filepath: Path,
    preid_bytes: int = 128 * 1024 * 1024,
    is_stopped: Optional[Callable[[], bool]] = None,
) -> tuple[str, str]:
    """单次顺序读取本地文件，同时计算全量 SHA1 与前 ``preid_bytes`` 字节摘要。"""
    full_sha1 = hashlib.sha1()
    pre_sha1 = hashlib.sha1()
    pre_remaining = max(preid_bytes, 0)
    chunk_size = 65536
    with open(filepath, "rb") as file_obj:
        while chunk := file_obj.read(chunk_size):
            if is_stopped and is_stopped():
                raise InterruptedError("插件已停用，中止秒传哈希计算")
            full_sha1.update(chunk)
            if pre_remaining > 0:
                if len(chunk) <= pre_remaining:
                    pre_sha1.update(chunk)
                    pre_remaining -= len(chunk)
                else:
                    pre_sha1.update(chunk[:pre_remaining])
                    pre_remaining = 0
    return full_sha1.hexdigest(), pre_sha1.hexdigest()


def calc_sha1(filepath: Path, size: Optional[int] = None) -> str:
    """计算本地文件完整 SHA1 或前 ``size`` 字节的十六进制摘要。"""
    sha1 = hashlib.sha1()
    with open(filepath, "rb") as file_obj:
        if size:
            remaining = size
            while remaining > 0:
                chunk = file_obj.read(min(65536, remaining))
                if not chunk:
                    break
                sha1.update(chunk)
                remaining -= len(chunk)
        else:
            while chunk := file_obj.read(65536):
                sha1.update(chunk)
    return sha1.hexdigest()


def calc_slice_sha1_upper(filepath: Path, start: int, end: int) -> str:
    """计算闭区间 ``[start, end]`` 内容的大写十六进制 SHA1 摘要。"""
    length = max(end - start + 1, 0)
    sha1 = hashlib.sha1()
    with open(filepath, "rb") as file_obj:
        file_obj.seek(start)
        remaining = length
        while remaining > 0:
            chunk = file_obj.read(min(8192, remaining))
            if not chunk:
                break
            sha1.update(chunk)
            remaining -= len(chunk)
    return sha1.hexdigest().upper()


def build_uploaded_fileitem(
    target_path: Path,
    local_path: Path,
    file_size: int,
) -> FileItem:
    """秒传成功但目录索引尚未刷新时构造目标 FileItem。"""
    return FileItem(
        storage=StorageSchema.U115.value,
        path=target_path.as_posix(),
        type="file",
        name=target_path.name,
        basename=target_path.stem,
        extension=target_path.suffix[1:] or None,
        size=file_size,
        modify_time=local_path.stat().st_mtime if local_path.exists() else None,
    )


def build_fileitem_from_info(
    info_resp: dict[str, Any],
    target_path: Path,
) -> FileItem:
    """根据 115 ``/open/folder/get_info`` 响应构造 FileItem。"""
    is_dir = str(info_resp.get("file_category", "1")) == "0"
    file_name = str(info_resp.get("file_name") or target_path.name)
    name_path = Path(file_name)
    return FileItem(
        storage=StorageSchema.U115.value,
        fileid=str(info_resp["file_id"]),
        path=target_path.as_posix() + ("/" if is_dir else ""),
        type="dir" if is_dir else "file",
        name=file_name,
        basename=name_path.stem,
        extension=name_path.suffix[1:] if not is_dir else None,
        pickcode=info_resp.get("pick_code"),
        size=info_resp.get("size_byte") if not is_dir else None,
        modify_time=info_resp.get("utime"),
    )


class U115RapidStorageProxy(StorageBase):
    """代理 115 存储操作，在 ``upload`` 阶段强制只走秒传，禁止 OSS 普通上传。"""

    _is_u115_rapid_proxy = True
    schema = StorageSchema.U115
    transtype = {"move": "移动", "copy": "复制"}

    def __init__(
        self,
        delegate: StorageBase,
        *,
        should_enforce_rapid: Callable[[Path], bool],
        on_rapid_success: Callable[[Path, str], None],
        on_rapid_failure: Callable[[Path, Optional[str], str], None],
        is_stopped: Optional[Callable[[], bool]] = None,
    ) -> None:
        """初始化 115 只秒传代理并绑定委托存储与回调。"""
        super().__init__()
        self._delegate = delegate
        self._should_enforce_rapid = should_enforce_rapid
        self._on_rapid_success = on_rapid_success
        self._on_rapid_failure = on_rapid_failure
        self._is_stopped = is_stopped or (lambda: False)

    @property
    def snapshot_check_folder_modtime(self) -> bool:
        """透传底层 115 存储的目录快照策略。"""
        return bool(getattr(self._delegate, "snapshot_check_folder_modtime", False))

    @property
    def snapshot_strict_query(self) -> bool:
        """透传底层 115 存储的严格查询开关。"""
        return bool(getattr(self._delegate, "snapshot_strict_query", False))

    def init_storage(self) -> None:
        """委托底层 115 存储初始化。"""
        self._delegate.init_storage()

    def check(self) -> bool:
        """委托检查底层 115 存储是否可用。"""
        return bool(self._delegate.check())

    def close(self) -> bool | None:
        """关闭请求不应销毁共享的底层 115 单例客户端。"""
        return None

    def list(self, fileitem: FileItem) -> list[FileItem]:
        """委托列举目录子项。"""
        return self._delegate.list(fileitem)

    def create_folder(self, fileitem: FileItem, name: str) -> Optional[FileItem]:
        """委托创建目录。"""
        return self._delegate.create_folder(fileitem, name)

    def get_folder(self, path: Path) -> Optional[FileItem]:
        """委托获取或递归创建目录。"""
        return self._delegate.get_folder(path)

    def get_item(self, path: Path) -> Optional[FileItem]:
        """委托按路径查询文件或目录。"""
        return self._delegate.get_item(path)

    def get_parent(self, fileitem: FileItem) -> Optional[FileItem]:
        """委托获取父目录项。"""
        return self._delegate.get_parent(fileitem)

    def delete(self, fileitem: FileItem) -> bool:
        """委托删除文件或目录。"""
        return self._delegate.delete(fileitem)

    def rename(self, fileitem: FileItem, name: str) -> bool:
        """委托重命名文件或目录。"""
        return self._delegate.rename(fileitem, name)

    def download(self, fileitem: FileItem, path: Optional[Path] = None) -> Optional[Path]:
        """委托下载文件。"""
        return self._delegate.download(fileitem, path)

    def detail(self, fileitem: FileItem) -> Optional[FileItem]:
        """委托查询文件详情。"""
        return self._delegate.detail(fileitem)

    def copy(self, fileitem: FileItem, path: Path, new_name: str) -> bool:
        """委托复制文件。"""
        return self._delegate.copy(fileitem, path, new_name)

    def move(self, fileitem: FileItem, path: Path, new_name: str) -> bool:
        """委托移动文件。"""
        return self._delegate.move(fileitem, path, new_name)

    def link(self, fileitem: FileItem, target_file: Path) -> bool:
        """委托硬链接操作。"""
        return self._delegate.link(fileitem, target_file)

    def softlink(self, fileitem: FileItem, target_file: Path) -> bool:
        """委托软链接操作。"""
        return self._delegate.softlink(fileitem, target_file)

    def usage(self) -> Optional[StorageUsage]:
        """委托查询存储容量。"""
        return self._delegate.usage()

    def __getattr__(self, name: str) -> Any:
        """透传如 ``get_item_strict``、``move_item`` 等扩展接口。"""
        return getattr(self._delegate, name)

    def _resolve_target_dir(self, target_dir: FileItem) -> Optional[FileItem]:
        """确保目标目录带有有效的 115 目录 fileid。"""
        if target_dir.fileid and target_dir.path:
            return target_dir
        if target_dir.path:
            resolved = self._delegate.get_item(Path(target_dir.path))
            if resolved and resolved.fileid:
                return resolved
        return None

    def _request_upload_init(
        self,
        *,
        local_path: Path,
        target_name: str,
        target_cid: str,
        file_size: int,
    ) -> tuple[Optional[dict[str, Any]], str]:
        """执行 115 秒传初始化（含必要时的二次区间摘要校验）。"""
        request_api = getattr(self._delegate, "_request_api", None)
        if not callable(request_api):
            return None, "底层 115 存储不支持开放接口秒传初始化"

        check_session = getattr(self._delegate, "_check_session", None)
        if callable(check_session):
            try:
                check_session()
            except Exception as err:
                return None, f"115 登录凭据不可用：{err}"

        if self._is_stopped():
            raise InterruptedError("插件已停用，中止秒传初始化")

        file_sha1, file_preid = calc_file_digests(
            local_path,
            is_stopped=self._is_stopped,
        )
        init_data: dict[str, Any] = {
            "file_name": target_name,
            "file_size": file_size,
            "target": f"U_1_{target_cid}",
            "fileid": file_sha1,
            "preid": file_preid,
        }
        init_resp = request_api("POST", "/open/upload/init", data=init_data)
        if not isinstance(init_resp, dict) or not init_resp:
            return None, "115 秒传初始化请求无响应或被风控冷却"
        if not init_resp.get("state"):
            return None, f"115 初始化上传失败：{init_resp.get('error') or '未知错误'}"

        init_result = init_resp.get("data")
        if not isinstance(init_result, dict):
            return None, "115 初始化上传返回数据格式异常"

        sign_check = init_result.get("sign_check")
        pick_code = init_result.get("pick_code")
        sign_key = init_result.get("sign_key")
        if init_result.get("code") in (700, 701) and sign_check:
            logger.debug(f"【115秒传助手】{target_name} 需要二次区段校验：{sign_check}")
            try:
                start_str, end_str = str(sign_check).split("-", 1)
                sign_val = calc_slice_sha1_upper(local_path, int(start_str), int(end_str))
            except (OSError, ValueError) as err:
                return None, f"计算 115 二次校验摘要失败：{err}"
            init_data.update(
                {
                    "pick_code": pick_code,
                    "sign_key": sign_key,
                    "sign_val": sign_val,
                }
            )
            init_resp = request_api("POST", "/open/upload/init", data=init_data)
            if not isinstance(init_resp, dict) or not init_resp:
                return None, "115 秒传二次认证请求无响应或被风控冷却"
            if not init_resp.get("state"):
                return None, f"115 秒传二次认证失败：{init_resp.get('error') or '未知错误'}"
            init_result = init_resp.get("data")
            if not isinstance(init_result, dict):
                return None, "115 秒传二次认证返回数据格式异常"

        return init_result, ""

    def _confirm_rapid_item(
        self,
        *,
        init_result: dict[str, Any],
        target_path: Path,
        local_path: Path,
        file_size: int,
    ) -> FileItem:
        """秒传命中后查询远端文件信息，查询不可见时合成目标项。"""
        file_id = init_result.get("file_id")
        request_api = getattr(self._delegate, "_request_api", None)
        if file_id and callable(request_api):
            if not self._is_stopped():
                time.sleep(2)
            try:
                fid_param: int | str = int(file_id)
            except (TypeError, ValueError):
                fid_param = str(file_id)
            try:
                info_resp = request_api(
                    "GET",
                    "/open/folder/get_info",
                    "data",
                    params={"file_id": fid_param},
                )
            except Exception as err:
                logger.debug(f"【115秒传助手】查询秒传文件详情失败，将回退路径查询：{err}")
                info_resp = None
            if isinstance(info_resp, dict) and info_resp.get("file_id"):
                return build_fileitem_from_info(info_resp, target_path)

        uploaded_item = self._delegate.get_item(target_path)
        return uploaded_item or build_uploaded_fileitem(target_path, local_path, file_size)

    def upload(
        self,
        fileitem: FileItem,
        path: Path,
        new_name: Optional[str] = None,
    ) -> Optional[FileItem]:
        """上传文件；受控文件仅允许 115 秒传，秒传失败时直接返回 None。"""
        if not path or not path.is_file():
            logger.warning(f"【115秒传助手】本地源文件不存在，无法上传：{path}")
            return None

        if not self._should_enforce_rapid(path):
            logger.debug(f"【115秒传助手】文件不满足只秒传拦截条件，放行普通上传：{path.name}")
            return self._delegate.upload(fileitem, path, new_name)

        resolved_dir = self._resolve_target_dir(fileitem)
        if not resolved_dir or not resolved_dir.fileid or not resolved_dir.path:
            reason = f"目标目录缺少 115 目录标识：{fileitem.path}"
            logger.warning(f"【115秒传助手】{reason}")
            self._on_rapid_failure(path, None, reason)
            return None

        target_name = new_name or path.name
        target_path = Path(cast(str, resolved_dir.path)) / target_name
        file_size = path.stat().st_size
        logger.debug(
            f"【115秒传助手】开始秒传校验：{target_name} -> {target_path.as_posix()} (size={file_size})"
        )

        try:
            init_result, error_msg = self._request_upload_init(
                local_path=path,
                target_name=target_name,
                target_cid=resolved_dir.fileid,
                file_size=file_size,
            )
        except InterruptedError as err:
            logger.info(f"【115秒传助手】{err}：{target_name}")
            return None
        except Exception as err:
            error_msg = f"115 秒传检测异常：{err}"
            init_result = None

        if init_result is None:
            logger.warning(f"【115秒传助手】{target_name} {error_msg}，已禁止普通上传")
            self._on_rapid_failure(path, target_path.as_posix(), error_msg)
            return None

        status = init_result.get("status")
        if status != 2:
            reason = f"秒传未命中（status={status}），已拒绝普通上传"
            logger.warning(f"【115秒传助手】{target_name} {reason}")
            self._on_rapid_failure(path, target_path.as_posix(), reason)
            return None

        logger.info(f"【115秒传助手】{target_name} 秒传成功")
        uploaded = self._confirm_rapid_item(
            init_result=init_result,
            target_path=target_path,
            local_path=path,
            file_size=file_size,
        )
        self._on_rapid_success(path, target_path.as_posix())
        return uploaded
