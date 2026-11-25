"""115 网盘整理只秒传与延迟重试插件。"""

from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
from threading import Event as ThreadEvent
from threading import RLock
import time
from typing import Any, Optional

from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel, Field

from app.chain.transfer import TransferChain
from app.db.oper.transferhistory import TransferHistoryOper
from app.schemas.event import StorageOperSelectionEventData
from app.schemas.response import Response
from app.schemas.types import ChainEventType, EventType, MessageType, StorageSchema
from app.sdk import scheduler as scheduler_sdk
from app.sdk.config import settings
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase

from .rapid_proxy import U115RapidStorageProxy

_DEFAULT_MEDIA_EXTS = frozenset(
    {
        ".mp4",
        ".mkv",
        ".avi",
        ".mov",
        ".wmv",
        ".flv",
        ".ts",
        ".m2ts",
        ".iso",
        ".rmvb",
        ".webm",
        ".mp3",
        ".flac",
        ".wav",
        ".ape",
        ".aac",
        ".m4a",
        ".ogg",
        ".opus",
    }
)
_MAX_RECORD_HISTORY = 50
_DEFAULT_CRON = "*/5 * * * *"


def _normalize_path(path: str | Path) -> str:
    """将文件路径规范化为稳定的 POSIX 文本键。"""
    return Path(path).as_posix()


def _format_ts(ts: float | None) -> str:
    """将 Unix 时间戳格式化为本地可读时间字符串。"""
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _extract_attr_or_key(obj: Any, key: str, default: Any = None) -> Any:
    """兼容从字典或对象实例读取字段。"""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class RapidTransferStatusData(BaseModel):
    """插件状态查询响应数据模型。"""

    enabled: bool = Field(description="插件是否启用")
    stats: dict[str, int] = Field(default_factory=dict, description="累计统计计数")
    pending: list[dict[str, Any]] = Field(default_factory=list, description="待重试列表")
    records: list[dict[str, Any]] = Field(default_factory=list, description="最近记录")


class RapidTransferActionData(BaseModel):
    """插件手动操作响应数据模型。"""

    count: int = Field(default=0, description="本次处理数量")


class U115RapidTransfer(_PluginBase):
    """在整理文件到 115 网盘时强制只走秒传，秒传失败后按指定间隔自动重试。"""

    plugin_name = "115秒传助手"
    plugin_desc = "整理文件到115网盘时仅允许秒传（禁止普通上传），秒传失败后等待指定时间自动重试。"
    plugin_icon = "https://raw.githubusercontent.com/ruins-qiao/MoviePilot-Plugins/main/icons/U115RapidTransfer.png"
    plugin_version = "1.1.2"
    plugin_author = "qiaoba"
    author_url = "https://github.com/ruins-qiao/MoviePilot-Plugins"
    plugin_config_prefix = "u115rapidtransfer_"
    plugin_order = 50
    auth_level = 1

    def __init__(self) -> None:
        """建立实例独立的配置与状态容器，不在导入或构造期启动后台任务。"""
        super().__init__()
        self._lock = RLock()
        self._stop_event = ThreadEvent()
        self._enabled = False
        self._notify = True
        self._only_media = True
        self._min_size_mb = 0
        self._retry_delay = 600
        self._max_retries = 5
        self._cron = _DEFAULT_CRON
        self._rapid_failed_items: dict[str, dict[str, Any]] = {}
        self._scheduled_job_ids: set[str] = set()
        self._in_flight_retries: set[str] = set()
        self._last_scan_at: Optional[float] = None

    @staticmethod
    def _coerce_non_negative_int(value: Any, default: int, minimum: int = 0) -> int:
        """解析整数配置并钳制到允许的下限。"""
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return max(parsed, minimum)

    @staticmethod
    def _validate_cron(expr: Any) -> str:
        """校验 Cron 表达式，非法时回退为默认周期。"""
        text = str(expr or "").strip() or _DEFAULT_CRON
        try:
            CronTrigger.from_crontab(text)
            return text
        except ValueError:
            logger.warning(f"【115秒传助手】无效的 Cron 表达式 {text!r}，已回退为 {_DEFAULT_CRON}")
            return _DEFAULT_CRON

    @staticmethod
    def _make_retry_job_id(norm_path: str) -> str:
        """为源文件生成稳定且固定长度的一次性调度任务 ID。"""
        digest = hashlib.sha1(norm_path.encode("utf-8")).hexdigest()[:16]
        return f"retry_{digest}"

    def _cancel_all_once_jobs(self) -> int:
        """取消当前插件登记的所有待执行一次性重试任务，返回清理的调度任务数。"""
        remove_job = getattr(scheduler_sdk, "remove_plugin_once_job", None)
        with self._lock:
            job_ids = list(self._scheduled_job_ids)
            self._scheduled_job_ids.clear()
        if not callable(remove_job):
            return 0
        plugin_id = self.__class__.__name__
        all_ids = [*job_ids, "scan_pending_once"]
        for job_id in all_ids:
            try:
                remove_job(plugin_id, job_id)
            except Exception as err:
                logger.debug(f"【115秒传助手】取消调度任务 {job_id} 忽略异常：{err}")
        return len(job_ids)

    def _schedule_once_job(
        self,
        job_id: str,
        func: Any,
        name: str,
        *,
        delay_seconds: float,
        func_kwargs: Optional[dict[str, Any]] = None,
    ) -> bool:
        """通过宿主调度器安全注册一次性任务，不可用时由周期巡检兜底。"""
        add_job = getattr(scheduler_sdk, "add_plugin_once_job", None)
        if not callable(add_job):
            return False
        try:
            scheduled = bool(
                add_job(
                    self.__class__.__name__,
                    job_id,
                    func,
                    name,
                    delay_seconds=max(delay_seconds, 1),
                    func_kwargs=func_kwargs,
                )
            )
        except Exception as err:
            logger.warning(f"【115秒传助手】注册一次性任务 {job_id} 失败：{err}")
            return False
        if scheduled:
            with self._lock:
                self._scheduled_job_ids.add(job_id)
        return scheduled

    def init_plugin(self, config: dict | None = None) -> None:
        """加载并规范化插件配置，恢复持久化待重试队列。"""
        self._cancel_all_once_jobs()
        self._stop_event.clear()

        cfg = dict(config or {})
        self._enabled = bool(cfg.get("enabled"))
        self._notify = bool(cfg.get("notify", True))
        self._only_media = bool(cfg.get("only_media", True))
        self._min_size_mb = self._coerce_non_negative_int(cfg.get("min_size_mb"), 0, 0)
        self._retry_delay = self._coerce_non_negative_int(cfg.get("retry_delay"), 600, 10)
        self._max_retries = self._coerce_non_negative_int(cfg.get("max_retries"), 5, 0)
        self._cron = self._validate_cron(cfg.get("cron"))

        with self._lock:
            self._rapid_failed_items.clear()
            self._in_flight_retries.clear()

        if not self._enabled:
            logger.info("【115秒传助手】插件未启用")
            return

        restored_count = self._reschedule_persisted_retries()
        logger.info(
            "【115秒传助手】插件已启用："
            f"only_media={self._only_media}, min_size_mb={self._min_size_mb}, "
            f"retry_delay={self._retry_delay}s, max_retries={self._max_retries}, "
            f"cron='{self._cron}', restored_pending={restored_count}"
        )

        if cfg.get("onlyonce"):
            cfg["onlyonce"] = False
            self.update_config(cfg)
            self._schedule_once_job(
                "scan_pending_once",
                self.check_pending_retries,
                "115秒传助手立即巡检待重试队列",
                delay_seconds=3,
            )
            logger.info("【115秒传助手】已登记立即巡检待重试队列任务（3 秒后执行）")

    def get_state(self) -> bool:
        """返回插件当前是否启用。"""
        return self._enabled

    @staticmethod
    def get_command() -> list[dict[str, Any]]:
        """注册远程手动立即重试命令。"""
        return [
            {
                "cmd": "/u115_rapid_retry",
                "event": EventType.PluginAction,
                "desc": "立即重试115秒传失败队列",
                "category": "文件整理",
                "data": {"action": "u115_rapid_retry"},
            }
        ]

    def get_api(self) -> list[dict[str, Any]]:
        """暴露队列查询与手动重试管理接口。"""
        return [
            {
                "path": "/status",
                "endpoint": self.api_get_status,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": Response[RapidTransferStatusData],
                "summary": "获取115秒传助手运行状态",
            },
            {
                "path": "/retry",
                "endpoint": self.api_trigger_retry,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": Response[RapidTransferActionData],
                "summary": "立即重试待秒传任务",
            },
            {
                "path": "/clear",
                "endpoint": self.api_clear_pending,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": Response[RapidTransferActionData],
                "summary": "清理115待重试队列",
            },
            {
                "path": "/clear_records",
                "endpoint": self.api_clear_records,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": Response[RapidTransferActionData],
                "summary": "清空最近处理记录",
            },
        ]

    def get_service(self) -> list[dict[str, Any]]:
        """插件启用时注册周期巡检服务，确保重启后或调度器未运行时仍可按期重试。"""
        if not self._enabled:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except ValueError:
            trigger = CronTrigger.from_crontab(_DEFAULT_CRON)
        return [
            {
                "id": f"{self.__class__.__name__}.CheckPendingRetries",
                "name": "115秒传助手待重试队列巡检",
                "trigger": trigger,
                "func": self.check_pending_retries,
                "kwargs": {},
            }
        ]

    def get_form(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """返回插件配置表单与默认模型。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即巡检一次",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "only_media",
                                            "label": "仅对媒体文件限制只秒传",
                                            "hint": "开启后字幕、NFO、图片等附属文件允许普通上传，仅音视频文件强制只秒传",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "min_size_mb",
                                            "label": "最小限制文件大小（MB）",
                                            "type": "number",
                                            "placeholder": "0",
                                            "hint": "小于该大小的文件允许普通上传，0 表示不按体积放行",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "retry_delay",
                                            "label": "失败重试等待时间（秒）",
                                            "type": "number",
                                            "placeholder": "600",
                                            "hint": "秒传失败后等待多少秒再重试（最小 10 秒，例如 600 = 10 分钟）",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_retries",
                                            "label": "最大自动重试次数",
                                            "type": "number",
                                            "placeholder": "5",
                                            "hint": "设为 0 表示仅阻止普通上传，不自动重试",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "兜底巡检周期",
                                            "placeholder": _DEFAULT_CRON,
                                            "hint": "定期检查因重启或错过定时而到期的待重试任务",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "工作原理：当文件整理目标存储为 115 网盘（u115）时，插件仅调用 115 秒传初始化校验；"
                                                "若秒传未命中，将直接中止本次上传（不会触发慢速 OSS 分片上传），"
                                                "并在整理失败后按设定等待时间自动触发重新整理。可使用命令 /u115_rapid_retry 立即重试。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "only_media": True,
            "min_size_mb": 0,
            "retry_delay": 600,
            "max_retries": 5,
            "cron": _DEFAULT_CRON,
        }

    def get_page(self) -> list[dict[str, Any]]:
        """返回插件详情页，展示待重试队列、交互操作按钮与最近秒传记录。"""
        pending = self._load_pending_retries()
        records = self._load_records()
        stats = self._load_stats()
        plugin_id = self.__class__.__name__

        summary_text = (
            f"状态：{'运行中' if self._enabled else '已停用'} | "
            f"等待重试：{len(pending)} 项 | "
            f"秒传成功：{stats.get('rapid_success', 0)} | "
            f"拦截普通上传：{stats.get('rapid_blocked', 0)} | "
            f"重试耗尽：{stats.get('retry_exhausted', 0)} | "
            f"最近巡检：{_format_ts(self._last_scan_at)}"
        )

        contents: list[dict[str, Any]] = [
            {
                "component": "VAlert",
                "props": {
                    "type": "success" if self._enabled else "warning",
                    "variant": "tonal",
                    "text": summary_text,
                    "class": "mb-4",
                },
            },
            {
                "component": "div",
                "props": {"class": "d-flex flex-wrap ga-2 mb-4"},
                "content": [
                    {
                        "component": "VBtn",
                        "props": {
                            "color": "primary",
                            "variant": "tonal",
                            "size": "small",
                            "disabled": not (self._enabled and pending),
                        },
                        "text": "立即全部重试",
                        "events": {
                            "click": {
                                "api": f"plugin/{plugin_id}/retry",
                                "method": "post",
                            }
                        },
                    },
                    {
                        "component": "VBtn",
                        "props": {
                            "color": "error",
                            "variant": "tonal",
                            "size": "small",
                            "disabled": not pending,
                        },
                        "text": "清空等待队列",
                        "events": {
                            "click": {
                                "api": f"plugin/{plugin_id}/clear",
                                "method": "post",
                            }
                        },
                    },
                    {
                        "component": "VBtn",
                        "props": {
                            "color": "secondary",
                            "variant": "tonal",
                            "size": "small",
                            "disabled": not records,
                        },
                        "text": "清空处理记录",
                        "events": {
                            "click": {
                                "api": f"plugin/{plugin_id}/clear_records",
                                "method": "post",
                            }
                        },
                    },
                ],
            },
        ]

        if pending:
            pending_rows = []
            for item in sorted(pending.values(), key=lambda x: x.get("next_retry_at", 0)):
                src_path = str(item.get("src_path") or "")
                pending_rows.append(
                    {
                        "component": "tr",
                        "content": [
                            {"component": "td", "text": Path(src_path).name or "-"},
                            {
                                "component": "td",
                                "text": f"{item.get('attempt', 0)} / {item.get('max_retries', self._max_retries)}",
                            },
                            {"component": "td", "text": _format_ts(item.get("next_retry_at"))},
                            {"component": "td", "text": str(item.get("last_error") or "-")},
                            {
                                "component": "td",
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {"class": "d-flex ga-2"},
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "color": "primary",
                                                    "variant": "text",
                                                    "size": "x-small",
                                                    "disabled": not self._enabled or not src_path,
                                                },
                                                "text": "重试",
                                                "events": {
                                                    "click": {
                                                        "api": f"plugin/{plugin_id}/retry",
                                                        "method": "post",
                                                        "params": {"src_path": src_path},
                                                    }
                                                },
                                            },
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "color": "error",
                                                    "variant": "text",
                                                    "size": "x-small",
                                                    "disabled": not src_path,
                                                },
                                                "text": "移除",
                                                "events": {
                                                    "click": {
                                                        "api": f"plugin/{plugin_id}/clear",
                                                        "method": "post",
                                                        "params": {"src_path": src_path},
                                                    }
                                                },
                                            },
                                        ],
                                    }
                                ],
                            },
                        ],
                    }
                )
            contents.append(
                {
                    "component": "VCard",
                    "props": {"variant": "outlined", "class": "mb-4"},
                    "content": [
                        {"component": "VCardTitle", "text": "等待延迟重试队列"},
                        {
                            "component": "VTable",
                            "props": {"density": "compact"},
                            "content": [
                                {
                                    "component": "thead",
                                    "content": [
                                        {
                                            "component": "tr",
                                            "content": [
                                                {"component": "th", "text": "文件名"},
                                                {"component": "th", "text": "重试进度"},
                                                {"component": "th", "text": "下次重试时间"},
                                                {"component": "th", "text": "拦截原因"},
                                                {"component": "th", "text": "操作"},
                                            ],
                                        }
                                    ],
                                },
                                {"component": "tbody", "content": pending_rows},
                            ],
                        },
                    ],
                }
            )
        else:
            contents.append(
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "当前没有等待重试的 115 秒传任务。",
                        "class": "mb-4",
                    },
                }
            )

        if records:
            record_rows = [
                {
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": _format_ts(rec.get("time"))},
                        {"component": "td", "text": Path(rec.get("src_path", "")).name or "-"},
                        {"component": "td", "text": str(rec.get("status") or "-")},
                        {"component": "td", "text": str(rec.get("message") or "-")},
                    ],
                }
                for rec in records[:20]
            ]
            contents.append(
                {
                    "component": "VCard",
                    "props": {"variant": "outlined"},
                    "content": [
                        {"component": "VCardTitle", "text": "最近处理记录"},
                        {
                            "component": "VTable",
                            "props": {"density": "compact"},
                            "content": [
                                {
                                    "component": "thead",
                                    "content": [
                                        {
                                            "component": "tr",
                                            "content": [
                                                {"component": "th", "text": "时间"},
                                                {"component": "th", "text": "文件名"},
                                                {"component": "th", "text": "结果"},
                                                {"component": "th", "text": "详情"},
                                            ],
                                        }
                                    ],
                                },
                                {"component": "tbody", "content": record_rows},
                            ],
                        },
                    ],
                }
            )

        return contents

    def stop_service(self) -> None:
        """停用插件并清理定时调度与内存状态。"""
        self._stop_event.set()
        cancelled = self._cancel_all_once_jobs()
        with self._lock:
            self._in_flight_retries.clear()
        self._enabled = False
        logger.info(f"【115秒传助手】插件已停用，已清理 {cancelled} 个待执行重试任务")

    def _media_extensions(self) -> set[str]:
        """读取宿主配置的媒体文件后缀，缺失时使用内置默认集合。"""
        exts: set[str] = set()
        for attr in ("RMT_MEDIAEXT", "RMT_AUDIOEXT"):
            configured = getattr(settings, attr, None)
            if isinstance(configured, (list, tuple, set)):
                exts.update(str(item).lower() for item in configured if item)
        return exts or set(_DEFAULT_MEDIA_EXTS)

    def _should_enforce_rapid(self, local_path: Path) -> bool:
        """判断给定本地文件是否必须强制只走秒传。"""
        if self._only_media:
            if local_path.suffix.lower() not in self._media_extensions():
                return False
        if self._min_size_mb > 0:
            try:
                size_bytes = local_path.stat().st_size
            except OSError:
                return True
            if size_bytes < self._min_size_mb * 1024 * 1024:
                return False
        return True

    def _load_pending_retries(self) -> dict[str, dict[str, Any]]:
        """读取持久化的待重试字典。"""
        raw = self.get_data("pending_retries")
        return dict(raw) if isinstance(raw, dict) else {}

    def _save_pending_retries(self, data: dict[str, dict[str, Any]]) -> None:
        """保存待重试字典到当前实例存储。"""
        self.save_data("pending_retries", data)

    def _load_records(self) -> list[dict[str, Any]]:
        """读取最近处理记录列表。"""
        raw = self.get_data("records")
        return list(raw) if isinstance(raw, list) else []

    def _append_record(self, src_path: str, status: str, message: str) -> None:
        """追加一条处理记录并裁剪到最大保留条数。"""
        with self._lock:
            records = self._load_records()
            records.insert(
                0,
                {
                    "time": time.time(),
                    "src_path": src_path,
                    "status": status,
                    "message": message,
                },
            )
            self.save_data("records", records[:_MAX_RECORD_HISTORY])

    def _load_stats(self) -> dict[str, int]:
        """读取累计统计计数。"""
        raw = self.get_data("stats")
        if not isinstance(raw, dict):
            return {"rapid_success": 0, "rapid_blocked": 0, "retry_exhausted": 0}
        return {
            "rapid_success": int(raw.get("rapid_success", 0)),
            "rapid_blocked": int(raw.get("rapid_blocked", 0)),
            "retry_exhausted": int(raw.get("retry_exhausted", 0)),
        }

    def _increment_stat(self, key: str) -> None:
        """原子递增指定统计计数器。"""
        with self._lock:
            stats = self._load_stats()
            stats[key] = stats.get(key, 0) + 1
            self.save_data("stats", stats)

    def _on_rapid_success(self, local_path: Path, target_path: str) -> None:
        """处理秒传成功回调：清理失败标记与待重试项。"""
        norm_path = _normalize_path(local_path)
        with self._lock:
            self._rapid_failed_items.pop(norm_path, None)
            pending = self._load_pending_retries()
            removed = pending.pop(norm_path, None)
            if removed is not None:
                self._save_pending_retries(pending)
        self._increment_stat("rapid_success")
        self._append_record(norm_path, "秒传成功", f"目标：{target_path}")

    def _on_rapid_failure(
        self,
        local_path: Path,
        target_path: Optional[str],
        reason: str,
    ) -> None:
        """记录被拦截的 115 秒传失败，供后续 TransferFailed 事件精准识别。"""
        norm_path = _normalize_path(local_path)
        with self._lock:
            self._rapid_failed_items[norm_path] = {
                "target_path": target_path,
                "reason": reason,
                "failed_at": time.time(),
            }
        self._increment_stat("rapid_blocked")
        self._append_record(norm_path, "秒传未命中", reason)

    @staticmethod
    def _create_u115_delegate() -> Any:
        """按需构造宿主 115 存储操作实例，避免模块导入期副作用。"""
        from app.modules.filemanager.storages.u115 import U115Pan

        return U115Pan()

    @eventmanager.register(ChainEventType.StorageOperSelection)
    def on_storage_oper_selection(self, event: Optional[Event] = None) -> None:
        """在整理链选择 115 存储适配器时注入只秒传代理。"""
        if not self._enabled or not event or not event.event_data:
            return
        event_data: StorageOperSelectionEventData = event.event_data
        if event_data.storage != StorageSchema.U115.value:
            return
        base_oper = event_data.storage_oper
        if base_oper is not None and getattr(base_oper, "_is_u115_rapid_proxy", False):
            return
        delegate = base_oper or self._create_u115_delegate()
        event_data.storage_oper = U115RapidStorageProxy(
            delegate,
            should_enforce_rapid=self._should_enforce_rapid,
            on_rapid_success=self._on_rapid_success,
            on_rapid_failure=self._on_rapid_failure,
            is_stopped=self._stop_event.is_set,
        )
        logger.info("【115秒传助手】已接管 u115 存储操作，启用只秒传模式")

    def _resolve_transfer_history_id(
        self,
        src_path: str,
        src_storage: str,
        event_history_id: Any,
    ) -> Optional[int]:
        """优先读取事件中的整理历史 ID，缺失时按源路径回查。"""
        if isinstance(event_history_id, int) and event_history_id > 0:
            return event_history_id
        try:
            history = TransferHistoryOper().get_by_src(src_path, storage=src_storage)
            if history is not None and getattr(history, "id", None):
                return int(history.id)
        except Exception as err:
            logger.debug(f"【115秒传助手】查询源路径 {src_path} 整理历史异常：{err}")
        return None

    def _send_notice(self, title: str, text: str) -> None:
        """在开启通知时通过宿主消息通道推送插件通知。"""
        if not self._notify:
            return
        try:
            self.post_message(
                mtype=MessageType.Plugin,
                title=title,
                text=text,
            )
        except Exception as err:
            logger.debug(f"【115秒传助手】发送通知失败：{err}")

    @eventmanager.register(EventType.TransferFailed)
    def on_transfer_failed(self, event: Optional[Event] = None) -> None:
        """在 115 秒传拦截导致整理失败后登记并调度延迟重试。"""
        if not self._enabled or not event or not event.event_data:
            return
        data = event.event_data
        fileitem = _extract_attr_or_key(data, "fileitem")
        src_raw = _extract_attr_or_key(fileitem, "path")
        if not src_raw:
            return
        norm_path = _normalize_path(src_raw)
        src_storage = str(_extract_attr_or_key(fileitem, "storage") or "local")
        transferinfo = _extract_attr_or_key(data, "transferinfo")
        target_storage = _extract_attr_or_key(transferinfo, "target_storage")
        mediainfo = _extract_attr_or_key(data, "mediainfo")
        media_title = (
            _extract_attr_or_key(mediainfo, "title_year")
            or _extract_attr_or_key(mediainfo, "title")
            or Path(norm_path).name
        )

        with self._lock:
            rapid_failure = self._rapid_failed_items.pop(norm_path, None)
            pending = self._load_pending_retries()
            existing = pending.get(norm_path)

        # 只接管由本插件拦截导致的失败，避免误重试识别失败等非秒传问题。
        if rapid_failure is None and not (
            existing is not None and target_storage in (None, StorageSchema.U115.value)
        ):
            return

        reason = (
            rapid_failure.get("reason")
            if rapid_failure
            else _extract_attr_or_key(transferinfo, "message")
            or (existing or {}).get("last_error")
            or "115秒传失败"
        )
        target_path = (rapid_failure or {}).get("target_path") or (existing or {}).get("target_path")

        if self._max_retries == 0:
            self._append_record(norm_path, "已拦截", f"{reason}（已关闭自动重试）")
            self._send_notice(
                "【115秒传助手】已阻止普通上传",
                f"文件：{media_title}\n路径：{norm_path}\n原因：{reason}\n未开启自动重试（最大重试次数为 0）。",
            )
            return

        current_attempt = int((existing or {}).get("attempt", 0))
        if current_attempt >= self._max_retries:
            with self._lock:
                pending = self._load_pending_retries()
                pending.pop(norm_path, None)
                self._save_pending_retries(pending)
            self._increment_stat("retry_exhausted")
            self._append_record(
                norm_path,
                "重试耗尽",
                f"已达到最大重试次数 {self._max_retries} 次：{reason}",
            )
            logger.warning(
                f"【115秒传助手】{Path(norm_path).name} 已达到最大重试次数 {self._max_retries}，停止重试"
            )
            self._send_notice(
                "【115秒传助手】达到最大重试次数",
                f"文件：{media_title}\n路径：{norm_path}\n已重试 {self._max_retries} 次仍未秒传成功，已停止自动重试。",
            )
            return

        next_attempt = current_attempt + 1
        history_id = self._resolve_transfer_history_id(
            src_raw,
            src_storage,
            _extract_attr_or_key(data, "transfer_history_id"),
        )
        next_retry_at = time.time() + self._retry_delay
        entry = {
            "src_path": norm_path,
            "raw_src_path": src_raw,
            "src_storage": src_storage,
            "target_path": target_path,
            "history_id": history_id,
            "attempt": next_attempt,
            "max_retries": self._max_retries,
            "next_retry_at": next_retry_at,
            "last_error": reason,
            "title": media_title,
        }
        with self._lock:
            pending = self._load_pending_retries()
            pending[norm_path] = entry
            self._save_pending_retries(pending)

        job_id = self._make_retry_job_id(norm_path)
        self._schedule_once_job(
            job_id,
            self._retry_transfer_item,
            f"115秒传助手重试({next_attempt}/{self._max_retries}): {Path(norm_path).name}",
            delay_seconds=self._retry_delay,
            func_kwargs={"src_path": norm_path},
        )
        logger.info(
            f"【115秒传助手】{Path(norm_path).name} 将在 {self._retry_delay} 秒后进行第 "
            f"{next_attempt}/{self._max_retries} 次重试"
        )
        if next_attempt == 1:
            self._send_notice(
                "【115秒传助手】已安排延迟重试",
                f"文件：{media_title}\n原因：{reason}\n将在 {self._retry_delay} 秒后进行第 {next_attempt}/{self._max_retries} 次重试。",
            )

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Optional[Event] = None) -> None:
        """整理完成时清理待重试队列并发送重试成功通知。"""
        if not self._enabled or not event or not event.event_data:
            return
        data = event.event_data
        fileitem = _extract_attr_or_key(data, "fileitem")
        src_raw = _extract_attr_or_key(fileitem, "path")
        if not src_raw:
            return
        norm_path = _normalize_path(src_raw)
        with self._lock:
            self._rapid_failed_items.pop(norm_path, None)
            pending = self._load_pending_retries()
            removed = pending.pop(norm_path, None)
            if removed is not None:
                self._save_pending_retries(pending)
        if removed is not None:
            logger.info(
                f"【115秒传助手】{Path(norm_path).name} 重试秒传成功（第 {removed.get('attempt', 1)} 次），已移出待重试队列"
            )
            self._send_notice(
                "【115秒传助手】重试秒传成功",
                f"文件：{removed.get('title') or Path(norm_path).name}\n第 {removed.get('attempt', 1)} 次重试已成功完成整理。",
            )

    @eventmanager.register(EventType.PluginAction)
    def on_plugin_action(self, event: Optional[Event] = None) -> None:
        """响应远程命令立即触发待重试队列。"""
        if not self._enabled or not event or not event.event_data:
            return
        event_data = event.event_data
        if _extract_attr_or_key(event_data, "action") != "u115_rapid_retry":
            return
        triggered = self.retry_all_pending()
        logger.info(f"【115秒传助手】收到远程命令，已触发 {triggered} 个待重试任务")
        channel = _extract_attr_or_key(event_data, "channel")
        source = _extract_attr_or_key(event_data, "source")
        userid = _extract_attr_or_key(event_data, "user")
        self.post_message(
            channel=channel,
            source=source,
            userid=userid,
            title="【115秒传助手】手动重试",
            text=f"已触发 {triggered} 个待重试文件的整理重试。",
        )

    def _reschedule_persisted_retries(self) -> int:
        """插件启用或重载时为持久化队列中尚未到期的任务恢复一次性调度。"""
        pending = self._load_pending_retries()
        if not pending:
            return 0
        now = time.time()
        for norm_path, item in pending.items():
            delay = max(float(item.get("next_retry_at", now)) - now, 5.0)
            self._schedule_once_job(
                self._make_retry_job_id(norm_path),
                self._retry_transfer_item,
                f"115秒传助手恢复重试: {Path(norm_path).name}",
                delay_seconds=delay,
                func_kwargs={"src_path": norm_path},
            )
        return len(pending)

    def check_pending_retries(self) -> int:
        """巡检待重试队列，执行已到期的任务。"""
        if not self._enabled or self._stop_event.is_set():
            return 0
        now = time.time()
        self._last_scan_at = now
        pending = self._load_pending_retries()
        if not pending:
            return 0
        due_paths = [
            norm_path
            for norm_path, item in pending.items()
            if float(item.get("next_retry_at", 0)) <= now
        ]
        logger.debug(
            f"【115秒传助手】巡检待重试队列：共 {len(pending)} 项，到期 {len(due_paths)} 项"
        )
        executed = 0
        for norm_path in due_paths:
            if self._stop_event.is_set():
                break
            if self._retry_transfer_item(src_path=norm_path):
                executed += 1
        return executed

    def retry_all_pending(self) -> int:
        """立即执行当前所有待重试任务，返回成功投递数量。"""
        pending = self._load_pending_retries()
        count = 0
        for norm_path in list(pending.keys()):
            if self._retry_transfer_item(src_path=norm_path):
                count += 1
        return count

    def _retry_transfer_item(self, src_path: str) -> bool:
        """执行单个待重试文件的重新整理。"""
        if not self._enabled or self._stop_event.is_set():
            return False

        norm_path = _normalize_path(src_path)
        with self._lock:
            if norm_path in self._in_flight_retries:
                logger.debug(f"【115秒传助手】{Path(norm_path).name} 正在执行重试，跳过并发重入")
                return False
            self._in_flight_retries.add(norm_path)
            self._scheduled_job_ids.discard(self._make_retry_job_id(norm_path))
            pending = self._load_pending_retries()
            entry = pending.get(norm_path)

        try:
            if not entry:
                return False

            attempt = int(entry.get("attempt", 1))
            if self._max_retries == 0 or attempt > self._max_retries:
                logger.info(
                    f"【115秒传助手】{Path(norm_path).name} 当前尝试次数({attempt})超过最新上限({self._max_retries})，移除待重试项"
                )
                with self._lock:
                    pending = self._load_pending_retries()
                    pending.pop(norm_path, None)
                    self._save_pending_retries(pending)
                self._append_record(
                    norm_path,
                    "取消重试",
                    f"当前尝试次数 {attempt} 超过最大限制 {self._max_retries}",
                )
                return False

            raw_src = str(entry.get("raw_src_path") or norm_path)
            src_storage = str(entry.get("src_storage") or "local")

            if src_storage == "local" and not Path(raw_src).is_file():
                logger.info(f"【115秒传助手】本地源文件已不存在，移除待重试项：{raw_src}")
                with self._lock:
                    pending = self._load_pending_retries()
                    pending.pop(norm_path, None)
                    self._save_pending_retries(pending)
                self._append_record(norm_path, "已跳过", "本地源文件已不存在")
                return False

            history_oper = TransferHistoryOper()
            history = history_oper.get_by_src(raw_src, storage=src_storage)
            history_id = getattr(history, "id", None) if history is not None else entry.get("history_id")
            if history is not None and getattr(history, "status", False):
                logger.info(f"【115秒传助手】整理历史已成功，移除待重试项：{norm_path}")
                with self._lock:
                    pending = self._load_pending_retries()
                    pending.pop(norm_path, None)
                    self._save_pending_retries(pending)
                return False

            if not history_id:
                logger.warning(f"【115秒传助手】找不到整理历史记录，无法重试：{norm_path}")
                with self._lock:
                    pending = self._load_pending_retries()
                    pending.pop(norm_path, None)
                    self._save_pending_retries(pending)
                self._append_record(norm_path, "取消重试", "缺少对应的整理历史记录")
                return False

            logger.info(
                f"【115秒传助手】开始执行第 {attempt}/{self._max_retries} 次重试："
                f"{Path(norm_path).name} (history_id={history_id})"
            )
            self._append_record(norm_path, "发起重试", f"第 {attempt} 次重试（history_id={history_id}）")
            state, errmsg = TransferChain().redo_transfer_history(int(history_id))
            if not state:
                msg = str(errmsg or "未知错误")
                logger.warning(f"【115秒传助手】触发重试失败：{norm_path} - {msg}")
                if "不存在" in msg:
                    with self._lock:
                        pending = self._load_pending_retries()
                        pending.pop(norm_path, None)
                        self._save_pending_retries(pending)
                    self._append_record(norm_path, "取消重试", msg)
                else:
                    with self._lock:
                        pending = self._load_pending_retries()
                        if norm_path in pending:
                            pending[norm_path]["next_retry_at"] = time.time() + self._retry_delay
                            pending[norm_path]["last_error"] = msg
                            self._save_pending_retries(pending)
                    self._schedule_once_job(
                        self._make_retry_job_id(norm_path),
                        self._retry_transfer_item,
                        f"115秒传助手重试顺延: {Path(norm_path).name}",
                        delay_seconds=self._retry_delay,
                        func_kwargs={"src_path": norm_path},
                    )
                return False
            return True
        finally:
            with self._lock:
                self._in_flight_retries.discard(norm_path)

    def api_get_status(self) -> Response[RapidTransferStatusData]:
        """返回插件运行状态、统计与待重试队列。"""
        pending = self._load_pending_retries()
        return Response(
            success=True,
            message="",
            data=RapidTransferStatusData(
                enabled=self._enabled,
                stats=self._load_stats(),
                pending=list(pending.values()),
                records=self._load_records()[:20],
            ),
        )

    def api_trigger_retry(
        self, src_path: Optional[str] = None
    ) -> Response[RapidTransferActionData]:
        """立即触发指定文件或全部待重试任务的重新整理。"""
        if not self._enabled:
            return Response(
                success=False,
                message="插件未启用",
                data=RapidTransferActionData(count=0),
            )
        if src_path:
            ok = self._retry_transfer_item(src_path=src_path)
            logger.info(f"【115秒传助手】手动单条重试：{src_path} -> {'成功' if ok else '失败/未命中'}")
            return Response(
                success=ok,
                message="已触发重试" if ok else "未找到任务或触发失败",
                data=RapidTransferActionData(count=1 if ok else 0),
            )
        triggered = self.retry_all_pending()
        logger.info(f"【115秒传助手】手动全部重试：已触发 {triggered} 个任务")
        return Response(
            success=True,
            message=f"已触发 {triggered} 个重试任务",
            data=RapidTransferActionData(count=triggered),
        )

    def api_clear_pending(
        self, src_path: Optional[str] = None
    ) -> Response[RapidTransferActionData]:
        """清理指定文件或全部待重试队列。"""
        remove_job = getattr(scheduler_sdk, "remove_plugin_once_job", None)
        with self._lock:
            pending = self._load_pending_retries()
            if src_path:
                norm_path = _normalize_path(src_path)
                removed = pending.pop(norm_path, None)
                self._save_pending_retries(pending)
                job_ids = [self._make_retry_job_id(norm_path)] if removed else []
                self._scheduled_job_ids.difference_update(job_ids)
            else:
                job_ids = [self._make_retry_job_id(path) for path in pending]
                pending.clear()
                self._save_pending_retries(pending)
                self._scheduled_job_ids.difference_update(job_ids)
        if callable(remove_job):
            for job_id in job_ids:
                try:
                    remove_job(self.__class__.__name__, job_id)
                except Exception:
                    pass
        logger.info(f"【115秒传助手】清理待重试队列：共移除 {len(job_ids)} 项")
        return Response(
            success=True,
            message=f"已清理 {len(job_ids)} 个待重试任务",
            data=RapidTransferActionData(count=len(job_ids)),
        )

    def api_clear_records(self) -> Response[RapidTransferActionData]:
        """清空最近处理记录。"""
        with self._lock:
            records = self._load_records()
            count = len(records)
            self.save_data("records", [])
        logger.info(f"【115秒传助手】已清空 {count} 条处理记录")
        return Response(
            success=True,
            message=f"已清空 {count} 条处理记录",
            data=RapidTransferActionData(count=count),
        )
