"""Configuration helpers for bizytrd-sdk."""

from __future__ import annotations

from typing import Any, TypedDict
from urllib.parse import urlparse

DEFAULT_API_BASE_URL = "https://uat-api.bizyair.cn/x/v1"
DEFAULT_UPLOAD_BASE_URL = "https://uat-api.bizyair.cn/x/v1"
DEFAULT_TIMEOUT = 60
DEFAULT_POLLING_INTERVAL = 5.0
DEFAULT_MAX_POLLING_TIME = 1800


def _normalize_upload_base_url(upload_base_url: str, api_base_url: str) -> str:
    text = str(upload_base_url or api_base_url).rstrip("/")
    if text.endswith("/x/v1"):
        return text

    parsed = urlparse(text)
    if parsed.scheme and parsed.netloc and parsed.path in {"", "/"}:
        return f"{text}/x/v1"
    return text


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class BizyTRDConfig(TypedDict):
    base_url: str
    api_key: str
    upload_base_url: str
    timeout: int
    polling_interval: float
    max_polling_time: int
