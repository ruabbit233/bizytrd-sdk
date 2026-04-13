"""Upload and media normalization helpers for bizytrd-sdk."""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import io
import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from .config import BizyTRDConfig
from .exceptions import ConfigError, UploadError
from .http_utils import json_or_raise

_logger = logging.getLogger(__name__)


CLIENT_VERSION = "0.1.0"


def _is_remote_reference(text: str) -> bool:
    return text.startswith(("http://", "https://", "data:"))


def _normalize_local_path(text: str) -> Path | None:
    if text.startswith("file://"):
        return Path(urlparse(text).path)

    candidate = Path(text).expanduser()
    if candidate.exists():
        return candidate
    return None


def _auth_headers(config: BizyTRDConfig) -> dict[str, str]:
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "User-Agent": "BizyAir Client",
        "x-bizyair-client-version": CLIENT_VERSION,
    }
    if config.get("api_key"):
        headers["authorization"] = f"Bearer {config['api_key']}"
    return headers


def _process_response_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if "result" not in payload:
        return payload

    try:
        message = json.loads(payload["result"])
    except json.JSONDecodeError as exc:
        raise UploadError(
            f"Failed to decode JSON from response payload: {payload}"
        ) from exc

    if "request_id" in payload and "request_id" not in message:
        message["request_id"] = payload["request_id"]
    return message


def _sign_request(
    method: str,
    bucket: str,
    object_key: str,
    headers: dict[str, str],
    access_key_id: str,
    access_key_secret: str,
) -> str:
    canonical_string = f"{method}\n"
    canonical_string += f"{headers.get('Content-MD5', '')}\n"
    canonical_string += f"{headers.get('Content-Type', '')}\n"
    canonical_string += f"{headers.get('Date', '')}\n"

    for key in sorted(headers.keys()):
        if key.lower().startswith("x-oss-"):
            canonical_string += f"{key.lower()}:{headers[key]}\n"

    canonical_string += f"/{bucket}/{object_key}"

    digest = hmac.new(
        access_key_secret.encode("utf-8"),
        canonical_string.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    signature = base64.b64encode(digest).decode("utf-8")
    return f"OSS {access_key_id}:{signature}"


def parse_upload_token(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError(f"Upload token response missing data: {payload}")

    file_info = data.get("file")
    storage = data.get("storage")
    if not isinstance(file_info, dict) or not isinstance(storage, dict):
        raise ValueError(f"Upload token response missing file/storage: {payload}")

    return file_info | storage


def _upload_file_without_sdk(
    file_content: io.BytesIO,
    bucket: str,
    object_key: str,
    endpoint: str,
    access_key_id: str,
    access_key_secret: str,
    security_token: str,
    **_: Any,
) -> str:
    import requests

    file_content.seek(0)
    date = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    headers = {
        "Host": f"{bucket}.{endpoint}",
        "Date": date,
        "Content-Type": "application/octet-stream",
        "Content-Length": str(file_content.getbuffer().nbytes),
        "x-oss-security-token": security_token,
    }
    headers["Authorization"] = _sign_request(
        "PUT",
        bucket,
        object_key,
        headers,
        access_key_id,
        access_key_secret,
    )

    url = f"https://{bucket}.{endpoint}/{object_key}"
    response = requests.put(url, headers=headers, data=file_content, timeout=300)
    if response.status_code >= 400:
        raise UploadError(
            f"Bizy upload failed: HTTP {response.status_code}, body={response.text[:500]}"
        )
    return url


def request_upload_token(
    file_name: str,
    config: BizyTRDConfig,
    *,
    file_type: str = "inputs",
) -> dict[str, Any]:
    import requests

    if not str(config.get("api_key") or "").strip():
        raise ConfigError(
            "BizyTRD API key is empty. Set `BIZYAIR_API_KEY` (preferred), "
            "`BIZYTRD_API_KEY`, or place BizyAir's `api_key.ini` where BizyAir "
            "can load it so bizytrd can reuse the shared credential."
        )

    base_url = str(config["upload_base_url"]).rstrip("/")
    url = f"{base_url}/upload/token?file_name={quote(file_name)}&file_type={quote(file_type)}"
    response = requests.get(
        url,
        headers=_auth_headers(config),
        timeout=max(int(config.get("timeout", 60)), 10),
    )
    payload = _process_response_payload(json_or_raise(response))

    if response.status_code >= 400:
        raise UploadError(
            f"Upload token request failed: HTTP {response.status_code}, body={payload}"
        )
    if payload.get("status") is False:
        raise UploadError(f"Upload token request failed: {payload}")

    return parse_upload_token(payload)


def upload_bytes(
    file_content: io.BytesIO,
    file_name: str,
    config: BizyTRDConfig,
    *,
    file_type: str = "inputs",
) -> str:
    auth_info = request_upload_token(file_name, config, file_type=file_type)
    return _upload_file_without_sdk(file_content=file_content, **auth_info)


def upload_local_file(
    path: Path,
    config: BizyTRDConfig,
    *,
    file_name: str | None = None,
) -> str:
    with path.open("rb") as handle:
        file_bytes = io.BytesIO(handle.read())
    return upload_bytes(file_bytes, file_name or path.name, config)
