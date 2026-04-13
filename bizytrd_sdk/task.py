"""Task submission and polling for bizytrd-sdk."""

from __future__ import annotations

import random
import time
from typing import Any

from .config import BizyTRDConfig
from .exceptions import APIError
from .http_utils import json_or_raise

SUCCESS_CODE = 20000
ACCEPTED_CODE = 20002
RUNNING_STATUSES = {"running", "saving"}
SUCCESS_STATUSES = {"success"}
FAILED_STATUSES = {"failed"}
MAX_UNKNOWN_STATUS_COUNT = 10
MAX_TRANSIENT_RETRIES = 3
JITTER_FACTOR = 0.25


def _extract_request_id(data: dict[str, Any]) -> str:
    request_id = (
        (data.get("data") or {}).get("request_id")
        or (data.get("data") or {}).get("requestId")
        or data.get("request_id")
        or data.get("requestId")
    )
    if not request_id:
        raise APIError(f"No request_id in submit response: {data}")
    return str(request_id)


def _sleep_with_jitter(interval: float) -> None:
    jitter = interval * JITTER_FACTOR * (2 * random.random() - 1)
    time.sleep(max(0.5, interval + jitter))


def _request_with_retry(method: str, url: str, **kwargs: Any) -> Any:
    import requests

    last_exc: Exception | None = None
    for attempt in range(MAX_TRANSIENT_RETRIES):
        try:
            return requests.request(method, url, **kwargs)
        except requests.ConnectionError as exc:
            last_exc = exc
            if attempt < MAX_TRANSIENT_RETRIES - 1:
                _sleep_with_jitter(2**attempt)
    raise APIError(
        f"Request failed after {MAX_TRANSIENT_RETRIES} retries: {url}"
    ) from last_exc


def submit_task(
    model_key: str, payload: dict[str, Any], config: BizyTRDConfig
) -> tuple[str, dict[str, Any]]:
    url = f"{config['base_url'].rstrip('/')}/trd_api/{model_key}"
    headers = {"Content-Type": "application/json"}
    if config.get("api_key"):
        headers["Authorization"] = f"Bearer {config['api_key']}"

    response = _request_with_retry(
        "POST", url, json=payload, headers=headers, timeout=config["timeout"]
    )
    data = json_or_raise(response)

    if response.status_code >= 400:
        raise APIError(f"Submit failed: HTTP {response.status_code}, body={data}")
    if data.get("status") is False:
        raise APIError(f"Submit failed: {data.get('message') or data}")
    if data.get("code") not in (SUCCESS_CODE, ACCEPTED_CODE, None):
        raise APIError(f"Submit failed: code={data.get('code')} body={data}")

    return _extract_request_id(data), data


def poll_task(request_id: str, config: BizyTRDConfig) -> dict[str, Any]:
    url = f"{config['base_url'].rstrip('/')}/trd_api/{request_id}"
    headers: dict[str, str] = {}
    if config.get("api_key"):
        headers["Authorization"] = f"Bearer {config['api_key']}"

    started_at = time.time()
    interval = float(config["polling_interval"])
    timeout_seconds = int(config["max_polling_time"])
    last_payload: dict[str, Any] | None = None
    unknown_status_count = 0
    consecutive_errors = 0

    while True:
        if time.time() - started_at > timeout_seconds:
            raise APIError(
                f"Task polling timed out after {timeout_seconds}s for request_id={request_id}. "
                f"Last payload={last_payload}"
            )

        try:
            response = _request_with_retry(
                "GET", url, headers=headers, timeout=min(config["timeout"], 30)
            )
            payload = json_or_raise(response)
            consecutive_errors = 0
        except APIError:
            consecutive_errors += 1
            if consecutive_errors >= MAX_TRANSIENT_RETRIES:
                raise
            _sleep_with_jitter(interval)
            continue

        last_payload = payload

        if response.status_code >= 400:
            raise APIError(f"Poll failed: HTTP {response.status_code}, body={payload}")
        if payload.get("status") is False:
            raise APIError(f"Poll failed: {payload.get('message') or payload}")

        data = payload.get("data") or {}
        status = str(data.get("status") or "").strip().lower()

        if status in SUCCESS_STATUSES:
            return payload
        if status in FAILED_STATUSES:
            raise APIError(
                data.get("message")
                or payload.get("message")
                or f"Task failed: {payload}"
            )
        if not status or status in RUNNING_STATUSES:
            unknown_status_count = 0
            _sleep_with_jitter(interval)
            continue

        unknown_status_count += 1
        if unknown_status_count >= MAX_UNKNOWN_STATUS_COUNT:
            raise APIError(
                f"Task encountered unknown status '{status}' too many times "
                f"({unknown_status_count}) for request_id={request_id}. "
                f"Last payload={last_payload}"
            )
        _sleep_with_jitter(interval)
