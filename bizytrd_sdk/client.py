"""bizytrd-sdk: ComfyUI-agnostic SDK for BizyAir TRD task submission and polling."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any, Callable

from .adapters import build_payload_for_model as _sdk_build_payload_for_model
from .config import (
    DEFAULT_API_BASE_URL,
    DEFAULT_MAX_POLLING_TIME,
    DEFAULT_POLLING_INTERVAL,
    DEFAULT_TIMEOUT,
    BizyTRDConfig,
    _normalize_upload_base_url,
)
from .exceptions import AdapterError
from .task import poll_task as _sdk_poll_task
from .task import submit_task as _sdk_submit_task
from .upload import (
    _is_remote_reference,
    _normalize_local_path,
    request_upload_token as _sdk_request_upload_token,
    upload_bytes as _sdk_upload_bytes,
    upload_local_file as _sdk_upload_local_file,
)


class BizyTRDClient:
    def __init__(
        self,
        *,
        api_key: str = "",
        base_url: str = DEFAULT_API_BASE_URL,
        upload_base_url: str = "",
        timeout: int = DEFAULT_TIMEOUT,
        polling_interval: float = DEFAULT_POLLING_INTERVAL,
        max_polling_time: int = DEFAULT_MAX_POLLING_TIME,
    ):
        self._config: BizyTRDConfig = {
            "base_url": str(base_url).rstrip("/"),
            "api_key": str(api_key).strip(),
            "upload_base_url": _normalize_upload_base_url(
                upload_base_url or base_url, base_url
            ),
            "timeout": timeout,
            "polling_interval": polling_interval,
            "max_polling_time": max_polling_time,
        }
        self._media_handlers: dict[str, Callable[..., Any]] = {}

    @property
    def config(self) -> BizyTRDConfig:
        return self._config

    def register_media_handler(
        self, media_type: str, handler: Callable[..., Any]
    ) -> None:
        self._media_handlers[media_type] = handler

    def normalize_media_input(
        self, value: Any, media_type: str, input_name: str, **kwargs
    ) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            text = value.strip()
            if _is_remote_reference(text):
                return text
            local_path = _normalize_local_path(text)
            if local_path is not None:
                return self.upload_local_file(local_path, file_name=local_path.name)
        handler = self._media_handlers.get(media_type)
        if handler:
            return handler(value, input_name=input_name, client=self, **kwargs)
        raise AdapterError(
            f"Unsupported media type '{media_type}' or no handler registered for non-string input of type {type(value).__name__}"
        )

    def upload_image_input(
        self,
        value: Any,
        *,
        file_name_prefix: str,
        total_pixels: int = 10000 * 10000,
        max_size: int = 20 * 1024 * 1024,
        **kwargs,
    ) -> str:
        if isinstance(value, str):
            text = value.strip()
            if _is_remote_reference(text):
                return text
            local_path = _normalize_local_path(text)
            if local_path is not None:
                return self.upload_local_file(
                    local_path,
                    file_name=f"{file_name_prefix}{local_path.suffix or '.webp'}",
                )
        handler = self._media_handlers.get("IMAGE")
        if handler:
            return handler(
                value, file_name_prefix=file_name_prefix, client=self, **kwargs
            )
        raise ValueError(
            f"Cannot handle IMAGE input of type {type(value).__name__}. Register a handler with client.register_media_handler('IMAGE', handler)"
        )

    def upload_video_input(
        self,
        value: Any,
        *,
        file_name_prefix: str,
        max_size: int = 100 * 1024 * 1024,
        enforce_duration_range: tuple[float, float] | None = None,
        **kwargs,
    ) -> str:
        if isinstance(value, str):
            text = value.strip()
            if _is_remote_reference(text):
                return text
            local_path = _normalize_local_path(text)
            if local_path is not None:
                return self.upload_local_file(
                    local_path,
                    file_name=f"{file_name_prefix}{local_path.suffix or '.mp4'}",
                )
        handler = self._media_handlers.get("VIDEO")
        if handler:
            return handler(
                value,
                file_name_prefix=file_name_prefix,
                client=self,
                enforce_duration_range=enforce_duration_range,
                **kwargs,
            )
        raise ValueError(
            f"Cannot handle VIDEO input of type {type(value).__name__}. Register a handler with client.register_media_handler('VIDEO', handler)"
        )

    def upload_audio_input(
        self,
        value: Any,
        *,
        file_name_prefix: str,
        format: str = "mp3",
        max_size: int = 50 * 1024 * 1024,
        **kwargs,
    ) -> str:
        if isinstance(value, str):
            text = value.strip()
            if _is_remote_reference(text):
                return text
            local_path = _normalize_local_path(text)
            if local_path is not None:
                return self.upload_local_file(
                    local_path,
                    file_name=f"{file_name_prefix}{local_path.suffix or '.mp3'}",
                )
        handler = self._media_handlers.get("AUDIO")
        if handler:
            return handler(
                value,
                file_name_prefix=file_name_prefix,
                client=self,
                format=format,
                **kwargs,
            )
        raise ValueError(
            f"Cannot handle AUDIO input of type {type(value).__name__}. Register a handler with client.register_media_handler('AUDIO', handler)"
        )

    def request_upload_token(
        self, file_name: str, *, file_type: str = "inputs"
    ) -> dict[str, Any]:
        return _sdk_request_upload_token(file_name, self._config, file_type=file_type)

    def upload_bytes(
        self, file_content: io.BytesIO, file_name: str, *, file_type: str = "inputs"
    ) -> str:
        return _sdk_upload_bytes(
            file_content, file_name, self._config, file_type=file_type
        )

    def upload_local_file(self, path: Path, *, file_name: str | None = None) -> str:
        return _sdk_upload_local_file(path, self._config, file_name=file_name)

    def submit_task(
        self, model_key: str, payload: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        return _sdk_submit_task(model_key, payload, self._config)

    def poll_task(self, request_id: str) -> dict[str, Any]:
        return _sdk_poll_task(request_id, self._config)

    def build_payload(
        self, model_def: dict[str, Any], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        return _sdk_build_payload_for_model(
            model_def, self._config, kwargs, client=self
        )
