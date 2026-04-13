"""Orchestration engine for the adapters package."""

from __future__ import annotations

from typing import Any

from ..config import BizyTRDConfig
from ..exceptions import AdapterError
from .dsl import _evaluate_condition, _resolve_ref
from .transforms import _apply_transforms
from .utils import _is_blank
from .validators import _run_validator


def _default_adapter(
    model_def: dict[str, Any],
    config: BizyTRDConfig,
    kwargs: dict[str, Any],
    *,
    client: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"model": model_def["model_key"]}
    for param in model_def.get("params", []):
        input_name = param["name"]
        api_field = param.get("api_field", input_name)
        value = kwargs.get(input_name)
        if value is None:
            continue
        param_type = param.get("type", "STRING")
        if param_type in {"IMAGE", "VIDEO", "AUDIO"}:
            payload[api_field] = client.normalize_media_input(
                value,
                param_type,
                input_name,
            )
        else:
            payload[api_field] = value
    return payload


def _upload_media_value(
    value: Any,
    media_type: str,
    client: Any,
    input_name: str,
    options: dict[str, Any],
    *,
    index: int | None = None,
) -> str:
    file_name_prefix = str(options.get("file_name_prefix", input_name))
    if index is not None:
        file_name_prefix = file_name_prefix.format(index=index)

    if media_type == "IMAGE":
        return client.upload_image_input(
            value,
            file_name_prefix=file_name_prefix,
            total_pixels=int(options.get("total_pixels", 10000 * 10000)),
            max_size=int(options.get("max_size", 20 * 1024 * 1024)),
        )
    if media_type == "VIDEO":
        duration_range = options.get("enforce_duration_range")
        if isinstance(duration_range, list):
            duration_range = tuple(duration_range)
        return client.upload_video_input(
            value,
            file_name_prefix=file_name_prefix,
            max_size=int(options.get("max_size", 100 * 1024 * 1024)),
            enforce_duration_range=duration_range,
        )
    if media_type == "AUDIO":
        return client.upload_audio_input(
            value,
            file_name_prefix=file_name_prefix,
            format=str(options.get("format", "mp3")),
            max_size=int(options.get("max_size", 50 * 1024 * 1024)),
        )
    return client.normalize_media_input(value, media_type, input_name)


def _build_media_array(
    items: list[dict[str, Any]],
    kwargs: dict[str, Any],
    context: dict[str, Any],
    model_def: dict[str, Any],
    client: Any,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in items:
        if not _evaluate_condition(item.get("when"), kwargs, context, model_def):
            continue
        value = _resolve_ref(
            item.get("value") or {"input": item["input"]}, kwargs, context, model_def
        )
        if _is_blank(value):
            if item.get("required"):
                raise ValueError(
                    item.get("message") or f"'{item.get('input', 'value')}' is required"
                )
            continue

        media_type = item["media_type"]
        upload_options = {
            "file_name_prefix": item.get(
                "file_name_prefix", item.get("input", "media")
            ),
            "total_pixels": item.get("total_pixels", 10000 * 10000),
            "max_size": item.get("max_size", 20 * 1024 * 1024),
            "enforce_duration_range": item.get("enforce_duration_range"),
        }
        url = _upload_media_value(
            value,
            media_type,
            client,
            item.get("input", "media"),
            upload_options,
        )
        result.append(
            {
                "type": item["item_type"],
                "url": url,
            }
        )
    return result


def _build_context(
    context_specs: list[dict[str, Any]],
    kwargs: dict[str, Any],
    model_def: dict[str, Any],
    client: Any,
) -> dict[str, Any]:
    context: dict[str, Any] = {}
    for spec in context_specs:
        if not _evaluate_condition(spec.get("when"), kwargs, context, model_def):
            continue
        value = _resolve_ref(spec, kwargs, context, model_def)
        value = _apply_transforms(value, spec, kwargs, context, model_def, client)
        for validator in spec.get("validators", []):
            _run_validator(validator, kwargs, context, model_def, current_value=value)
        context[spec["name"]] = value
    return context


def _structured_adapter(
    model_def: dict[str, Any],
    config: BizyTRDConfig,
    kwargs: dict[str, Any],
    *,
    client: Any,
) -> dict[str, Any]:
    adapter = model_def.get("adapter") or {}
    context = _build_context(adapter.get("context", []), kwargs, model_def, client)

    for validator in adapter.get("validators", []):
        _run_validator(validator, kwargs, context, model_def)

    payload: dict[str, Any] = {}
    for item in adapter.get("payload", []):
        if not _evaluate_condition(item.get("when"), kwargs, context, model_def):
            continue

        if item.get("build") == "media_array":
            value = _build_media_array(
                item.get("items", []), kwargs, context, model_def, client
            )
        else:
            value = _resolve_ref(item, kwargs, context, model_def)
            value = _apply_transforms(value, item, kwargs, context, model_def, client)

        for validator in item.get("validators", []):
            _run_validator(validator, kwargs, context, model_def, current_value=value)

        if item.get("skip_if_blank") and _is_blank(value):
            continue
        if value is None and item.get("skip_if_none", False):
            continue
        payload[item["field"]] = value

    if "model" not in payload:
        payload["model"] = model_def["model_key"]
    return payload


def build_payload_for_model(
    model_def: dict[str, Any],
    config: BizyTRDConfig,
    kwargs: dict[str, Any],
    *,
    client: Any,
) -> dict[str, Any]:
    adapter = model_def.get("adapter")
    if not adapter or adapter == "default":
        return _default_adapter(model_def, config, kwargs, client=client)
    if isinstance(adapter, dict):
        kind = adapter.get("kind", "structured")
        if kind == "structured":
            return _structured_adapter(model_def, config, kwargs, client=client)
        raise AdapterError(f"Unsupported adapter kind '{kind}'")
    raise AdapterError(f"Unsupported adapter definition '{adapter}'")
