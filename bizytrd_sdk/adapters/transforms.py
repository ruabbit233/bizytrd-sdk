"""Transform registry and implementations for the adapters package."""

from __future__ import annotations

import json
from typing import Any, Callable

from ..exceptions import AdapterError
from .dsl import _evaluate_condition, _resolve_ref
from .utils import _coerce_int, _image_batches, _is_blank, _listify


def _transform_image_batches(value, params, kwargs, context, model_def, client):
    return _image_batches(value)


def _transform_count(value, params, kwargs, context, model_def, client):
    return len(value or [])


def _transform_collect_counted_inputs(
    value, params, kwargs, context, model_def, client
):
    count_value = _resolve_ref(
        params.get("count", {"const": 1}), kwargs, context, model_def
    )
    max_count = int(params.get("max_count", 1))
    input_count = max(1, min(_coerce_int(count_value, 1), max_count))
    extra_input_pattern = str(params.get("extra_input_pattern", "{index}"))
    item_transform = params.get("item_transform")

    collected: list[Any] = []
    collected.extend(
        _collect_transformed_items(
            value, item_transform, kwargs, context, model_def, client
        )
    )
    for index in range(2, input_count + 1):
        extra_value = kwargs.get(extra_input_pattern.format(index=index))
        collected.extend(
            _collect_transformed_items(
                extra_value, item_transform, kwargs, context, model_def, client
            )
        )
    return collected


def _transform_upload_media_list(value, params, kwargs, context, model_def, client):
    from .engine import _upload_media_value

    items = list(value or [])
    media_type = str(params["media_type"])
    options = {
        "file_name_prefix": params.get("file_name_prefix", "media_{index}"),
        "total_pixels": params.get("total_pixels", 10000 * 10000),
        "max_size": params.get("max_size", 20 * 1024 * 1024),
        "enforce_duration_range": params.get("enforce_duration_range"),
    }
    urls = []
    for index, item in enumerate(items, start=1):
        urls.append(
            _upload_media_value(
                item,
                media_type,
                client,
                params.get("input_name", "media"),
                options,
                index=index,
            )
        )
    return urls


def _transform_resolve_size(value, params, kwargs, context, model_def, client):
    size = str(value)
    custom_width = int(_resolve_ref(params["custom_width"], kwargs, context, model_def))
    custom_height = int(
        _resolve_ref(params["custom_height"], kwargs, context, model_def)
    )
    rules = params["rules"]

    if size != "Custom":
        presets = rules.get("presets", {})
        if size in presets:
            preset_rule = presets[size]
            current_model = kwargs.get("model") or model_def.get("model_key")
            allowed = preset_rule.get("allowed_models", [])
            if allowed and current_model not in allowed:
                raise ValueError(
                    preset_rule.get(
                        "error",
                        f"Size '{size}' is not supported for model '{current_model}'",
                    )
                )
            when = preset_rule.get("when")
            if when and not _evaluate_condition(when, kwargs, context, model_def):
                raise ValueError(
                    preset_rule.get(
                        "error",
                        f"Size '{size}' is not available under current conditions",
                    )
                )
        return size

    custom_rules = rules["custom"]
    total_pixels = custom_width * custom_height
    ratio = custom_width / custom_height

    min_aspect_ratio = custom_rules.get("min_aspect_ratio", 1 / 8)
    max_aspect_ratio = custom_rules.get("max_aspect_ratio", 8)
    if ratio < min_aspect_ratio or ratio > max_aspect_ratio:
        raise ValueError(
            f"Custom size aspect ratio must be between "
            f"1:{int(round(1 / min_aspect_ratio))} and "
            f"{int(max_aspect_ratio)}:1"
        )

    min_pixels = custom_rules.get("min_pixels", 768 * 768)
    if total_pixels < min_pixels:
        side = int(round(min_pixels**0.5))
        raise ValueError(f"Custom size total pixels must be at least {side}*{side}")

    max_pixels = custom_rules.get("default_max_pixels", 2048 * 2048)
    for override in custom_rules.get("max_pixels_overrides", []):
        if _evaluate_condition(override["when"], kwargs, context, model_def):
            max_pixels = override["max_pixels"]
            break

    if total_pixels > max_pixels:
        raise ValueError(
            f"Custom size total pixels exceed the current scene limit: {max_pixels}"
        )

    return f"{custom_width}*{custom_height}"


def _parse_bbox_list(bbox_list_str: str, image_count: int):
    if not bbox_list_str or not bbox_list_str.strip():
        return None
    try:
        bbox_list = json.loads(bbox_list_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"bbox_list must be valid JSON: {exc}") from exc
    if not isinstance(bbox_list, list):
        raise ValueError("bbox_list must be a JSON array")
    if len(bbox_list) != image_count:
        raise ValueError(
            "bbox_list length must exactly match the number of input images"
        )
    for image_boxes in bbox_list:
        if not isinstance(image_boxes, list):
            raise ValueError("Each bbox_list item must be an array")
        if len(image_boxes) > 2:
            raise ValueError("Each input image supports at most 2 bounding boxes")
        for box in image_boxes:
            if (
                not isinstance(box, list)
                or len(box) != 4
                or not all(isinstance(v, int) for v in box)
            ):
                raise ValueError(
                    "Each bounding box must be a list of 4 integers: [x1, y1, x2, y2]"
                )
    return bbox_list


def _transform_parse_bbox_list(value, params, kwargs, context, model_def, client):
    image_count = int(
        _resolve_ref(params["image_count"], kwargs, context, model_def) or 0
    )
    return _parse_bbox_list(str(value or ""), image_count)


def _parse_color_palette(color_palette_str: str, enable_sequential: bool):
    if not color_palette_str or not color_palette_str.strip():
        return None
    if enable_sequential:
        raise ValueError(
            "color_palette is only supported when enable_sequential is false"
        )
    try:
        color_palette = json.loads(color_palette_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"color_palette must be valid JSON: {exc}") from exc
    if not isinstance(color_palette, list):
        raise ValueError("color_palette must be a JSON array")
    if len(color_palette) < 3 or len(color_palette) > 10:
        raise ValueError("color_palette must contain between 3 and 10 colors")
    ratio_sum = 0.0
    for color in color_palette:
        if not isinstance(color, dict):
            raise ValueError("Each color_palette item must be an object")
        hex_value = color.get("hex", "")
        ratio_value = color.get("ratio", "")
        if (
            not isinstance(hex_value, str)
            or len(hex_value) != 7
            or not hex_value.startswith("#")
        ):
            raise ValueError(
                "Each color_palette item must contain a hex value like #C2D1E6"
            )
        if not isinstance(ratio_value, str) or not ratio_value.endswith("%"):
            raise ValueError(
                'Each color_palette item must contain a ratio string like "23.51%"'
            )
        try:
            ratio_sum += float(ratio_value[:-1])
        except ValueError as exc:
            raise ValueError(
                f"Invalid color ratio value in color_palette: {ratio_value}"
            ) from exc
    if abs(ratio_sum - 100.0) > 0.05:
        raise ValueError("color_palette ratios must sum to 100.00%")
    return color_palette


def _transform_parse_color_palette(value, params, kwargs, context, model_def, client):
    enable_sequential = bool(
        _resolve_ref(params["enable_sequential"], kwargs, context, model_def)
    )
    return _parse_color_palette(str(value or ""), enable_sequential)


TRANSFORMS: dict[str, Callable[..., Any]] = {
    "image_batches": _transform_image_batches,
    "count": _transform_count,
    "collect_counted_inputs": _transform_collect_counted_inputs,
    "upload_media_list": _transform_upload_media_list,
    "resolve_size": _transform_resolve_size,
    "parse_bbox_list": _transform_parse_bbox_list,
    "parse_color_palette": _transform_parse_color_palette,
}


def _apply_transform(
    value: Any,
    transform_spec: str | dict[str, Any],
    kwargs: dict[str, Any],
    context: dict[str, Any],
    model_def: dict[str, Any],
    client: Any,
) -> Any:
    if isinstance(transform_spec, str):
        name = transform_spec
        params: dict[str, Any] = {}
    else:
        name = transform_spec["name"]
        params = dict(transform_spec)

    handler = TRANSFORMS.get(name)
    if handler is None:
        raise AdapterError(f"Unsupported adapter transform '{name}'")
    return handler(value, params, kwargs, context, model_def, client)


def _collect_transformed_items(
    value: Any,
    item_transform: str | dict[str, Any] | None,
    kwargs: dict[str, Any],
    context: dict[str, Any],
    model_def: dict[str, Any],
    client: Any,
) -> list[Any]:
    if _is_blank(value):
        return []
    if item_transform is None:
        return _listify(value)
    transformed = _apply_transform(
        value, item_transform, kwargs, context, model_def, client
    )
    return _listify(transformed)


def _apply_transforms(
    value: Any,
    spec: dict[str, Any],
    kwargs: dict[str, Any],
    context: dict[str, Any],
    model_def: dict[str, Any],
    client: Any,
) -> Any:
    transforms: list[Any] = []
    if "transform" in spec:
        transforms.append(spec["transform"])
    transforms.extend(spec.get("transforms", []))
    for transform_spec in transforms:
        value = _apply_transform(
            value, transform_spec, kwargs, context, model_def, client
        )
    return value
