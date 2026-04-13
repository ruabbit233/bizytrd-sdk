"""Utility helpers for the adapters package."""

from __future__ import annotations

from typing import Any


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    if hasattr(value, "numel"):
        try:
            return value.numel() == 0
        except (AttributeError, TypeError):
            pass
    if hasattr(value, "shape"):
        try:
            return any(dim == 0 for dim in value.shape)
        except (AttributeError, TypeError):
            pass
    return False


def _image_batches(images: Any) -> list[Any]:
    if images is None:
        return []
    if hasattr(images, "shape"):
        if len(images.shape) > 3:
            return [images[i] for i in range(images.shape[0])]
        return [images]
    if isinstance(images, (list, tuple)):
        batches: list[Any] = []
        for item in images:
            if item is None:
                continue
            if hasattr(item, "shape") and len(item.shape) > 3:
                batches.extend(item[i] for i in range(item.shape[0]))
            else:
                batches.append(item)
        return batches
    return [images]


def _listify(value: Any) -> list[Any]:
    if _is_blank(value):
        return []
    if isinstance(value, (list, tuple)):
        return [item for item in value if not _is_blank(item)]
    return [value]


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, (list, tuple)):
        if not value:
            return default
        value = value[0]
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
