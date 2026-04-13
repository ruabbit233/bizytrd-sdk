"""DSL interpreter: ref resolution and condition evaluation."""

from __future__ import annotations

from typing import Any

from ..exceptions import AdapterError
from .utils import _is_blank


def _resolve_ref(
    ref: Any,
    kwargs: dict[str, Any],
    context: dict[str, Any],
    model_def: dict[str, Any],
) -> Any:
    if not isinstance(ref, dict):
        return ref
    if "const" in ref:
        return ref["const"]
    if "input" in ref:
        return kwargs.get(ref["input"])
    if "context" in ref:
        return context.get(ref["context"])
    if "model" in ref:
        return model_def.get(ref["model"])
    if "model_key" in ref:
        return model_def.get("model_key")
    recognised = {"const", "input", "context", "model", "model_key"}
    unrecognised = set(ref.keys()) - recognised
    if unrecognised:
        raise AdapterError(
            f"Unrecognised ref keys {unrecognised!r} in adapter definition: {ref!r}"
        )
    return None


def _evaluate_condition(
    condition: dict[str, Any] | None,
    kwargs: dict[str, Any],
    context: dict[str, Any],
    model_def: dict[str, Any],
) -> bool:
    if not condition:
        return True
    if "all" in condition:
        return all(
            _evaluate_condition(item, kwargs, context, model_def)
            for item in condition["all"]
        )
    if "any" in condition:
        return any(
            _evaluate_condition(item, kwargs, context, model_def)
            for item in condition["any"]
        )
    if "not" in condition:
        return not _evaluate_condition(condition["not"], kwargs, context, model_def)

    source = condition.get("source", "input")
    key = condition.get("key")
    op = condition.get("op", "exists")
    value = _resolve_ref({source: key}, kwargs, context, model_def)
    target = condition.get("value")

    if op == "exists":
        return value is not None
    if op == "non_empty":
        return not _is_blank(value)
    if op == "empty":
        return _is_blank(value)
    if op == "eq":
        return value == target
    if op == "ne":
        return value != target
    if op == "in":
        return value in (target or [])
    if op == "not_in":
        return value not in (target or [])
    if op == "gt":
        try:
            return value is not None and value > target
        except TypeError:
            return False
    if op == "gte":
        try:
            return value is not None and value >= target
        except TypeError:
            return False
    if op == "lt":
        try:
            return value is not None and value < target
        except TypeError:
            return False
    if op == "lte":
        try:
            return value is not None and value <= target
        except TypeError:
            return False
    if op == "is_true":
        return bool(value) is True
    if op == "is_false":
        return bool(value) is False
    raise AdapterError(f"Unsupported adapter condition op '{op}'")
