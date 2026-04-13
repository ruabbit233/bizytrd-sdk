"""Validator registry and implementations for the adapters package."""

from __future__ import annotations

from typing import Any, Callable

from ..exceptions import AdapterError
from .dsl import _evaluate_condition, _resolve_ref
from .utils import _is_blank


def _validate_max_count(validator, kwargs, context, model_def, current_value):
    max_count = int(validator["max"])
    count = len(current_value or [])
    message = validator.get("message")
    if count > max_count:
        raise ValueError(message or f"Maximum count is {max_count}")


def _validate_int_range(validator, kwargs, context, model_def, current_value):
    if current_value is None:
        return
    minimum = int(validator["min"])
    maximum = int(validator["max"])
    integer = int(current_value)
    message = validator.get("message")
    if integer < minimum or integer > maximum:
        raise ValueError(message or f"Value must be between {minimum} and {maximum}")


def _validate_require_any_non_empty(
    validator, kwargs, context, model_def, current_value
):
    refs = validator.get("refs", [])
    message = validator.get("message")
    if not any(
        not _is_blank(_resolve_ref(ref, kwargs, context, model_def)) for ref in refs
    ):
        raise ValueError(message or "At least one value is required")


VALIDATORS: dict[str, Callable[..., Any]] = {
    "max_count": _validate_max_count,
    "int_range": _validate_int_range,
    "require_any_non_empty": _validate_require_any_non_empty,
}


def _run_validator(
    validator: dict[str, Any],
    kwargs: dict[str, Any],
    context: dict[str, Any],
    model_def: dict[str, Any],
    current_value: Any = None,
) -> None:
    if not _evaluate_condition(validator.get("when"), kwargs, context, model_def):
        return

    name = validator["name"]
    value = (
        _resolve_ref(validator["value"], kwargs, context, model_def)
        if "value" in validator
        else current_value
    )

    handler = VALIDATORS.get(name)
    if handler is None:
        raise AdapterError(f"Unsupported adapter validator '{name}'")
    handler(validator, kwargs, context, model_def, value)
