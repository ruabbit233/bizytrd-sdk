"""Shared HTTP utility helpers for bizytrd."""

from __future__ import annotations

from typing import Any


def json_or_raise(response: Any) -> dict[str, Any]:
    """Parse JSON from an HTTP response, raising a clear error on failure."""
    try:
        return response.json() if response.text else {}
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid JSON response: HTTP {response.status_code}, "
            f"body={response.text[:500]}"
        ) from exc
