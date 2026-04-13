"""Custom exception hierarchy for bizytrd."""

from __future__ import annotations


class BizyTRDError(Exception):
    """Base exception for all bizytrd errors."""


class ConfigError(BizyTRDError):
    """Raised when configuration is invalid or missing."""


class APIError(BizyTRDError):
    """Raised when an API request fails."""


class UploadError(BizyTRDError):
    """Raised when a file upload fails."""


class AdapterError(BizyTRDError):
    """Raised when payload adaptation or transform logic fails."""


class RegistryError(BizyTRDError):
    """Raised when the models registry is invalid or missing."""
