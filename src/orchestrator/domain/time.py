"""Timestamps inside the domain are always timezone-aware."""

from __future__ import annotations

from datetime import datetime


class NaiveDatetimeError(ValueError):
    pass


def require_aware(value: datetime | None, name: str) -> datetime | None:
    if value is not None and (value.tzinfo is None or value.tzinfo.utcoffset(value) is None):
        raise NaiveDatetimeError(f"{name} must be timezone-aware")
    return value
