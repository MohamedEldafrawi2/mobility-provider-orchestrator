"""Provider B's time policy (docs/provider-integration-guide.md).

The legacy API gives naive local ``HH:MM`` times, a service date, and an arrival day offset.
A local time that falls in a DST gap (does not exist) or a DST fold (exists twice) is rejected
rather than guessed; the search result reports it as a warning.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo


class AmbiguousLocalTimeError(ValueError):
    pass


def localize(service_date: date, hhmm: str, zone: str, *, day_offset: int = 0) -> datetime:
    hour, minute = (int(part) for part in hhmm.split(":"))
    tz = ZoneInfo(zone)
    naive = datetime.combine(service_date + timedelta(days=day_offset), time(hour, minute))
    aware = naive.replace(tzinfo=tz)
    # Gap first: a wall time that does not exist does not round-trip through UTC.
    if aware.astimezone(UTC).astimezone(tz).replace(tzinfo=None) != naive:
        raise AmbiguousLocalTimeError(f"{naive} does not exist in {zone} (DST gap)")
    # Fold: the same wall time maps to two instants.
    if aware.replace(fold=1).utcoffset() != aware.utcoffset():
        raise AmbiguousLocalTimeError(f"{naive} is ambiguous in {zone} (DST fold)")
    return aware
