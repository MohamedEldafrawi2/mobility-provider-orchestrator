"""State of the fictional Provider C, "mobility-async", in one SQLite file.

- Creates are deduplicated by **client reference**, bound before execution: a repeated
  reference replays the original outcome or reports "in progress". Records live 24 hours and
  survive a generation bump.
- A booking starts ``PENDING`` and progresses to ``CONFIRMED`` or ``FAILED`` on the provider's
  own schedule (the pending window), independent of any request expiry. Every change bumps the
  booking's ``sequence``; the provider's ``generation`` is reported on every response.
- The **fenced lookup** by client reference runs as a write transaction (``BEGIN IMMEDIATE``),
  inserts a fence row and answers finally; every create checks the fence inside its own write
  transaction and aborts with ``fenced`` if one exists.
- Cancellation is free and idempotent; it addresses one confirmed booking.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

STOPS: list[tuple[str, str, str, str]] = [
    ("MOB-BER", "Berlin Hauptbahnhof (shuttle stand)", "Europe/Berlin", "DE"),
    ("MOB-BER-AIR", "Berlin Brandenburg Airport", "Europe/Berlin", "DE"),
    ("MOB-HAM", "Hamburg Hauptbahnhof (shuttle stand)", "Europe/Berlin", "DE"),
    ("MOB-CPH", "Copenhagen Central (shuttle stand)", "Europe/Copenhagen", "DK"),
]

# product id, origin, destination, departure HH:MM, duration minutes, price cents, seats
PRODUCTS: list[tuple[str, str, str, str, int, int, int]] = [
    ("SHUTTLE-BER-AIR-0630", "MOB-BER", "MOB-BER-AIR", "06:30", 45, 1900, 8),
    ("SHUTTLE-BER-AIR-1130", "MOB-BER", "MOB-BER-AIR", "11:30", 45, 1900, 8),
    ("SHUTTLE-HAM-CPH-0800", "MOB-HAM", "MOB-CPH", "08:00", 330, 6900, 6),
    ("SHUTTLE-BER-HAM-0715", "MOB-BER", "MOB-HAM", "07:15", 180, 3900, 2),
]


class FencedError(Exception):
    """A fence exists for this client reference: the create must not commit."""


class ExpiredError(Exception):
    """The request's executeBefore passed before the mutation could commit."""


class SoldOutError(Exception):
    """Not enough seats left at the moment of the commit."""


@dataclass(frozen=True, slots=True)
class EventRow:
    event_id: str
    booking_id: str
    status: str
    sequence: int
    generation: int
    created_at: datetime
    payload: str
    delivered: bool
    attempts: int
    last_attempt_at: datetime | None


@dataclass(frozen=True, slots=True)
class RefRecord:
    client_ref: str
    state: str  # IN_PROGRESS | DONE
    status_code: int | None
    body: str | None


@dataclass(frozen=True, slots=True)
class BookingRow:
    booking_id: str
    client_ref: str
    product_id: str
    service_date: str
    status: str  # PENDING | CONFIRMED | FAILED | CANCELLED
    sequence: int
    passengers: int
    created_at: datetime
    due_at: datetime  # when the pending outcome is decided


class AsyncStore:
    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._lock = threading.RLock()
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS refs (
                client_ref TEXT PRIMARY KEY, state TEXT NOT NULL, status_code INTEGER, body TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fences (
                client_ref TEXT PRIMARY KEY, fenced_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bookings (
                booking_id TEXT PRIMARY KEY, client_ref TEXT NOT NULL, product_id TEXT NOT NULL,
                service_date TEXT NOT NULL, status TEXT NOT NULL, sequence INTEGER NOT NULL,
                passengers INTEGER NOT NULL, created_at TEXT NOT NULL, due_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_async_ref ON bookings(client_ref);
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, booking_id TEXT NOT NULL, status TEXT NOT NULL,
                sequence INTEGER NOT NULL, generation INTEGER NOT NULL, created_at TEXT NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0, payload TEXT NOT NULL DEFAULT '',
                attempts INTEGER NOT NULL DEFAULT 0, last_attempt_at TEXT
            );
            """
        )
        columns = {r[1] for r in self._conn.execute("PRAGMA table_info(events)").fetchall()}
        for name, ddl in (
            ("payload", "TEXT NOT NULL DEFAULT ''"),
            ("attempts", "INTEGER NOT NULL DEFAULT 0"),
            ("last_attempt_at", "TEXT"),
        ):
            if name not in columns:  # a database from before these columns existed
                self._conn.execute(f"ALTER TABLE events ADD COLUMN {name} {ddl}")
        self._counter = self._max_counter()

    def _max_counter(self) -> int:
        """Identifiers continue where a reopened database left off (never collide)."""
        best = 0
        for table, column in (("bookings", "booking_id"), ("events", "event_id")):
            for (value,) in self._conn.execute(f"SELECT {column} FROM {table}").fetchall():  # noqa: S608
                digits = "".join(ch for ch in str(value) if ch.isdigit())
                if digits:
                    best = max(best, int(digits))
        return best

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}{self._counter:06d}"

    # Catalogue ---------------------------------------------------------------------------------

    @staticmethod
    def stops(query: str) -> list[tuple[str, str, str, str]]:
        needle = query.strip().lower()
        return [s for s in STOPS if not needle or needle in s[1].lower()]

    @staticmethod
    def products(origin: str, destination: str) -> list[tuple[str, str, str, str, int, int, int]]:
        return [p for p in PRODUCTS if p[1] == origin and p[2] == destination]

    @staticmethod
    def product(product_id: str) -> tuple[str, str, str, str, int, int, int] | None:
        return next((p for p in PRODUCTS if p[0] == product_id), None)

    def seats_left(self, product_id: str, service_date: str) -> int | None:
        product = self.product(product_id)
        if product is None:
            return None
        with self._lock:
            taken = self._conn.execute(
                "SELECT COALESCE(SUM(passengers), 0) FROM bookings WHERE product_id = ? "
                "AND service_date = ? AND status IN ('PENDING', 'CONFIRMED')",
                (product_id, service_date),
            ).fetchone()[0]
        return int(product[6]) - int(taken)

    # Client references, bound before execution ------------------------------------------------

    def admit_ref(self, client_ref: str, *, now: datetime) -> RefRecord | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT client_ref, state, status_code, body FROM refs WHERE client_ref = ?",
                    (client_ref,),
                ).fetchone()
                if row is not None:
                    return RefRecord(row[0], row[1], row[2], row[3])
                self._conn.execute(
                    "INSERT INTO refs VALUES (?, 'IN_PROGRESS', NULL, NULL, ?)",
                    (client_ref, now.isoformat()),
                )
                return None
            finally:
                self._conn.execute("COMMIT")

    def finish_ref(self, client_ref: str, *, status_code: int, body: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE refs SET state = 'DONE', status_code = ?, body = ? WHERE client_ref = ?",
                (status_code, body, client_ref),
            )

    def release_ref(self, client_ref: str) -> None:
        """The execution was refused before anything was created (an expired request): the
        reference may be used again with a fresh expiry."""
        with self._lock:
            self._conn.execute(
                "DELETE FROM refs WHERE client_ref = ? AND state = 'IN_PROGRESS'", (client_ref,)
            )

    # Bookings ----------------------------------------------------------------------------------

    def create_booking(
        self,
        *,
        client_ref: str,
        product_id: str,
        service_date: str,
        passengers: int,
        now: datetime,
        due_at: datetime,
        execute_before: datetime | None = None,
        clock: Callable[[], datetime] | None = None,
        seats: int | None = None,
    ) -> BookingRow:
        """Commit inside one write transaction that checks the fence, the request's expiry (on
        a clock read inside the transaction) and the remaining capacity: a writer that paused
        past any of them never commits, and two concurrent writers never oversell."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if self._conn.execute(
                    "SELECT 1 FROM fences WHERE client_ref = ?", (client_ref,)
                ).fetchone():
                    raise FencedError(client_ref)
                if clock is not None:
                    now = clock()
                if execute_before is not None and now >= execute_before:
                    raise ExpiredError(client_ref)
                if seats is not None:
                    taken = self._conn.execute(
                        "SELECT COALESCE(SUM(passengers), 0) FROM bookings WHERE product_id = ? "
                        "AND service_date = ? AND status IN ('PENDING', 'CONFIRMED')",
                        (product_id, service_date),
                    ).fetchone()[0]
                    if int(taken) + passengers > seats:
                        raise SoldOutError(product_id)
                booking_id = self._next_id("MB")
                self._conn.execute(
                    "INSERT INTO bookings VALUES (?, ?, ?, ?, 'PENDING', 1, ?, ?, ?)",
                    (
                        booking_id,
                        client_ref,
                        product_id,
                        service_date,
                        passengers,
                        now.isoformat(),
                        due_at.isoformat(),
                    ),
                )
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")
        found = self.booking(booking_id)
        assert found is not None
        return found

    def booking(self, booking_id: str) -> BookingRow | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT booking_id, client_ref, product_id, service_date, status, sequence, "
                "passengers, created_at, due_at FROM bookings WHERE booking_id = ?",
                (booking_id,),
            ).fetchone()
        return self._row(row) if row else None

    def bookings_by_ref(self, client_ref: str) -> list[BookingRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT booking_id, client_ref, product_id, service_date, status, sequence, "
                "passengers, created_at, due_at FROM bookings WHERE client_ref = ? "
                "ORDER BY booking_id",
                (client_ref,),
            ).fetchall()
        return [self._row(r) for r in rows]

    def transition(
        self,
        booking_id: str,
        *,
        to: str,
        only_from: tuple[str, ...],
        execute_before: datetime | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> BookingRow | None:
        """Move a booking and bump its sequence; a no-op when it is not in ``only_from``. With
        ``execute_before``, the expiry is enforced inside the lock on a clock read there."""
        with self._lock:
            if execute_before is not None:
                now = clock() if clock is not None else datetime.now(UTC)
                if now >= execute_before:
                    raise ExpiredError(booking_id)
            placeholders = ",".join("?" * len(only_from))
            self._conn.execute(
                "UPDATE bookings SET status = ?, sequence = sequence + 1 "  # noqa: S608 - placeholders only
                f"WHERE booking_id = ? AND status IN ({placeholders})",
                (to, booking_id, *only_from),
            )
            return self.booking(booking_id)

    def due_pending(self, *, now: datetime) -> list[BookingRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT booking_id, client_ref, product_id, service_date, status, sequence, "
                "passengers, created_at, due_at FROM bookings "
                "WHERE status = 'PENDING' AND due_at <= ?",
                (now.isoformat(),),
            ).fetchall()
        return [self._row(r) for r in rows]

    # Fenced lookup -----------------------------------------------------------------------------

    def fenced_lookup(self, client_ref: str, *, now: datetime) -> list[BookingRow]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT OR IGNORE INTO fences VALUES (?, ?)", (client_ref, now.isoformat())
                )
                rows = self._conn.execute(
                    "SELECT booking_id, client_ref, product_id, service_date, status, sequence, "
                    "passengers, created_at, due_at FROM bookings WHERE client_ref = ? "
                    "ORDER BY booking_id",
                    (client_ref,),
                ).fetchall()
            finally:
                self._conn.execute("COMMIT")
        return [self._row(r) for r in rows]

    # Events (webhooks) ---------------------------------------------------------------------------

    def next_event_id(self) -> str:
        with self._lock:
            return self._next_id("evt_")

    def record_event(
        self,
        event_id: str,
        booking: BookingRow,
        *,
        generation: int,
        now: datetime,
        payload: str,
        delivered: bool = False,
    ) -> EventRow:
        """An event is an immutable fact: its payload is stored once and redelivered verbatim
        (only the delivery's timestamp and signature are fresh per attempt)."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO events (event_id, booking_id, status, sequence, generation, "
                "created_at, delivered, payload, attempts, last_attempt_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)",
                (
                    event_id,
                    booking.booking_id,
                    booking.status,
                    booking.sequence,
                    generation,
                    now.isoformat(),
                    1 if delivered else 0,
                    payload,
                ),
            )
        found = self.event(event_id)
        assert found is not None
        return found

    _EVENT_COLUMNS = (
        "event_id, booking_id, status, sequence, generation, created_at, payload, delivered, "
        "attempts, last_attempt_at"
    )

    def event(self, event_id: str) -> EventRow | None:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {self._EVENT_COLUMNS} FROM events WHERE event_id = ?",  # noqa: S608
                (event_id,),
            ).fetchone()
        return self._event(row) if row else None

    def event_rows(self) -> list[EventRow]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {self._EVENT_COLUMNS} FROM events ORDER BY event_id"  # noqa: S608
            ).fetchall()
        return [self._event(r) for r in rows]

    def events(self) -> list[dict[str, object]]:
        return [
            {
                "eventId": e.event_id,
                "bookingId": e.booking_id,
                "status": e.status,
                "sequence": e.sequence,
                "generation": e.generation,
                "createdAt": e.created_at.isoformat(),
                "delivered": e.delivered,
                "attempts": e.attempts,
            }
            for e in self.event_rows()
        ]

    def undelivered(self, *, max_attempts: int) -> list[EventRow]:
        """Events still owed to the receiver: never acknowledged, attempts left."""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {self._EVENT_COLUMNS} FROM events "  # noqa: S608
                "WHERE delivered = 0 AND attempts < ? ORDER BY event_id",
                (max_attempts,),
            ).fetchall()
        return [self._event(r) for r in rows]

    def claim_delivery(self, event_id: str, *, now: datetime) -> None:
        """Mark a first delivery as in flight (the attempt itself is recorded when it ends)."""
        with self._lock:
            self._conn.execute(
                "UPDATE events SET last_attempt_at = ? WHERE event_id = ? AND attempts = 0",
                (now.isoformat(), event_id),
            )

    def record_delivery_attempt(self, event_id: str, *, acknowledged: bool, now: datetime) -> None:
        """Only an acknowledged delivery (a non-5xx answer) completes the event; a failed one
        is counted and stays owed."""
        with self._lock:
            self._conn.execute(
                "UPDATE events SET attempts = attempts + 1, last_attempt_at = ?, "
                "delivered = CASE WHEN ? THEN 1 ELSE delivered END WHERE event_id = ?",
                (now.isoformat(), 1 if acknowledged else 0, event_id),
            )

    @staticmethod
    def _event(r: tuple[object, ...]) -> EventRow:
        return EventRow(
            str(r[0]),
            str(r[1]),
            str(r[2]),
            int(str(r[3])),
            int(str(r[4])),
            datetime.fromisoformat(str(r[5])),
            str(r[6]),
            bool(r[7]),
            int(str(r[8])),
            datetime.fromisoformat(str(r[9])) if r[9] else None,
        )

    # Truth ---------------------------------------------------------------------------------------

    def truth(self) -> list[BookingRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT booking_id, client_ref, product_id, service_date, status, sequence, "
                "passengers, created_at, due_at FROM bookings ORDER BY booking_id"
            ).fetchall()
        return [self._row(r) for r in rows]

    def wipe(self) -> None:
        with self._lock:
            self._conn.executescript(
                "DELETE FROM bookings; DELETE FROM refs; DELETE FROM fences; DELETE FROM events;"
            )

    @staticmethod
    def _row(r: tuple[object, ...]) -> BookingRow:
        return BookingRow(
            str(r[0]),
            str(r[1]),
            str(r[2]),
            str(r[3]),
            str(r[4]),
            int(str(r[5])),
            int(str(r[6])),
            datetime.fromisoformat(str(r[7])),
            datetime.fromisoformat(str(r[8])),
        )


def utcnow() -> datetime:
    return datetime.now(UTC)
