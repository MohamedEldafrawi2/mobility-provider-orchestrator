"""State of the fictional Provider A, "rail-osdm", in one SQLite file.

The two properties the platform's design leans on are implemented here, not faked:

- **The idempotency key is bound before execution.** ``admit_key`` records the key as
  ``IN_PROGRESS`` in its own write transaction before anything executes; a repeated key
  either replays the recorded outcome or reports "in progress". Records survive a generation
  bump and live for the documented window.
- **The fenced lookup serializes with writers.** SQLite has one writer at a time, so a fenced
  lookup runs as a write transaction (``BEGIN IMMEDIATE``): it waits for any in-flight
  mutation on that key to commit or roll back, inserts a fence row, reads what the key
  produced, and commits. Every mutation for a key checks the fence table inside its own write
  transaction and aborts with ``fenced`` if a fence exists. A writer paused after its expiry
  check but before its commit is therefore either seen by the lookup or aborted by it.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

PLACES: list[tuple[str, str, str, str]] = [
    ("8500010", "Basel SBB", "Europe/Zurich", "CH"),
    ("8503000", "Zürich HB", "Europe/Zurich", "CH"),
    ("8507000", "Bern", "Europe/Zurich", "CH"),
    ("8300000", "Milano Centrale", "Europe/Rome", "IT"),
    ("8700000", "Paris Gare de Lyon", "Europe/Paris", "FR"),
]

# trip id, origin, destination, departure HH:MM, arrival HH:MM, base price cents, seats
TRIPS: list[tuple[str, str, str, str, str, int, int]] = [
    ("IC-BS-ZH-0704", "8500010", "8503000", "07:04", "08:00", 3400, 40),
    ("IC-BS-ZH-0904", "8500010", "8503000", "09:04", "10:00", 3400, 2),
    ("EC-ZH-MI-0933", "8503000", "8300000", "09:33", "13:20", 8900, 30),
    ("TGV-PA-BS-0723", "8700000", "8500010", "07:23", "10:26", 11900, 30),
    ("IC-ZH-BE-0802", "8503000", "8507000", "08:02", "08:58", 2600, 50),
]


class FencedError(Exception):
    """A fence exists for this key: the mutation must not commit."""


class ExpiredError(Exception):
    """The request's executeBefore passed before the mutation could commit."""


@dataclass(frozen=True, slots=True)
class KeyRecord:
    key: str
    state: str  # IN_PROGRESS | DONE
    status_code: int | None
    body: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class BookingRow:
    booking_id: str
    external_ref: str
    offer_id: str
    trip_id: str
    service_date: str  # ISO date
    status: str  # PREBOOKED | CONFIRMED | CANCELLED | EXPIRED
    confirmation_time_limit: datetime
    version: int
    key: str
    passengers: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class RefundOfferRow:
    refund_offer_id: str
    booking_id: str
    status: str  # PROPOSED | CONFIRMED | EXPIRED | REJECTED
    valid_until: datetime
    refund_cents: int
    fee_cents: int


class RailStore:
    SCHEMA_VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._lock = threading.RLock()
        self._ensure_schema()
        self._counter = self._max_counter()

    # Schema --------------------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS offers (
                    offer_id TEXT PRIMARY KEY,
                    trip_id TEXT NOT NULL,
                    service_date TEXT NOT NULL,
                    adults INTEGER NOT NULL,
                    children INTEGER NOT NULL,
                    price_cents INTEGER NOT NULL,
                    refundable INTEGER NOT NULL,
                    fee_percent INTEGER NOT NULL,
                    valid_until TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS keys (
                    key TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    status_code INTEGER,
                    body TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fences (
                    key TEXT PRIMARY KEY,
                    fenced_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bookings (
                    booking_id TEXT PRIMARY KEY,
                    external_ref TEXT NOT NULL,
                    offer_id TEXT NOT NULL,
                    trip_id TEXT NOT NULL,
                    service_date TEXT NOT NULL,
                    status TEXT NOT NULL,
                    confirmation_time_limit TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    key TEXT NOT NULL,
                    passengers INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_bookings_ref ON bookings(external_ref);
                CREATE INDEX IF NOT EXISTS ix_bookings_key ON bookings(key);
                CREATE TABLE IF NOT EXISTS refund_offers (
                    refund_offer_id TEXT PRIMARY KEY,
                    booking_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    valid_until TEXT NOT NULL,
                    refund_cents INTEGER NOT NULL,
                    fee_cents INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_refund_booking ON refund_offers(booking_id);
                """
            )
            self._conn.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")

    def _max_counter(self) -> int:
        """Identifiers continue where a reopened database left off (never collide)."""
        best = 0
        for table, column in (
            ("offers", "offer_id"),
            ("bookings", "booking_id"),
            ("refund_offers", "refund_offer_id"),
        ):
            for (value,) in self._conn.execute(f"SELECT {column} FROM {table}").fetchall():  # noqa: S608
                digits = "".join(ch for ch in str(value) if ch.isdigit())
                if digits:
                    best = max(best, int(digits))
        return best

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}{self._counter:06d}"

    # Catalogue -----------------------------------------------------------------------------

    @staticmethod
    def places(name: str) -> list[tuple[str, str, str, str]]:
        needle = name.strip().lower()
        return [p for p in PLACES if not needle or needle in p[1].lower()]

    @staticmethod
    def place(place_id: str) -> tuple[str, str, str, str] | None:
        return next((p for p in PLACES if p[0] == place_id), None)

    @staticmethod
    def trips(origin: str, destination: str) -> list[tuple[str, str, str, str, str, int, int]]:
        return [t for t in TRIPS if t[1] == origin and t[2] == destination]

    def seats_left(self, trip_id: str, service_date: str) -> int | None:
        trip = next((t for t in TRIPS if t[0] == trip_id), None)
        if trip is None:
            return None
        with self._lock:
            taken = self._conn.execute(
                "SELECT COALESCE(SUM(passengers), 0) FROM bookings "
                "WHERE trip_id = ? AND service_date = ? AND status IN ('PREBOOKED', 'CONFIRMED')",
                (trip_id, service_date),
            ).fetchone()[0]
        return int(trip[6]) - int(taken)

    def create_offer(
        self,
        *,
        trip_id: str,
        service_date: str,
        adults: int,
        children: int,
        price_cents: int,
        refundable: bool,
        fee_percent: int,
        valid_until: datetime,
    ) -> str:
        with self._lock:
            offer_id = self._next_id("OF")
            self._conn.execute(
                "INSERT INTO offers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    offer_id,
                    trip_id,
                    service_date,
                    adults,
                    children,
                    price_cents,
                    int(refundable),
                    fee_percent,
                    valid_until.isoformat(),
                ),
            )
        return offer_id

    def offer(self, offer_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT offer_id, trip_id, service_date, adults, children, price_cents, "
                "refundable, fee_percent, valid_until FROM offers WHERE offer_id = ?",
                (offer_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "offer_id": row[0],
            "trip_id": row[1],
            "service_date": row[2],
            "adults": int(row[3]),
            "children": int(row[4]),
            "price_cents": int(row[5]),
            "refundable": bool(row[6]),
            "fee_percent": int(row[7]),
            "valid_until": datetime.fromisoformat(row[8]),
        }

    # Idempotency keys, bound before execution ------------------------------------------------

    def admit_key(self, key: str, *, now: datetime) -> KeyRecord | None:
        """Bind the key now. Returns the existing record if the key was seen before."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT key, state, status_code, body, created_at FROM keys WHERE key = ?",
                    (key,),
                ).fetchone()
                if row is not None:
                    return KeyRecord(row[0], row[1], row[2], row[3], datetime.fromisoformat(row[4]))
                self._conn.execute(
                    "INSERT INTO keys VALUES (?, 'IN_PROGRESS', NULL, NULL, ?)",
                    (key, now.isoformat()),
                )
                return None
            finally:
                self._conn.execute("COMMIT")

    def finish_key(self, key: str, *, status_code: int, body: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE keys SET state = 'DONE', status_code = ?, body = ? WHERE key = ?",
                (status_code, body, key),
            )

    def release_key(self, key: str) -> None:
        """The execution failed before anything was created: the key may be used again."""
        with self._lock:
            self._conn.execute("DELETE FROM keys WHERE key = ? AND state = 'IN_PROGRESS'", (key,))

    def expire_keys(self, *, older_than: datetime) -> int:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM keys WHERE created_at < ?", (older_than.isoformat(),)
            )
            return int(cur.rowcount or 0)

    # Bookings --------------------------------------------------------------------------------

    def create_booking(
        self,
        *,
        key: str,
        external_ref: str,
        offer_id: str,
        trip_id: str,
        service_date: str,
        passengers: int,
        confirmation_time_limit: datetime,
        now: datetime,
        execute_before: datetime | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> BookingRow:
        """Commit the hold inside one write transaction that also checks the fence and the
        request's expiry: a writer that paused past either never commits. ``clock`` is read
        inside the transaction, so time spent waiting for the lock counts against the request."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if self._conn.execute("SELECT 1 FROM fences WHERE key = ?", (key,)).fetchone():
                    raise FencedError(key)
                if clock is not None:
                    now = clock()
                if execute_before is not None and now >= execute_before:
                    raise ExpiredError(key)
                booking_id = self._next_id("RB")
                self._conn.execute(
                    "INSERT INTO bookings VALUES (?, ?, ?, ?, ?, 'PREBOOKED', ?, 1, ?, ?, ?)",
                    (
                        booking_id,
                        external_ref,
                        offer_id,
                        trip_id,
                        service_date,
                        confirmation_time_limit.isoformat(),
                        key,
                        passengers,
                        now.isoformat(),
                    ),
                )
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")
        found = self.booking(booking_id, now=now)
        assert found is not None
        return found

    def booking(self, booking_id: str, *, now: datetime) -> BookingRow | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT booking_id, external_ref, offer_id, trip_id, service_date, status, "
                "confirmation_time_limit, version, key, passengers, created_at "
                "FROM bookings WHERE booking_id = ?",
                (booking_id,),
            ).fetchone()
        if row is None:
            return None
        booking = self._row(row)
        return self._expire_if_due(booking, now=now)

    def bookings_by_external_ref(self, external_ref: str, *, now: datetime) -> list[BookingRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT booking_id, external_ref, offer_id, trip_id, service_date, status, "
                "confirmation_time_limit, version, key, passengers, created_at "
                "FROM bookings WHERE external_ref = ? ORDER BY booking_id",
                (external_ref,),
            ).fetchall()
        return [self._expire_if_due(self._row(r), now=now) for r in rows]

    def _expire_if_due(self, booking: BookingRow, *, now: datetime) -> BookingRow:
        """A hold past its limit is expired the moment anyone looks (lazily, durably)."""
        if booking.status == "PREBOOKED" and now >= booking.confirmation_time_limit:
            with self._lock:
                self._conn.execute(
                    "UPDATE bookings SET status = 'EXPIRED', version = version + 1 "
                    "WHERE booking_id = ? AND status = 'PREBOOKED'",
                    (booking.booking_id,),
                )
            refreshed = self.booking_raw(booking.booking_id)
            assert refreshed is not None
            return refreshed
        return booking

    def booking_raw(self, booking_id: str) -> BookingRow | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT booking_id, external_ref, offer_id, trip_id, service_date, status, "
                "confirmation_time_limit, version, key, passengers, created_at "
                "FROM bookings WHERE booking_id = ?",
                (booking_id,),
            ).fetchone()
        return self._row(row) if row else None

    def confirm(
        self,
        booking_id: str,
        *,
        now: datetime,
        execute_before: datetime | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> BookingRow | None:
        """CONFIRMED if the hold is live; idempotent when already confirmed. The write
        transaction is taken first and the clock is read inside it, so neither the lock wait
        nor a pause can let a request commit after its expiry or after the hold's deadline."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if clock is not None:
                    now = clock()
                booking = self.booking_raw(booking_id)
                if booking is not None:
                    if booking.status == "PREBOOKED" and now >= booking.confirmation_time_limit:
                        self._conn.execute(
                            "UPDATE bookings SET status = 'EXPIRED', version = version + 1 "
                            "WHERE booking_id = ? AND status = 'PREBOOKED'",
                            (booking_id,),
                        )
                    elif execute_before is not None and now >= execute_before:
                        raise ExpiredError(booking_id)
                    elif booking.status == "PREBOOKED":
                        self._conn.execute(
                            "UPDATE bookings SET status = 'CONFIRMED', version = version + 1 "
                            "WHERE booking_id = ? AND status = 'PREBOOKED'",
                            (booking_id,),
                        )
                result = self.booking_raw(booking_id) if booking is not None else None
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")
            return result

    def cancel(self, booking_id: str) -> BookingRow | None:
        with self._lock:
            self._conn.execute(
                "UPDATE bookings SET status = 'CANCELLED', version = version + 1 "
                "WHERE booking_id = ? AND status = 'CONFIRMED'",
                (booking_id,),
            )
            return self.booking_raw(booking_id)

    # Fenced lookup ---------------------------------------------------------------------------

    def fenced_lookup(self, key: str, *, now: datetime) -> list[BookingRow]:
        """Fence the key and return what it produced. Final: nothing later can commit for it."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT OR IGNORE INTO fences VALUES (?, ?)", (key, now.isoformat())
                )
                rows = self._conn.execute(
                    "SELECT booking_id, external_ref, offer_id, trip_id, service_date, status, "
                    "confirmation_time_limit, version, key, passengers, created_at "
                    "FROM bookings WHERE key = ? ORDER BY booking_id",
                    (key,),
                ).fetchall()
            finally:
                self._conn.execute("COMMIT")
        return [self._expire_if_due(self._row(r), now=now) for r in rows]

    def is_fenced(self, key: str) -> bool:
        with self._lock:
            return bool(self._conn.execute("SELECT 1 FROM fences WHERE key = ?", (key,)).fetchone())

    # Refund offers ---------------------------------------------------------------------------

    def create_refund_offer(
        self, booking_id: str, *, refund_cents: int, fee_cents: int, valid_until: datetime
    ) -> RefundOfferRow:
        with self._lock:
            refund_id = self._next_id("RF")
            self._conn.execute(
                "INSERT INTO refund_offers VALUES (?, ?, 'PROPOSED', ?, ?, ?)",
                (refund_id, booking_id, valid_until.isoformat(), refund_cents, fee_cents),
            )
        found = self.refund_offer(refund_id, now=valid_until - timedelta(seconds=1))
        assert found is not None
        return found

    def refund_offer(self, refund_id: str, *, now: datetime) -> RefundOfferRow | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT refund_offer_id, booking_id, status, valid_until, refund_cents, fee_cents "
                "FROM refund_offers WHERE refund_offer_id = ?",
                (refund_id,),
            ).fetchone()
            if row is None:
                return None
            offer = RefundOfferRow(
                row[0], row[1], row[2], datetime.fromisoformat(row[3]), int(row[4]), int(row[5])
            )
            if offer.status == "PROPOSED" and now >= offer.valid_until:
                self._conn.execute(
                    "UPDATE refund_offers SET status = 'EXPIRED' WHERE refund_offer_id = ?",
                    (refund_id,),
                )
                offer = RefundOfferRow(
                    offer.refund_offer_id,
                    offer.booking_id,
                    "EXPIRED",
                    offer.valid_until,
                    offer.refund_cents,
                    offer.fee_cents,
                )
            return offer

    def refund_offers_for(self, booking_id: str, *, now: datetime) -> list[RefundOfferRow]:
        with self._lock:
            ids = [
                r[0]
                for r in self._conn.execute(
                    "SELECT refund_offer_id FROM refund_offers WHERE booking_id = ? "
                    "ORDER BY refund_offer_id",
                    (booking_id,),
                ).fetchall()
            ]
        out = []
        for refund_id in ids:
            found = self.refund_offer(refund_id, now=now)
            if found is not None:
                out.append(found)
        return out

    def accept_refund_offer(
        self,
        refund_id: str,
        *,
        now: datetime,
        execute_before: datetime | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> RefundOfferRow | None:
        """Accept a live proposed offer: the offer is CONFIRMED and the booking CANCELLED, in
        one write transaction; every other proposed offer for the booking is REJECTED. The
        expiry is enforced inside the lock, on a clock read inside the lock."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")  # the lock first, then the clock and the checks
            try:
                if clock is not None:
                    now = clock()
                offer = self.refund_offer(refund_id, now=now)
                if offer is None:
                    self._conn.execute("ROLLBACK")
                    return None
                if offer.status != "PROPOSED":
                    self._conn.execute("ROLLBACK")
                    return offer
                if execute_before is not None and now >= execute_before:
                    raise ExpiredError(refund_id)
                self._conn.execute(
                    "UPDATE refund_offers SET status = 'CONFIRMED' WHERE refund_offer_id = ?",
                    (refund_id,),
                )
                self._conn.execute(
                    "UPDATE refund_offers SET status = 'REJECTED' "
                    "WHERE booking_id = ? AND refund_offer_id != ? AND status = 'PROPOSED'",
                    (offer.booking_id, refund_id),
                )
                self._conn.execute(
                    "UPDATE bookings SET status = 'CANCELLED', version = version + 1 "
                    "WHERE booking_id = ? AND status = 'CONFIRMED'",
                    (offer.booking_id,),
                )
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")
            return self.refund_offer(refund_id, now=now)

    # Truth -----------------------------------------------------------------------------------

    def truth(self) -> list[BookingRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT booking_id, external_ref, offer_id, trip_id, service_date, status, "
                "confirmation_time_limit, version, key, passengers, created_at "
                "FROM bookings ORDER BY booking_id"
            ).fetchall()
        return [self._row(r) for r in rows]

    def wipe(self) -> None:
        with self._lock:
            self._conn.executescript(
                "DELETE FROM bookings; DELETE FROM keys; DELETE FROM fences; "
                "DELETE FROM refund_offers; DELETE FROM offers;"
            )

    @staticmethod
    def _row(r: tuple[object, ...]) -> BookingRow:
        return BookingRow(
            str(r[0]),
            str(r[1]),
            str(r[2]),
            str(r[3]),
            str(r[4]),
            str(r[5]),
            datetime.fromisoformat(str(r[6])),
            int(str(r[7])),
            str(r[8]),
            int(str(r[9])),
            datetime.fromisoformat(str(r[10])),
        )


def utcnow() -> datetime:
    return datetime.now(UTC)
