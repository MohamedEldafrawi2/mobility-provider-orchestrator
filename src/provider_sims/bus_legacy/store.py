"""Provider B's own storage: SQLite, so reservations survive a restart.

Inventory is per journey per service date. The by-reference index lags writes
(``visible_after``), and a reservation can be flagged as never indexed. Both are what make
Provider B the worst case the platform must handle. Capacity is checked and consumed inside
one write transaction, so concurrent slow reservations cannot oversell.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

# (id, name, tz, country)
STOPS: list[tuple[int, str, str, str]] = [
    (101, "Roma Tiburtina", "Europe/Rome", "IT"),
    (102, "Milano Lampugnano", "Europe/Rome", "IT"),
    (103, "Firenze Villa Costanza", "Europe/Rome", "IT"),
    (201, "Paris Bercy", "Europe/Paris", "FR"),
    (202, "Lyon Perrache", "Europe/Paris", "FR"),
    (301, "Berlin ZOB", "Europe/Berlin", "DE"),
]

# (journey id, src, dst, dep HH:MM, arr HH:MM, arrival day offset, price cents, seats per date)
JOURNEYS: list[tuple[str, int, int, str, str, int, int, int]] = [
    ("BUS-ROM-MIL-0715", 101, 102, "07:15", "14:05", 0, 2350, 40),
    ("BUS-ROM-MIL-2350", 101, 102, "23:50", "06:10", 1, 1890, 40),
    ("BUS-MIL-ROM-0800", 102, 101, "08:00", "14:50", 0, 2350, 40),
    ("BUS-ROM-FIR-0930", 101, 103, "09:30", "12:45", 0, 1450, 30),
    ("BUS-PAR-LYO-1000", 201, 202, "10:00", "15:30", 0, 2900, 50),
    ("BUS-MIL-BER-2100", 102, 301, "21:00", "11:30", 1, 4990, 45),
    ("BUS-ROM-MIL-0230", 101, 102, "02:30", "09:15", 0, 1500, 2),  # DST trap, nearly full
]


@dataclass(frozen=True)
class ReservationRow:
    res_id: int
    your_ref: str
    jid: str
    service_date: str  # DD-MM-YYYY as the legacy API spells it
    pax: int
    committed_at: datetime
    visible_after: datetime | None  # None: never indexed


class BusStore:
    def __init__(self, path: Path | str) -> None:
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        self._init()

    SCHEMA_VERSION = 2

    def _init(self) -> None:
        with self._lock:
            self._migrate()
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS stops (
                    id INTEGER PRIMARY KEY, name TEXT, tz TEXT, country TEXT
                );
                CREATE TABLE IF NOT EXISTS journeys (
                    jid TEXT PRIMARY KEY, src INTEGER, dst INTEGER, dep TEXT, arr TEXT,
                    arr_day_offset INTEGER, price_cents INTEGER, seats INTEGER
                );
                CREATE TABLE IF NOT EXISTS reservations (
                    res_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    your_ref TEXT NOT NULL,
                    jid TEXT NOT NULL,
                    service_date TEXT NOT NULL,
                    pax INTEGER NOT NULL,
                    committed_at TEXT NOT NULL,
                    visible_after TEXT
                );
                CREATE INDEX IF NOT EXISTS ix_res_your_ref ON reservations(your_ref);
                CREATE INDEX IF NOT EXISTS ix_res_journey ON reservations(jid, service_date);
                """
            )
            self._conn.executemany("INSERT OR IGNORE INTO stops VALUES (?, ?, ?, ?)", STOPS)
            self._conn.executemany(
                "INSERT OR IGNORE INTO journeys VALUES (?, ?, ?, ?, ?, ?, ?, ?)", JOURNEYS
            )
            self._conn.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")

    def _migrate(self) -> None:
        """Bring an older file forward without losing a single reservation (truth is sacred)."""
        tables = {
            r[0]
            for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if "reservations" in tables:
            columns = {
                r[1] for r in self._conn.execute("PRAGMA table_info(reservations)").fetchall()
            }
            if "service_date" not in columns:
                self._conn.execute(
                    "ALTER TABLE reservations ADD COLUMN service_date TEXT NOT NULL "
                    "DEFAULT '01-01-1970'"
                )
        if "stops" in tables:
            columns = {r[1] for r in self._conn.execute("PRAGMA table_info(stops)").fetchall()}
            if "country" not in columns:
                self._conn.execute("ALTER TABLE stops ADD COLUMN country TEXT NOT NULL DEFAULT ''")
                self._conn.executemany(
                    "UPDATE stops SET country = ? WHERE id = ?", [(c, i) for i, _, _, c in STOPS]
                )

    # Reads ---------------------------------------------------------------------------------

    def stops(self, query: str) -> list[tuple[int, str, str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, name, tz, country FROM stops WHERE lower(name) LIKE ? ORDER BY id",
                (f"%{query.lower()}%",),
            ).fetchall()
        return [(int(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in rows]

    def journeys(self, src: int, dst: int) -> list[tuple[str, int, int, str, str, int, int, int]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT jid, src, dst, dep, arr, arr_day_offset, price_cents, seats "
                "FROM journeys WHERE src = ? AND dst = ? ORDER BY dep",
                (src, dst),
            ).fetchall()
        return [tuple(r) for r in rows]

    def journey_exists(self, jid: str) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM journeys WHERE jid = ?", (jid,)).fetchone()
        return row is not None

    def seats_left(self, jid: str, service_date: str) -> int | None:
        with self._lock:
            return self._seats_left_locked(jid, service_date)

    def _seats_left_locked(self, jid: str, service_date: str) -> int | None:
        row = self._conn.execute("SELECT seats FROM journeys WHERE jid = ?", (jid,)).fetchone()
        if row is None:
            return None
        taken = self._conn.execute(
            "SELECT COALESCE(SUM(pax), 0) FROM reservations WHERE jid = ? AND service_date = ?",
            (jid, service_date),
        ).fetchone()[0]
        return int(row[0]) - int(taken)

    # Writes --------------------------------------------------------------------------------

    def reserve(
        self,
        your_ref: str,
        jid: str,
        service_date: str,
        pax: int,
        *,
        lag: timedelta,
        never_index: bool,
    ) -> ReservationRow | None:
        """Commit a reservation, or return None if the journey is sold out.

        Capacity is re-checked inside the write transaction. Legacy semantics otherwise: no
        idempotency on ``your_ref`` at all.
        """
        now = datetime.now(UTC)
        visible_after = None if never_index else now + lag
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                left = self._seats_left_locked(jid, service_date)
                if left is None or left < pax:
                    self._conn.execute("ROLLBACK")
                    return None
                cur = self._conn.execute(
                    "INSERT INTO reservations "
                    "(your_ref, jid, service_date, pax, committed_at, visible_after) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        your_ref,
                        jid,
                        service_date,
                        pax,
                        now.isoformat(),
                        visible_after.isoformat() if visible_after else None,
                    ),
                )
                res_id = int(cur.lastrowid or 0)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return ReservationRow(res_id, your_ref, jid, service_date, pax, now, visible_after)

    def by_your_ref(self, your_ref: str, *, now: datetime) -> list[ReservationRow]:
        """What the lagging index exposes right now."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT res_id, your_ref, jid, service_date, pax, committed_at, visible_after "
                "FROM reservations WHERE your_ref = ? ORDER BY res_id",
                (your_ref,),
            ).fetchall()
        out: list[ReservationRow] = []
        for r in rows:
            visible_after = datetime.fromisoformat(r[6]) if r[6] else None
            if visible_after is not None and visible_after <= now:
                out.append(self._row(r))
        return out

    def truth(self) -> list[ReservationRow]:
        """Every reservation regardless of index visibility. For test oracles only."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT res_id, your_ref, jid, service_date, pax, committed_at, visible_after "
                "FROM reservations ORDER BY res_id"
            ).fetchall()
        return [self._row(r) for r in rows]

    @staticmethod
    def _row(r: tuple[object, ...]) -> ReservationRow:
        return ReservationRow(
            int(str(r[0])),
            str(r[1]),
            str(r[2]),
            str(r[3]),
            int(str(r[4])),
            datetime.fromisoformat(str(r[5])),
            datetime.fromisoformat(str(r[6])) if r[6] else None,
        )

    def rebuild_index(self, *, now: datetime) -> int:
        """Make every reservation visible now, including never-indexed ones. Returns count."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE reservations SET visible_after = ? "
                "WHERE visible_after IS NULL OR visible_after > ?",
                (now.isoformat(), now.isoformat()),
            )
            self._conn.commit()
            return int(cur.rowcount or 0)

    def wipe_reservations(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM reservations")
