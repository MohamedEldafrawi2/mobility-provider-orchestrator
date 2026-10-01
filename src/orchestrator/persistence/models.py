"""SQLAlchemy models. The schema is owned by Alembic migrations; these classes mirror it.

The booking row is also the work source (`next_action_at`, lease columns), so the worker's
claims and fencing are just row updates. Commands, attempts, review cases, and evidence mirror
the domain aggregates of docs/architecture.md; the domain objects are rebuilt from these
rows and never persisted by reference.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    type_annotation_map = {dict[str, Any]: JSON}


class ApiClient(Base):
    """A client of the public API. Bookings reference ``client_id`` as their owner."""

    __tablename__ = "api_clients"

    client_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class BookingRow(Base):
    __tablename__ = "bookings"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    client_id: Mapped[str] = mapped_column(ForeignKey("api_clients.client_id"), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False)
    offer_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    passengers: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    contact_email: Mapped[str] = mapped_column(String(200), nullable=False)
    provider_booking_ref: Mapped[str | None] = mapped_column(String(128))
    unresolved_reason: Mapped[str | None] = mapped_column(String(64))
    failure_code: Mapped[str | None] = mapped_column(String(64))
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Hold then confirm, generation and revision ordering, cancellation
    confirmation_deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_generation: Mapped[int | None] = mapped_column(Integer)
    last_revision: Mapped[int | None] = mapped_column(Integer)
    confirm_budget_remaining: Mapped[int | None] = mapped_column(Integer)
    refund: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    # Work source for the worker
    next_action_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[str | None] = mapped_column(String(40))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("ix_bookings_client", "client_id", "created_at"),
        Index("ix_bookings_work", "next_action_at", "lease_expires_at"),
        Index("ix_bookings_state", "state"),
    )


class CommandRow(Base):
    __tablename__ = "commands"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    booking_id: Mapped[str] = mapped_column(ForeignKey("bookings.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    intent: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    provider_key: Mapped[str] = mapped_column(String(128), nullable=False)
    first_dispatch_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    execution_cutoff: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    disposition: Mapped[str] = mapped_column(String(16), nullable=False)
    basis: Mapped[str | None] = mapped_column(String(16))
    submission_ref: Mapped[str | None] = mapped_column(String(128))
    lookups_performed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    phase: Mapped[str | None] = mapped_column(String(16))
    quote: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    target_ref: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_commands_booking", "booking_id", "kind"),)


class AttemptRow(Base):
    __tablename__ = "attempts"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    command_id: Mapped[str] = mapped_column(ForeignKey("commands.id"), nullable=False)
    n: Mapped[int] = mapped_column(Integer, nullable=False)
    request: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    dispatch_marked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(16))
    side_effect: Mapped[str | None] = mapped_column(String(16))
    error: Mapped[str | None] = mapped_column(String(500))
    excluded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (UniqueConstraint("command_id", "n", name="uq_attempts_command_n"),)


class ReviewCaseRow(Base):
    __tablename__ = "review_cases"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    booking_id: Mapped[str] = mapped_column(ForeignKey("bookings.id"), nullable=False)
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    remediable: Mapped[bool] = mapped_column(Boolean, nullable=False)
    outstanding_command_id: Mapped[str | None] = mapped_column(ForeignKey("commands.id"))
    implicated: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)  # {"refs": [...]}
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolution: Mapped[str | None] = mapped_column(String(24))
    resolved_by: Mapped[str | None] = mapped_column(String(64))
    closed_version: Mapped[int | None] = mapped_column(Integer)

    __table_args__ = (Index("ix_review_open", "closed_at", "opened_at"),)


class WebhookReceiptRow(Base):
    """One row per provider event: the unique constraint that makes a redelivery a no-op."""

    __tablename__ = "webhook_receipts"

    provider: Mapped[str] = mapped_column(String(32), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    booking_id: Mapped[str | None] = mapped_column(String(40))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (Index("ix_webhook_receipts_booking", "booking_id"),)


class EvidenceRow(Base):
    __tablename__ = "evidence"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    review_case_id: Mapped[str] = mapped_column(ForeignKey("review_cases.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    initiated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    subject_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    reservation: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    superseded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class BookingEventRow(Base):
    """The audit trail: every state change, in the same transaction as the change."""

    __tablename__ = "booking_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    booking_id: Mapped[str] = mapped_column(ForeignKey("bookings.id"), nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    from_state: Mapped[str | None] = mapped_column(String(24))
    to_state: Mapped[str] = mapped_column(String(24), nullable=False)
    trigger: Mapped[str] = mapped_column(String(48), nullable=False)
    source: Mapped[str] = mapped_column(String(24), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    correlation_id: Mapped[str | None] = mapped_column(String(128))
    trace_id: Mapped[str | None] = mapped_column(String(32))
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (UniqueConstraint("booking_id", "seq", name="uq_events_booking_seq"),)


class IdempotencyRow(Base):
    __tablename__ = "idempotency_keys"

    client_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    command_id: Mapped[str] = mapped_column(ForeignKey("commands.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
