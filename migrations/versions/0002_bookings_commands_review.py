"""bookings, commands, attempts, review cases, evidence, events, idempotency keys

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "bookings",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column(
            "client_id", sa.String(64), sa.ForeignKey("api_clients.client_id"), nullable=False
        ),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("state", sa.String(24), nullable=False),
        sa.Column("offer_snapshot", sa.JSON, nullable=False),
        sa.Column("passengers", sa.JSON, nullable=False),
        sa.Column("contact_email", sa.String(200), nullable=False),
        sa.Column("provider_booking_ref", sa.String(128)),
        sa.Column("unresolved_reason", sa.String(64)),
        sa.Column("failure_code", sa.String(64)),
        sa.Column("version", sa.Integer, nullable=False, server_default="0"),
        sa.Column("next_action_at", sa.DateTime(timezone=True)),
        sa.Column("lease_token", sa.String(40)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )
    op.create_index("ix_bookings_client", "bookings", ["client_id", "created_at"])
    op.create_index("ix_bookings_work", "bookings", ["next_action_at", "lease_expires_at"])
    op.create_index("ix_bookings_state", "bookings", ["state"])

    op.create_table(
        "commands",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("booking_id", sa.String(40), sa.ForeignKey("bookings.id"), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("intent", sa.JSON, nullable=False),
        sa.Column("provider_key", sa.String(128), nullable=False),
        sa.Column("first_dispatch_at", sa.DateTime(timezone=True)),
        sa.Column("execution_cutoff", sa.DateTime(timezone=True)),
        sa.Column("disposition", sa.String(16), nullable=False),
        sa.Column("basis", sa.String(16)),
        sa.Column("submission_ref", sa.String(128)),
        sa.Column("lookups_performed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_commands_booking", "commands", ["booking_id", "kind"])

    op.create_table(
        "attempts",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("command_id", sa.String(40), sa.ForeignKey("commands.id"), nullable=False),
        sa.Column("n", sa.Integer, nullable=False),
        sa.Column("request", sa.JSON, nullable=False),
        sa.Column("dispatch_marked_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("outcome", sa.String(16)),
        sa.Column("side_effect", sa.String(16)),
        sa.Column("error", sa.String(500)),
        sa.UniqueConstraint("command_id", "n", name="uq_attempts_command_n"),
    )

    op.create_table(
        "review_cases",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("booking_id", sa.String(40), sa.ForeignKey("bookings.id"), nullable=False),
        sa.Column("reason", sa.String(64), nullable=False),
        sa.Column("remediable", sa.Boolean, nullable=False),
        sa.Column("outstanding_command_id", sa.String(40), sa.ForeignKey("commands.id")),
        sa.Column("implicated", sa.JSON, nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.Column("resolution", sa.String(24)),
        sa.Column("resolved_by", sa.String(64)),
    )
    op.create_index("ix_review_open", "review_cases", ["closed_at", "opened_at"])

    op.create_table(
        "evidence",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column(
            "review_case_id", sa.String(40), sa.ForeignKey("review_cases.id"), nullable=False
        ),
        sa.Column("kind", sa.String(24), nullable=False),
        sa.Column("initiated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("subject_ref", sa.String(128), nullable=False),
        sa.Column("reservation", sa.JSON),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
        sa.Column("superseded", sa.Boolean, nullable=False, server_default=sa.false()),
    )

    op.create_table(
        "booking_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("booking_id", sa.String(40), sa.ForeignKey("bookings.id"), nullable=False),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("from_state", sa.String(24)),
        sa.Column("to_state", sa.String(24), nullable=False),
        sa.Column("trigger", sa.String(48), nullable=False),
        sa.Column("source", sa.String(24), nullable=False),
        sa.Column("actor", sa.String(64)),
        sa.Column("payload", sa.JSON, nullable=False),
        sa.Column("correlation_id", sa.String(128)),
        sa.Column("trace_id", sa.String(32)),
        sa.Column(
            "occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("booking_id", "seq", name="uq_events_booking_seq"),
    )

    op.create_table(
        "idempotency_keys",
        sa.Column("client_id", sa.String(64), primary_key=True),
        sa.Column("key", sa.String(128), primary_key=True),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("command_id", sa.String(40), sa.ForeignKey("commands.id"), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )


def downgrade() -> None:
    for table in (
        "idempotency_keys",
        "booking_events",
        "evidence",
        "review_cases",
        "attempts",
        "commands",
    ):
        op.drop_table(table)
    op.drop_index("ix_bookings_state", table_name="bookings")
    op.drop_index("ix_bookings_work", table_name="bookings")
    op.drop_index("ix_bookings_client", table_name="bookings")
    op.drop_table("bookings")
