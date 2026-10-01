"""webhook receipts: one row per provider event, the unique constraint that makes a redelivery
a no-op inside the applying transaction (ADR 011)

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-01
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "webhook_receipts",
        sa.Column("provider", sa.String(32), primary_key=True),
        sa.Column("event_id", sa.String(128), primary_key=True),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("booking_id", sa.String(40), nullable=True),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )
    op.create_index("ix_webhook_receipts_booking", "webhook_receipts", ["booking_id"])


def downgrade() -> None:
    op.drop_index("ix_webhook_receipts_booking", table_name="webhook_receipts")
    op.drop_table("webhook_receipts")
