"""holds and cancellations: confirmation deadlines, provider generation and revision, the
confirmation budget, refund information, cancel phases and quotes

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-01
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "bookings", sa.Column("confirmation_deadline", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("bookings", sa.Column("provider_generation", sa.Integer(), nullable=True))
    op.add_column("bookings", sa.Column("last_revision", sa.Integer(), nullable=True))
    op.add_column("bookings", sa.Column("confirm_budget_remaining", sa.Integer(), nullable=True))
    op.add_column("bookings", sa.Column("refund", sa.JSON(), nullable=True))
    op.add_column("commands", sa.Column("phase", sa.String(16), nullable=True))
    op.add_column("commands", sa.Column("quote", sa.JSON(), nullable=True))
    op.add_column("commands", sa.Column("target_ref", sa.String(128), nullable=True))
    op.create_index("ix_bookings_deadline", "bookings", ["state", "confirmation_deadline"])


def downgrade() -> None:
    op.drop_index("ix_bookings_deadline", table_name="bookings")
    op.drop_column("commands", "target_ref")
    op.drop_column("commands", "quote")
    op.drop_column("commands", "phase")
    op.drop_column("bookings", "refund")
    op.drop_column("bookings", "confirm_budget_remaining")
    op.drop_column("bookings", "last_revision")
    op.drop_column("bookings", "provider_generation")
    op.drop_column("bookings", "confirmation_deadline")
