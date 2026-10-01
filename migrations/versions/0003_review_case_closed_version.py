"""review cases: the booking version an operator resolve was issued against

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("review_cases", sa.Column("closed_version", sa.Integer(), nullable=True))
    op.create_index("ix_review_closed_version", "review_cases", ["booking_id", "closed_version"])


def downgrade() -> None:
    op.drop_index("ix_review_closed_version", table_name="review_cases")
    op.drop_column("review_cases", "closed_version")
