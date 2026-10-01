"""attempts: the instant a possible effect was settled as excluded

Revision ID: 0006
Revises: 0005
Create Date: 2026-10-01
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("attempts", sa.Column("excluded_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("attempts", "excluded_at")
