"""add retry scheduling to jobs

Revision ID: 596f69e6fd5e
Revises: 7e7b1f1bcd61
Create Date: 2026-09-10 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "596f69e6fd5e"
down_revision: str | Sequence[str] | None = "7e7b1f1bcd61"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "jobs",
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.alter_column(
        "jobs",
        "status",
        existing_type=sa.String(length=9),
        type_=sa.String(length=15),
        existing_nullable=False,
    )
    op.drop_constraint("ck_jobs_status_valid", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_status_valid",
        "jobs",
        "status IN ('QUEUED', 'RUNNING', 'RETRY_SCHEDULED', 'COMPLETED', 'FAILED')",
    )
    op.create_check_constraint(
        "ck_jobs_next_attempt_matches_status",
        "jobs",
        "(status = 'RETRY_SCHEDULED' AND next_attempt_at IS NOT NULL) OR "
        "(status <> 'RETRY_SCHEDULED' AND next_attempt_at IS NULL)",
    )
    op.create_index(
        "ix_jobs_status_next_attempt_at",
        "jobs",
        ["status", "next_attempt_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_jobs_status_next_attempt_at", table_name="jobs")
    op.drop_constraint(
        "ck_jobs_next_attempt_matches_status",
        "jobs",
        type_="check",
    )
    op.drop_constraint("ck_jobs_status_valid", "jobs", type_="check")
    op.execute(
        sa.text(
            "UPDATE jobs "
            "SET status = 'FAILED', next_attempt_at = NULL "
            "WHERE status = 'RETRY_SCHEDULED'"
        )
    )
    op.alter_column(
        "jobs",
        "status",
        existing_type=sa.String(length=15),
        type_=sa.String(length=9),
        existing_nullable=False,
    )
    op.create_check_constraint(
        "ck_jobs_status_valid",
        "jobs",
        "status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')",
    )
    op.drop_column("jobs", "next_attempt_at")
