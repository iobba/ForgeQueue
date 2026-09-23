"""add attempt leases

Revision ID: a4d9c8e2f731
Revises: 596f69e6fd5e
Create Date: 2026-09-22 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a4d9c8e2f731"
down_revision: str | Sequence[str] | None = "596f69e6fd5e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "job_attempts",
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "job_attempts",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        sa.text(
            "UPDATE job_attempts "
            "SET heartbeat_at = started_at, "
            "lease_expires_at = started_at + INTERVAL '60 seconds' "
            "WHERE status = 'RUNNING'"
        )
    )
    op.create_check_constraint(
        "ck_job_attempts_running_has_lease",
        "job_attempts",
        "status <> 'RUNNING' OR "
        "(heartbeat_at IS NOT NULL AND lease_expires_at IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_job_attempts_lease_order",
        "job_attempts",
        "(heartbeat_at IS NULL AND lease_expires_at IS NULL) OR "
        "(heartbeat_at IS NOT NULL AND lease_expires_at IS NOT NULL "
        "AND heartbeat_at >= started_at "
        "AND lease_expires_at > heartbeat_at)",
    )
    op.create_index(
        "ix_job_attempts_status_lease_expires_at",
        "job_attempts",
        ["status", "lease_expires_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_job_attempts_status_lease_expires_at",
        table_name="job_attempts",
    )
    op.drop_constraint(
        "ck_job_attempts_lease_order",
        "job_attempts",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_attempts_running_has_lease",
        "job_attempts",
        type_="check",
    )
    op.drop_column("job_attempts", "lease_expires_at")
    op.drop_column("job_attempts", "heartbeat_at")
