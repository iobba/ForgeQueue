"""create job attempts table

Revision ID: 7e7b1f1bcd61
Revises: 4c0c461c713c
Create Date: 2026-09-06 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7e7b1f1bcd61"
down_revision: str | Sequence[str] | None = "4c0c461c713c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "job_attempts",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=200), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                name="job_attempt_status",
                native_enum=False,
                create_constraint=False,
            ),
            server_default="RUNNING",
            nullable=False,
        ),
        sa.Column(
            "failure_kind",
            sa.Enum(
                "RETRYABLE",
                "PERMANENT",
                name="job_failure_kind",
                native_enum=False,
                create_constraint=False,
            ),
            nullable=True,
        ),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "attempt_number >= 1",
            name="ck_job_attempts_attempt_number_positive",
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at",
            name="ck_job_attempts_completion_not_before_start",
        ),
        sa.CheckConstraint(
            "(status = 'RUNNING' AND completed_at IS NULL) OR "
            "(status IN ('SUCCEEDED', 'FAILED') AND completed_at IS NOT NULL)",
            name="ck_job_attempts_completion_matches_status",
        ),
        sa.CheckConstraint(
            "(status = 'FAILED' AND failure_kind IS NOT NULL "
            "AND error_code IS NOT NULL AND error_message IS NOT NULL) OR "
            "(status <> 'FAILED' AND failure_kind IS NULL "
            "AND error_code IS NULL AND error_message IS NULL)",
            name="ck_job_attempts_failure_details_match_status",
        ),
        sa.CheckConstraint(
            "failure_kind IS NULL OR failure_kind IN ('RETRYABLE', 'PERMANENT')",
            name="ck_job_attempts_failure_kind_valid",
        ),
        sa.CheckConstraint(
            "status IN ('RUNNING', 'SUCCEEDED', 'FAILED')",
            name="ck_job_attempts_status_valid",
        ),
        sa.CheckConstraint(
            "length(btrim(worker_id)) > 0",
            name="ck_job_attempts_worker_id_not_blank",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "job_id",
            "attempt_number",
            name="uq_job_attempts_job_id_attempt_number",
        ),
    )
    op.create_index(
        "ix_job_attempts_status_started_at",
        "job_attempts",
        ["status", "started_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_job_attempts_status_started_at",
        table_name="job_attempts",
    )
    op.drop_table("job_attempts")
