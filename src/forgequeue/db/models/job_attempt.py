from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from forgequeue.db.base import Base
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind


class JobAttempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (
        CheckConstraint(
            "attempt_number >= 1",
            name="ck_job_attempts_attempt_number_positive",
        ),
        CheckConstraint(
            "length(btrim(worker_id)) > 0",
            name="ck_job_attempts_worker_id_not_blank",
        ),
        CheckConstraint(
            "status IN ('RUNNING', 'SUCCEEDED', 'FAILED')",
            name="ck_job_attempts_status_valid",
        ),
        CheckConstraint(
            "failure_kind IS NULL OR failure_kind IN ('RETRYABLE', 'PERMANENT')",
            name="ck_job_attempts_failure_kind_valid",
        ),
        CheckConstraint(
            "(status = 'RUNNING' AND completed_at IS NULL) OR "
            "(status IN ('SUCCEEDED', 'FAILED') AND completed_at IS NOT NULL)",
            name="ck_job_attempts_completion_matches_status",
        ),
        CheckConstraint(
            "(status = 'FAILED' AND failure_kind IS NOT NULL "
            "AND error_code IS NOT NULL AND error_message IS NOT NULL) OR "
            "(status <> 'FAILED' AND failure_kind IS NULL "
            "AND error_code IS NULL AND error_message IS NULL)",
            name="ck_job_attempts_failure_details_match_status",
        ),
        CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at",
            name="ck_job_attempts_completion_not_before_start",
        ),
        CheckConstraint(
            "status <> 'RUNNING' OR "
            "(heartbeat_at IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_job_attempts_running_has_lease",
        ),
        CheckConstraint(
            "(heartbeat_at IS NULL AND lease_expires_at IS NULL) OR "
            "(heartbeat_at IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND heartbeat_at >= started_at "
            "AND lease_expires_at > heartbeat_at)",
            name="ck_job_attempts_lease_order",
        ),
        UniqueConstraint(
            "job_id",
            "attempt_number",
            name="uq_job_attempts_job_id_attempt_number",
        ),
        Index("ix_job_attempts_status_started_at", "status", "started_at"),
        Index(
            "ix_job_attempts_status_lease_expires_at",
            "status",
            "lease_expires_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid7,
    )
    job_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    worker_id: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
    )
    status: Mapped[JobAttemptStatus] = mapped_column(
        Enum(
            JobAttemptStatus,
            name="job_attempt_status",
            native_enum=False,
            create_constraint=False,
            validate_strings=True,
        ),
        default=JobAttemptStatus.RUNNING,
        server_default=JobAttemptStatus.RUNNING.name,
        nullable=False,
    )
    failure_kind: Mapped[JobFailureKind | None] = mapped_column(
        Enum(
            JobFailureKind,
            name="job_failure_kind",
            native_enum=False,
            create_constraint=False,
            validate_strings=True,
        ),
        nullable=True,
    )
    error_code: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )
    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
