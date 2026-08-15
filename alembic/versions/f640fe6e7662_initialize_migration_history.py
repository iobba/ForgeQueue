"""initialize migration history

Revision ID: f640fe6e7662
Revises:
Create Date: 2026-08-15 10:12:46.335405

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "f640fe6e7662"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
