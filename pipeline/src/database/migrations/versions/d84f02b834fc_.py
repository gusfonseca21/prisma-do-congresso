"""empty message

Revision ID: d84f02b834fc
Revises: 8917fd66e3b9
Create Date: 2026-03-09 18:25:13.060189

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd84f02b834fc'
down_revision: Union[str, Sequence[str], None] = '8917fd66e3b9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
