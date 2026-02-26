"""tabela camara_deputados_ocupacoes

Revision ID: 8e85b3b600c9
Revises: 2c0eca4f7a41
Create Date: 2026-02-26 10:50:36.913934

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8e85b3b600c9'
down_revision: Union[str, Sequence[str], None] = '2c0eca4f7a41'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
