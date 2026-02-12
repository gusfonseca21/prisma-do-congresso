import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Lote(Base):
    __tablename__ = "lote"

    id = sa.Column(sa.Integer, sa.Identity(start=1, cycle=False), primary_key=True)
    data_inicio = sa.Column(
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    )
    data_fim = sa.Column(sa.TIMESTAMP(timezone=True), nullable=True)
