import datetime

import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import BaseMixin


class CamaraLegislaturaArg(BaseModel):
    """
    id_legislatura: int
    data_inicio: datetime.date
    data_fim: datetime.date
    """

    id_legislatura: int
    data_inicio: datetime.date
    data_fim: datetime.date


class CamaraLegislaturasLideresArg(BaseModel):
    """
    id_lote: int
    id_deputado: int
    id_legislatura: int
    titulo: str
    bancada_tipo: str
    bancada_nome: str
    id_bancada: int | None
    data_inicio: datetime.date
    data_fim: datetime.date | None
    """

    id_lote: int
    id_deputado: int
    id_legislatura: int
    titulo: str
    bancada_tipo: str
    bancada_nome: str
    id_bancada: int | None
    data_inicio: datetime.date
    data_fim: datetime.date | None


class CamaraLegislaturasMesaArg(BaseModel):
    """
    id_lote: int
    id_deputado: int
    titulo: str
    data_inicio: datetime.date
    data_fim: datetime.date | None
    id_legislatura: int
    """

    id_lote: int
    id_deputado: int
    titulo: str
    data_inicio: datetime.date
    data_fim: datetime.date | None
    id_legislatura: int


class CamaraLegislatura(Base, BaseMixin):
    __tablename__ = "camara_legislatura"

    id_legislatura = sa.Column(sa.Integer, nullable=False, unique=True)
    data_inicio = sa.Column(sa.Date, nullable=False)
    data_fim = sa.Column(sa.Date, nullable=False)


class CamaraLegislaturasLideres(Base, BaseMixin):
    __tablename__ = "camara_legislaturas_lideres"

    id_deputado = sa.Column(
        sa.Integer, sa.ForeignKey("camara_deputados.id_deputado"), nullable=False
    )
    id_legislatura = sa.Column(
        sa.Integer, sa.ForeignKey("camara_legislatura.id_legislatura"), nullable=False
    )
    titulo = sa.Column(sa.Text, nullable=False)
    bancada_tipo = sa.Column(sa.Text, nullable=False)
    bancada_nome = sa.Column(sa.Text, nullable=False)
    id_bancada = sa.Column(sa.Integer, nullable=True)
    data_inicio = sa.Column(sa.Date, nullable=False)
    data_fim = sa.Column(sa.Date, nullable=True)
    __table_args__ = (
        sa.UniqueConstraint(
            "id_deputado",
            "titulo",
            "data_inicio",
            "bancada_nome",
            name="uq_legislaturas_lideres",
        ),
    )


class CamaraLegislaturasMesa(Base, BaseMixin):
    __tablename__ = "camara_legislaturas_mesa"

    id_deputado = sa.Column(
        sa.Integer, sa.ForeignKey("camara_deputados.id_deputado"), nullable=False
    )
    titulo = sa.Column(sa.Text, nullable=False)
    data_inicio = sa.Column(sa.Date, nullable=False)
    data_fim = sa.Column(sa.Date, nullable=True)
    id_legislatura = sa.Column(
        sa.Integer, sa.ForeignKey("camara_legislatura.id_legislatura"), nullable=False
    )
    __table_args__ = (
        sa.UniqueConstraint(
            "id_deputado", "titulo", "data_inicio", name="uq_legislaturas_mesa"
        ),
    )
