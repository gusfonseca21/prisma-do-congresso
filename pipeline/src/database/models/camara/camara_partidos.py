import datetime

import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import BaseMixin


class CamaraPartidosArg(BaseModel):
    """
    id_lote: int
    id_partido: int
    sigla: str
    nome: str
    status_data: datetime | None
    id_legislatura: int
    situacao: str | None
    total_posse: int
    total_membros: int
    id_lider: int | None
    """

    id_lote: int
    id_partido: int
    sigla: str
    nome: str
    status_data: datetime.datetime | None
    id_legislatura: int
    situacao: str | None
    total_posse: int
    total_membros: int
    id_lider: int | None


class CamaraPartidos(Base, BaseMixin):
    __tablename__ = "camara_partidos"

    id_partido = sa.Column(sa.Integer, unique=True, nullable=False)
    sigla = sa.Column(sa.String(15), unique=True, nullable=False)
    nome = sa.Column(sa.Text, nullable=False, unique=True)
    status_data = sa.Column(sa.DateTime(timezone=True), nullable=True)
    id_legislatura = sa.Column(sa.Integer, nullable=False)
    situacao = sa.Column(sa.Text, nullable=True)
    total_posse = sa.Column(sa.Integer, nullable=False)
    total_membros = sa.Column(sa.Integer, nullable=False)
    id_lider = sa.Column(
        sa.Integer,
        sa.ForeignKey(
            "camara_deputados.id_deputado",
            use_alter=True,
        ),
        nullable=True,
    )
