from datetime import datetime

import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import BaseMixin


class CamaraEventosArg(BaseModel):
    """
    id_lote: int
    id_evento: int
    data_hora_inicio: datetime
    data_hora_fim: datetime | None
    situacao: str
    descricao_tipo: str
    descricao: str
    local_externo: str | None
    local_nome: str | None
    url_registro: str | None
    """

    id_lote: int
    id_evento: int
    data_hora_inicio: datetime
    data_hora_fim: datetime | None
    situacao: str
    descricao_tipo: str
    descricao: str
    local_externo: str | None
    local_nome: str | None
    url_registro: str | None


class CamaraEventosOrgaosArg(BaseModel):
    """
    id_lote: int
    id_evento: int
    id_orgao: int
    """

    id_lote: int
    id_evento: int
    id_orgao: int


class CamaraEventos(Base, BaseMixin):
    __tablename__ = "camara_eventos"

    id_evento = sa.Column(sa.Integer, nullable=False, unique=True)
    data_hora_inicio = sa.Column(sa.DateTime(timezone=True), nullable=False)
    data_hora_fim = sa.Column(sa.DateTime(timezone=True), nullable=True)
    situacao = sa.Column(sa.Text, nullable=False)
    descricao_tipo = sa.Column(sa.Text, nullable=False)
    descricao = sa.Column(sa.Text, nullable=False)
    local_externo = sa.Column(sa.Text, nullable=True)
    local_nome = sa.Column(sa.Text, nullable=True)
    url_registro = sa.Column(sa.Text, nullable=True)


class CamaraEventosOrgaos(Base, BaseMixin):
    __tablename__ = "camara_eventos_orgaos"

    id_evento = sa.Column(
        sa.Integer, sa.ForeignKey("camara_eventos.id_evento"), nullable=False
    )
    id_orgao = sa.Column(
        sa.Integer, sa.ForeignKey("camara_orgaos.id_orgao"), nullable=False
    )
    __table_args__ = (
        sa.UniqueConstraint("id_evento", "id_orgao", name="uq_eventos_orgaos"),
    )
