import datetime

import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import BaseMixin


class CamaraOrgaosTiposArg(BaseModel):
    """
    id_lote: int
    id_tipo_orgao: int
    nome: str
    """

    id_lote: int
    id_tipo_orgao: int
    nome: str


class CamaraOrgaosArg(BaseModel):
    """
    id_lote: int
    id_orgao: int
    sigla: str
    nome: str
    apelido: str
    id_tipo_orgao: int
    nome_publicacao: str | None
    nome_resumido: str | None
    """

    id_lote: int
    id_orgao: int
    sigla: str
    nome: str
    apelido: str
    id_tipo_orgao: int
    nome_publicacao: str | None
    nome_resumido: str | None


class CamaraOrgaosDetalhesArg(BaseModel):
    """
    id_lote: int
    id_orgao: int
    data_inicio: datetime.date
    data_instalacao: datetime.date | None
    data_fim: datetime.date | None
    data_fim_original: datetime.date | None
    url_website: str | None
    """

    id_lote: int
    id_orgao: int
    data_inicio: datetime.datetime
    data_instalacao: datetime.datetime | None
    data_fim: datetime.datetime | None
    data_fim_original: datetime.datetime | None
    url_website: str | None


class CamaraOrgaosTipos(Base, BaseMixin):
    __tablename__ = "camara_orgaos_tipos"

    id_tipo_orgao = sa.Column(sa.Integer, nullable=False, unique=True)
    nome = sa.Column(sa.Text, nullable=False)
    __table_args__ = (
        sa.UniqueConstraint("id_tipo_orgao", "nome", name="uq_tipos_orgaos"),
    )


class CamaraOrgaos(Base, BaseMixin):
    __tablename__ = "camara_orgaos"

    id_orgao = sa.Column(sa.Integer, nullable=False, unique=True)
    sigla = sa.Column(sa.Text, nullable=False, unique=True)
    nome = sa.Column(sa.Text, nullable=False, unique=False)
    apelido = sa.Column(sa.Text, nullable=False)
    id_tipo_orgao = sa.Column(
        sa.Integer, sa.ForeignKey("camara_orgaos_tipos.id_tipo_orgao"), nullable=False
    )
    nome_publicacao = sa.Column(sa.Text, nullable=True)
    nome_resumido = sa.Column(sa.Text, nullable=True)


class CamaraOrgaosDetalhes(Base, BaseMixin):
    __tablename__ = "camara_orgaos_detalhes"

    id_orgao = sa.Column(
        sa.Integer, sa.ForeignKey("camara_orgaos.id_orgao"), nullable=False, unique=True
    )
    data_inicio = sa.Column(sa.DateTime(timezone=True), nullable=False)
    data_instalacao = sa.Column(sa.DateTime(timezone=True), nullable=True)
    data_fim = sa.Column(sa.DateTime(timezone=True), nullable=True)
    data_fim_original = sa.Column(sa.DateTime(timezone=True), nullable=True)
    url_website = sa.Column(sa.Text, nullable=True)
