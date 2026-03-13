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
    data_inicio: datetime.datetime | None
    data_instalacao: datetime.datetime | None
    data_fim: datetime.datetime | None
    data_fim_original: datetime.datetime | None
    url_website: str | None


class CamaraOrgaosMembrosArg(BaseModel):
    """
    id_lote: int
    id_orgao: int
    id_deputado: int
    id_legislatura: int
    titulo: str
    data_inicio: datetime.date
    data_fim: datetime.date | None
    """

    id_lote: int
    id_orgao: int
    id_deputado: int
    id_legislatura: int
    titulo: str
    data_inicio: datetime.date
    data_fim: datetime.date | None


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
    sigla = sa.Column(sa.Text, nullable=False, unique=False)
    nome = sa.Column(sa.Text, nullable=False, unique=False)
    apelido = sa.Column(sa.Text, nullable=False)
    id_tipo_orgao = sa.Column(
        sa.Integer, sa.ForeignKey("camara_orgaos_tipos.id_tipo_orgao"), nullable=False
    )
    nome_publicacao = sa.Column(sa.Text, nullable=True)
    nome_resumido = sa.Column(sa.Text, nullable=True)
    data_inicio = sa.Column(sa.DateTime(timezone=True), nullable=True)
    data_instalacao = sa.Column(sa.DateTime(timezone=True), nullable=True)
    data_fim = sa.Column(sa.DateTime(timezone=True), nullable=True)
    data_fim_original = sa.Column(sa.DateTime(timezone=True), nullable=True)
    url_website = sa.Column(sa.Text, nullable=True)


class CamaraOrgaosMembros(Base, BaseMixin):
    __tablename__ = "camara_orgaos_membros"

    id_orgao = sa.Column(
        sa.Integer, sa.ForeignKey("camara_orgaos.id_orgao"), nullable=False
    )
    id_deputado = sa.Column(
        sa.Integer, sa.ForeignKey("camara_deputados.id_deputado"), nullable=False
    )
    id_legislatura = sa.Column(
        sa.Integer, sa.ForeignKey("camara_legislaturas.id_legislatura"), nullable=False
    )
    titulo = sa.Column(sa.Text, nullable=False)
    data_inicio = sa.Column(sa.Date, nullable=False)
    data_fim = sa.Column(sa.Date, nullable=True)
    __table_args__ = (
        sa.UniqueConstraint(
            "id_orgao", "id_deputado", "titulo", "data_inicio", name="uq_orgaos_membros"
        ),
    )
