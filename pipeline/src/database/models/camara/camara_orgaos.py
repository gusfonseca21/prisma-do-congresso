import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import BaseMixin


class CamaraTiposOrgaosArg(BaseModel):
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


class CamaraTiposOrgaos(Base, BaseMixin):
    __tablename__ = "camara_tipos_orgaos"

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
        sa.Integer, sa.ForeignKey("camara_tipos_orgaos.id_tipo_orgao"), nullable=False
    )
    nome_publicacao = sa.Column(sa.Text, nullable=True)
    nome_resumido = sa.Column(sa.Text, nullable=True)
