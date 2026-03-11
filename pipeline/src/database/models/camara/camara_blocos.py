import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import BaseMixin


class CamaraBlocosArg(BaseModel):
    """
    id_lote: int
    id_bloco: int
    nome: str
    id_legislatura: int
    federacao: bool
    """

    id_lote: int
    id_bloco: int
    nome: str
    id_legislatura: int
    federacao: bool


class CamaraBlocosPartidosArg(BaseModel):
    """
    id_lote: int
    id_bloco: int
    sigla: str
    nome: str
    """

    id_lote: int
    id_bloco: int
    sigla: str
    nome: str


class CamaraBlocos(Base, BaseMixin):
    __tablename__ = "camara_blocos"

    id_bloco = sa.Column(sa.Integer, unique=True, nullable=False)
    nome = sa.Column(sa.Text, nullable=False)
    id_legislatura = sa.Column(
        sa.Integer, sa.ForeignKey("camara_legislaturas.id_legislatura"), nullable=False
    )
    federacao = sa.Column(sa.Boolean, nullable=False)


class CamaraBlocosPartidos(Base, BaseMixin):
    __tablename__ = "camara_blocos_partidos"

    id_bloco = sa.Column(sa.Integer, nullable=False)
    sigla = sa.Column(sa.String(15), unique=True, nullable=False)
    nome = sa.Column(sa.Text, nullable=False)
