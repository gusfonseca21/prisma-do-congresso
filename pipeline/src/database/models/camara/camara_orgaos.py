import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import BaseMixin


class CamaraTiposOrgaosArg(BaseModel):
    """
    id_lote: int
    codigo: int
    nome: str
    """

    id_lote: int
    codigo: int
    nome: str


class CamaraTiposOrgaos(Base, BaseMixin):
    __tablename__ = "camara_tipos_orgaos"

    codigo = sa.Column(sa.Integer, nullable=False, unique=True)
    nome = sa.Column(sa.Text, nullable=False)
    __table_args__ = (sa.UniqueConstraint("codigo", "nome", name="uq_tipos_orgaos"),)
