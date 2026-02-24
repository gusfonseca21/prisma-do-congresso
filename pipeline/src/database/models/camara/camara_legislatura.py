import datetime

import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import LoteMixin


class CamaraLegislaturaArg(BaseModel):
    """
    id_legislatura: int
    data_inicio: date
    data_fim: date
    """

    id_legislatura: int
    data_inicio: datetime.date
    data_fim: datetime.date


class CamaraLegislatura(Base, LoteMixin):
    __tablename__ = "camara_legislatura"

    id = sa.Column(sa.Integer, sa.Identity(start=1, cycle=False), primary_key=True)
    id_legislatura = sa.Column(sa.Integer, nullable=False, unique=True)
    data_inicio = sa.Column(sa.Date, nullable=False)
    data_fim = sa.Column(sa.Date, nullable=False)
