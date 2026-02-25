import datetime

import sqlalchemy as sa
from pydantic import BaseModel

from database.models.base import Base
from database.models.mixins import BaseMixin


class CamaraDeputadosArg(BaseModel):
    """
    id_lote: int
    id_deputado: int
    nome_civil: str
    nome: str
    id_partido: int
    sigla_uf: str
    id_legislatura: int
    url_foto: str
    email: str | None
    data_ultimo_status: datetime.date
    nome_eleitoral: str | None
    gabinete_nome: str | None
    gabinete_predio: str | None
    gabinete_sala: str | None
    gabinete_andar: str | None
    gabinete_telefone: str | None
    situacao: str
    condicao_eleitoral: str
    descricao_status: str | None
    cpf: str
    sexo: str
    data_nascimento: datetime.date
    data_falecimento: datetime.date | None
    uf_nascimento: str
    municipio_nascimento: str
    escolaridade: str | None
    """

    id_lote: int
    id_deputado: int
    nome_civil: str
    nome: str
    id_partido: int
    sigla_uf: str
    id_legislatura: int
    url_foto: str
    email: str | None
    data_ultimo_status: datetime.date
    nome_eleitoral: str | None
    gabinete_nome: str | None
    gabinete_predio: str | None
    gabinete_sala: str | None
    gabinete_andar: str | None
    gabinete_telefone: str | None
    situacao: str
    condicao_eleitoral: str
    descricao_status: str | None
    cpf: str
    sexo: str
    data_nascimento: datetime.date
    data_falecimento: datetime.date | None
    uf_nascimento: str
    municipio_nascimento: str
    escolaridade: str | None


class CamaraDeputadosRedesSociaisArg(BaseModel):
    """
    id_deputado: int
    url: str
    """

    id_lote: int
    id_deputado: int
    url: str


class CamaraDeputadosHistoricoArg(BaseModel):
    """
    id_lote: int
    id_deputado: int
    nome: str
    id_partido: int
    sigla_uf: str
    id_legislatura: int
    data_hora: datetime.datetime
    situacao: str | None
    condicao_eleitoral: str | None
    descricao_status: str | None
    nome_eleitoral: str
    hash: str
    """

    id_lote: int
    id_deputado: int
    nome: str
    id_partido: int
    sigla_uf: str
    id_legislatura: int
    data_hora: datetime.datetime
    situacao: str | None
    condicao_eleitoral: str | None
    descricao_status: str | None
    nome_eleitoral: str
    hash: str


class CamaraDeputados(Base, BaseMixin):
    __tablename__ = "camara_deputados"

    id_deputado = sa.Column(sa.Integer, unique=True, nullable=False)
    nome_civil = sa.Column(sa.Text, nullable=False)
    nome = sa.Column(sa.Text, nullable=False)
    id_partido = sa.Column(sa.Integer, sa.ForeignKey("camara_partidos.id_partido"))
    sigla_uf = sa.Column(sa.CHAR(2), nullable=False)
    id_legislatura = sa.Column(sa.Integer, nullable=False)
    url_foto = sa.Column(sa.Text, nullable=False)
    email = sa.Column(sa.Text, nullable=True)
    data_ultimo_status = sa.Column(sa.Date, nullable=False)
    nome_eleitoral = sa.Column(sa.Text, nullable=False)
    gabinete_nome = sa.Column(sa.String(5), nullable=True)
    gabinete_predio = sa.Column(sa.String(2), nullable=True)
    gabinete_sala = sa.Column(sa.String(5), nullable=True)
    gabinete_andar = sa.Column(sa.String(2), nullable=True)
    gabinete_telefone = sa.Column(sa.CHAR(9), nullable=True)
    situacao = sa.Column(sa.Text, nullable=False)
    condicao_eleitoral = sa.Column(sa.Text, nullable=False)
    descricao_status = sa.Column(sa.Text, nullable=True)
    cpf = sa.Column(sa.CHAR(11), nullable=False)
    sexo = sa.Column(sa.CHAR(1), nullable=False)
    data_nascimento = sa.Column(sa.Date, nullable=False)
    data_falecimento = sa.Column(sa.Date, nullable=True)
    uf_nascimento = sa.Column(sa.CHAR(2), nullable=False)
    municipio_nascimento = sa.Column(sa.Text, nullable=False)
    escolaridade = sa.Column(sa.Text, nullable=True)


class CamaraDeputadosRedesSociais(Base, BaseMixin):
    __tablename__ = "camara_deputados_redes_sociais"

    id_deputado = sa.Column(
        sa.Integer, sa.ForeignKey("camara_deputados.id_deputado"), nullable=False
    )
    url = sa.Column(sa.Text, nullable=False, unique=True)


class CamaraDeputadosHistorico(Base, BaseMixin):
    __tablename__ = "camara_deputados_historico"

    id_deputado = sa.Column(
        sa.Integer, sa.ForeignKey("camara_deputados.id_deputado"), nullable=False
    )
    nome = sa.Column(sa.Text, nullable=False)
    id_partido = sa.Column(
        sa.Integer, sa.ForeignKey("camara_partidos.id_partido"), nullable=False
    )
    sigla_uf = sa.Column(sa.CHAR(2), nullable=False)
    id_legislatura = sa.Column(sa.Integer, nullable=False)
    data_hora = sa.Column(sa.DateTime(timezone=True), nullable=False)
    situacao = sa.Column(sa.Text, nullable=True)
    condicao_eleitoral = sa.Column(sa.Text, nullable=True)
    descricao_status = sa.Column(sa.Text, nullable=True)
    nome_eleitoral = sa.Column(sa.Text, nullable=False)
    hash = sa.Column(sa.CHAR(32), nullable=False, unique=True)
