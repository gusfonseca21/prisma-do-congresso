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
    sigla_partido: str
    sigla_uf: str
    id_legislatura: int
    data_hora: datetime.datetime
    situacao: str | None
    condicao_eleitoral: str | None
    descricao_status: str | None
    nome_eleitoral: str | None
    hash: str
    """

    id_lote: int
    id_deputado: int
    nome: str
    sigla_partido: str
    sigla_uf: str
    id_legislatura: int
    data_hora: datetime.datetime
    situacao: str | None
    condicao_eleitoral: str | None
    descricao_status: str | None
    nome_eleitoral: str | None
    hash: str


class CamaraDeputadosMandatosExternosArg(BaseModel):
    """
    id_lote: int
    id_deputado: int
    cargo: str
    sigla_uf: str | None
    municipio: str | None
    ano_inicio: int
    ano_fim: int | None
    sigla_partido: str | None
    """

    id_lote: int
    id_deputado: int
    cargo: str
    sigla_uf: str | None
    municipio: str | None
    ano_inicio: int
    ano_fim: int | None
    sigla_partido: str | None


class CamaraDeputadosOcupacoesArg(BaseModel):
    """
    id_lote: int
    id_deputado: int
    titulo: str
    entidade: str | None
    entidade_uf: str | None
    entidade_pais: str | None
    ano_inicio: int
    ano_fim: int | None
    """

    id_lote: int
    id_deputado: int
    titulo: str
    entidade: str | None
    entidade_uf: str | None
    entidade_pais: str | None
    ano_inicio: int
    ano_fim: int | None


class CamaraDeputadosProfissoesArg(BaseModel):
    """
    id_lote: int
    id_deputado: int
    data_hora: datetime.datetime | None
    titulo: str
    """

    id_lote: int
    id_deputado: int
    data_hora: datetime.datetime | None
    titulo: str


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
    sigla_partido = sa.Column(sa.String(15), nullable=False)
    sigla_uf = sa.Column(sa.CHAR(2), nullable=False)
    id_legislatura = sa.Column(sa.Integer, nullable=False)
    data_hora = sa.Column(sa.DateTime(timezone=True), nullable=False)
    situacao = sa.Column(sa.Text, nullable=True)
    condicao_eleitoral = sa.Column(sa.Text, nullable=True)
    descricao_status = sa.Column(sa.Text, nullable=True)
    nome_eleitoral = sa.Column(sa.Text, nullable=True)
    hash = sa.Column(sa.CHAR(32), nullable=False, unique=True)


class CamaraDeputadosMandatosExternos(Base, BaseMixin):
    __tablename__ = "camara_deputados_mandatos_externos"

    id_deputado = sa.Column(
        sa.Integer,
        sa.ForeignKey("camara_deputados.id_deputado"),
        nullable=False,
    )
    cargo = sa.Column(sa.Text, nullable=False)
    sigla_uf = sa.Column(sa.CHAR(2), nullable=True)
    municipio = sa.Column(sa.Text, nullable=True)
    ano_inicio = sa.Column(sa.Integer, nullable=False)
    ano_fim = sa.Column(sa.Integer, nullable=True)
    sigla_partido = sa.Column(sa.String(15), nullable=True)

    __table_args__ = (
        sa.UniqueConstraint(
            "id_deputado", "cargo", "ano_inicio", name="uq_mandato_externo"
        ),
    )


class CamaraDeputadosOcupacoes(Base, BaseMixin):
    __tablename__ = "camara_deputados_ocupacoes"

    id_deputado = sa.Column(
        sa.Integer, sa.ForeignKey("camara_deputados.id_deputado"), nullable=False
    )
    titulo = sa.Column(sa.Text, nullable=False)
    entidade = sa.Column(sa.Text, nullable=True)
    entidade_uf = sa.Column(sa.CHAR(2), nullable=True)
    entidade_pais = sa.Column(sa.Text, nullable=True)
    ano_inicio = sa.Column(sa.Integer, nullable=False)
    ano_fim = sa.Column(sa.Integer, nullable=True)

    __table_args__ = (
        sa.UniqueConstraint(
            "id_deputado", "titulo", "ano_inicio", name="uq_deputados_ocupacoes"
        ),
    )


class CamaraDeputadosProfissoes(Base, BaseMixin):
    __tablename__ = "camara_deputados_profissoes"

    id_deputado = sa.Column(
        sa.Integer, sa.ForeignKey("camara_deputados.id_deputado"), nullable=False
    )
    data_hora = sa.Column(sa.DateTime(timezone=True), nullable=True)
    titulo = sa.Column(sa.Text, nullable=False)

    __table_args__ = (sa.UniqueConstraint("id_deputado", "titulo"),)
