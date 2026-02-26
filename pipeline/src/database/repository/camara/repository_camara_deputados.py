from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_deputados import (
    CamaraDeputados,
    CamaraDeputadosArg,
    CamaraDeputadosHistorico,
    CamaraDeputadosHistoricoArg,
    CamaraDeputadosMandatosExternos,
    CamaraDeputadosMandatosExternosArg,
    CamaraDeputadosOcupacoes,
    CamaraDeputadosOcupacoesArg,
    CamaraDeputadosProfissoes,
    CamaraDeputadosProfissoesArg,
    CamaraDeputadosRedesSociais,
    CamaraDeputadosRedesSociaisArg,
)
from utils.db import columns_to_compare, update_dict, where_clause

deputados = CamaraDeputados.__table__
redes_sociais = CamaraDeputadosRedesSociais.__table__
historico = CamaraDeputadosHistorico.__table__
mandatos_externos = CamaraDeputadosMandatosExternos.__table__
ocupacoes = CamaraDeputadosOcupacoes.__table__
profissoes = CamaraDeputadosProfissoes.__table__


def insert_camara_deputados(
    lote_id: int,
    deputados_data: list[CamaraDeputadosArg],
    redes_sociais_data: list[CamaraDeputadosRedesSociaisArg],
):
    with get_connection() as conn:
        ## DEPUTADOS
        stmt_deputado = insert(deputados).values(
            [
                {
                    "id_lote": lote_id,
                    "id_deputado": deputado.id_deputado,
                    "nome_civil": deputado.nome_civil,
                    "nome": deputado.nome,
                    "id_partido": deputado.id_partido,
                    "sigla_uf": deputado.sigla_uf,
                    "id_legislatura": deputado.id_legislatura,
                    "url_foto": deputado.url_foto,
                    "email": deputado.email,
                    "data_ultimo_status": deputado.data_ultimo_status,
                    "nome_eleitoral": deputado.nome_eleitoral,
                    "gabinete_nome": deputado.gabinete_nome,
                    "gabinete_predio": deputado.gabinete_predio,
                    "gabinete_sala": deputado.gabinete_sala,
                    "gabinete_andar": deputado.gabinete_andar,
                    "gabinete_telefone": deputado.gabinete_telefone,
                    "situacao": deputado.situacao,
                    "condicao_eleitoral": deputado.condicao_eleitoral,
                    "descricao_status": deputado.descricao_status,
                    "cpf": deputado.cpf,
                    "sexo": deputado.sexo,
                    "data_nascimento": deputado.data_nascimento,
                    "data_falecimento": deputado.data_falecimento,
                    "uf_nascimento": deputado.uf_nascimento,
                    "municipio_nascimento": deputado.municipio_nascimento,
                    "escolaridade": deputado.escolaridade,
                }
                for deputado in deputados_data
            ]
        )

        columns = columns_to_compare(deputados, "id_deputado")

        stmt_deputado = stmt_deputado.on_conflict_do_update(
            index_elements=["id_deputado"],
            set_=update_dict(stmt=stmt_deputado, columns_to_compare=columns),
            where=where_clause(
                table=deputados, stmt=stmt_deputado, columns_to_compare=columns
            ),
        )

        conn.execute(stmt_deputado)

        ## REDES SOCIAIS
        stmt_redes_sociais = (
            insert(redes_sociais)
            .values(
                [
                    {
                        "id_lote": rede_social.id_lote,
                        "id_deputado": rede_social.id_deputado,
                        "url": rede_social.url,
                    }
                    for rede_social in redes_sociais_data
                ]
            )
            .on_conflict_do_nothing(index_elements=["url"])
        )
        conn.execute(stmt_redes_sociais)


def insert_camara_historico_deputados(
    historico_deputados_data: list[CamaraDeputadosHistoricoArg],
):
    with get_connection() as conn:
        stmt_historico = (
            insert(historico)
            .values(
                [
                    {
                        "id_lote": historico.id_lote,
                        "id_deputado": historico.id_deputado,
                        "nome": historico.nome,
                        "sigla_partido": historico.sigla_partido,
                        "sigla_uf": historico.sigla_uf,
                        "id_legislatura": historico.id_legislatura,
                        "data_hora": historico.data_hora,
                        "situacao": historico.situacao,
                        "condicao_eleitoral": historico.condicao_eleitoral,
                        "descricao_status": historico.descricao_status,
                        "nome_eleitoral": historico.nome_eleitoral,
                        "hash": historico.hash,
                    }
                    for historico in historico_deputados_data
                ]
            )
            .on_conflict_do_nothing(index_elements=["hash"])
        )
        conn.execute(stmt_historico)


def insert_camara_mandatos_externos_deputados(
    mandatos_externos_data: list[CamaraDeputadosMandatosExternosArg],
):
    with get_connection() as conn:
        stmt_mandatos_externos = insert(mandatos_externos).values(
            [
                {
                    "id_lote": mandato.id_lote,
                    "id_deputado": mandato.id_deputado,
                    "cargo": mandato.cargo,
                    "sigla_uf": mandato.sigla_uf,
                    "municipio": mandato.municipio,
                    "ano_inicio": mandato.ano_inicio,
                    "ano_fim": mandato.ano_fim,
                    "sigla_partido": mandato.sigla_partido,
                }
                for mandato in mandatos_externos_data
            ]
        )

        stmt_mandatos_externos = stmt_mandatos_externos.on_conflict_do_update(
            index_elements=["id_deputado", "cargo", "ano_inicio"],
            set_={"ano_fim": stmt_mandatos_externos.excluded.ano_fim},
            where=and_(
                stmt_mandatos_externos.excluded.ano_fim.isnot(None),
                mandatos_externos.c.ano_fim.is_(None),
            ),
        )
        conn.execute(stmt_mandatos_externos)


def insert_camara_ocupacoes_deputados(
    ocupacoes_data: list[CamaraDeputadosOcupacoesArg],
):
    with get_connection() as conn:
        stmt_ocupacoes = insert(ocupacoes).values(
            [
                {
                    "id_lote": ocupacao.id_lote,
                    "id_deputado": ocupacao.id_deputado,
                    "titulo": ocupacao.titulo,
                    "entidade": ocupacao.entidade,
                    "entidade_uf": ocupacao.entidade_uf,
                    "entidade_pais": ocupacao.entidade_pais,
                    "ano_inicio": ocupacao.ano_inicio,
                    "ano_fim": ocupacao.ano_fim,
                }
                for ocupacao in ocupacoes_data
            ]
        )

        stmt_ocupacoes = stmt_ocupacoes.on_conflict_do_update(
            index_elements=["id_deputado", "titulo", "ano_inicio"],
            set_={"ano_fim": stmt_ocupacoes.excluded.ano_fim},
            where=and_(
                stmt_ocupacoes.excluded.ano_fim.isnot(None),
                ocupacoes.c.ano_fim.is_(None),
            ),
        )
        conn.execute(stmt_ocupacoes)


def insert_camara_profissoes_deputados(
    profissoes_data: list[CamaraDeputadosProfissoesArg],
):
    with get_connection() as conn:
        stmt_profissoes = (
            insert(profissoes)
            .values(
                [
                    {
                        "id_lote": profissao.id_lote,
                        "id_deputado": profissao.id_deputado,
                        "data_hora": profissao.data_hora,
                        "titulo": profissao.titulo,
                    }
                    for profissao in profissoes_data
                ]
            )
            .on_conflict_do_nothing(index_elements=["id_deputado", "titulo"])
        )

        conn.execute(stmt_profissoes)
