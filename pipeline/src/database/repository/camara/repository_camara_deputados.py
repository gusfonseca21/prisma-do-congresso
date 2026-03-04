import sqlalchemy as sa
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
from database.repository.logs import insert_log_linhas_db
from utils.db import columns_to_compare, update_dict, where_clause

deputados = CamaraDeputados.__table__
redes_sociais = CamaraDeputadosRedesSociais.__table__
historico = CamaraDeputadosHistorico.__table__
mandatos_externos = CamaraDeputadosMandatosExternos.__table__
ocupacoes = CamaraDeputadosOcupacoes.__table__
profissoes = CamaraDeputadosProfissoes.__table__


def insert_camara_deputados_db(
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

        stmt_deputado = stmt_deputado.returning(deputados.c.id, sa.text("xmax"))

        result_deputado = conn.execute(stmt_deputado)

        rows_deputado = result_deputado.fetchall()
        total_deputado = len(deputados_data)
        inserted_deputado = sum(1 for row in rows_deputado if int(row.xmax) == 0)
        updated_deputado = sum(1 for row in rows_deputado if int(row.xmax) != 0)
        ignored_deputado = total_deputado - len(rows_deputado)

        insert_log_linhas_db(
            id_lote=deputados_data[0].id_lote,
            table=deputados.name,
            inserted=inserted_deputado,
            updated=updated_deputado,
            ignored=ignored_deputado,
            total=total_deputado,
        )

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

        result_redes_sociais = conn.execute(stmt_redes_sociais)

        total_redes_sociais = len(redes_sociais_data)
        inserted_redes_sociais = result_redes_sociais.rowcount
        updated_redes_sociais = 0  # Não atualiza
        ignored_redes_sociais = total_redes_sociais - inserted_redes_sociais

        insert_log_linhas_db(
            id_lote=redes_sociais_data[0].id_lote,
            table=redes_sociais.name,
            inserted=inserted_redes_sociais,
            updated=updated_redes_sociais,
            ignored=ignored_redes_sociais,
            total=total_redes_sociais,
        )


def insert_camara_historico_deputados_db(
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

        stmt_historico = stmt_historico

        result = conn.execute(stmt_historico)

        total = len(historico_deputados_data)
        inserted = result.rowcount
        updated = 0  # Não atualiza
        ignored = total - inserted

        insert_log_linhas_db(
            id_lote=historico_deputados_data[0].id_lote,
            table=historico.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )


def insert_camara_mandatos_externos_deputados_db(
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
            set_={
                "ano_fim": stmt_mandatos_externos.excluded.ano_fim,
                "id_lote": stmt_mandatos_externos.excluded.id_lote,
            },
            where=and_(
                stmt_mandatos_externos.excluded.ano_fim.isnot(None),
                mandatos_externos.c.ano_fim.is_(None),
            ),
        )

        stmt_mandatos_externos = stmt_mandatos_externos.returning(
            mandatos_externos.c.id, sa.text("xmax")
        )

        result = conn.execute(stmt_mandatos_externos)

        rows = result.fetchall()

        total = len(mandatos_externos_data)
        inserted = sum(1 for row in rows if int(row.xmax) == 0)
        updated = sum(1 for row in rows if int(row.xmax) != 0)
        ignored = total - len(rows)

        insert_log_linhas_db(
            id_lote=mandatos_externos_data[0].id_lote,
            table=mandatos_externos.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )


def insert_camara_ocupacoes_deputados_db(
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
            set_={
                "ano_fim": stmt_ocupacoes.excluded.ano_fim,
                "id_lote": stmt_ocupacoes.excluded.id_lote,
            },
            where=and_(
                stmt_ocupacoes.excluded.ano_fim.isnot(None),
                ocupacoes.c.ano_fim.is_(None),
            ),
        )

        stmt_ocupacoes = stmt_ocupacoes.returning(ocupacoes.c.id, sa.text("xmax"))

        result = conn.execute(stmt_ocupacoes)

        rows = result.fetchall()

        total = len(ocupacoes_data)
        inserted = sum(1 for row in rows if int(row.xmax) == 0)
        updated = sum(1 for row in rows if int(row.xmax) != 0)
        ignored = total - len(rows)

        insert_log_linhas_db(
            id_lote=ocupacoes_data[0].id_lote,
            table=ocupacoes.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )


def insert_camara_profissoes_deputados_db(
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

        result = conn.execute(stmt_profissoes)

        total = len(profissoes_data)
        inserted = result.rowcount
        updated = 0  # Não atualiza
        ignored = total - inserted

        insert_log_linhas_db(
            id_lote=profissoes_data[0].id_lote,
            table=profissoes.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )
