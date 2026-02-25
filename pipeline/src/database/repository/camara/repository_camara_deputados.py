from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_deputados import (
    CamaraDeputados,
    CamaraDeputadosArg,
    CamaraDeputadosHistorico,
    CamaraDeputadosHistoricoArg,
    CamaraDeputadosRedesSociais,
    CamaraDeputadosRedesSociaisArg,
)
from utils.db import columns_to_compare, update_dict, where_clause

deputados = CamaraDeputados.__table__
redes_sociais = CamaraDeputadosRedesSociais.__table__
historico = CamaraDeputadosHistorico.__table__


def insert_camara_deputados(
    lote_id: int,
    deputados_data: list[CamaraDeputadosArg],
    redes_sociais_data: list[CamaraDeputadosRedesSociaisArg],
    historico_deputados_data: list[CamaraDeputadosHistoricoArg],
):
    """
    Carrega os dados de Deputados e suas Redes Sociais no Banco de Dados
    """
    with get_connection() as conn:
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

        stmt_historico = (
            insert(historico)
            .values(
                [
                    {
                        "id_lote": historico.id_lote,
                        "id_deputado": historico.id_deputado,
                        "nome": historico.nome,
                        "id_partido": historico.id_partido,
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
