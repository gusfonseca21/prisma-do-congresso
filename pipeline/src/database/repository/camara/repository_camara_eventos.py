from sqlalchemy import or_, text
from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_eventos import (
    CamaraEventos,
    CamaraEventosArg,
    CamaraEventosOrgaos,
    CamaraEventosOrgaosArg,
)
from database.repository.logs import insert_log_linhas_db

camara_eventos = CamaraEventos.__table__
camara_eventos_orgaos = CamaraEventosOrgaos.__table__


def insert_camara_eventos_db(data: list[CamaraEventosArg]):
    with get_connection() as conn:
        stmt = insert(camara_eventos).values(
            [
                {
                    "id_lote": item.id_lote,
                    "id_evento": item.id_evento,
                    "data_hora_inicio": item.data_hora_inicio,
                    "data_hora_fim": item.data_hora_fim,
                    "situacao": item.situacao,
                    "descricao_tipo": item.descricao_tipo,
                    "descricao": item.descricao,
                    "local_externo": item.local_externo,
                    "local_nome": item.local_nome,
                    "url_registro": item.url_registro,
                }
                for item in data
            ]
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["id_evento"],
            set_={
                "data_hora_inicio": stmt.excluded.data_hora_inicio,
                "data_hora_fim": stmt.excluded.data_hora_fim,
                "situacao": stmt.excluded.situacao,
                "descricao_tipo": stmt.excluded.descricao_tipo,
                "descricao": stmt.excluded.descricao,
                "local_externo": stmt.excluded.local_externo,
                "local_nome": stmt.excluded.local_nome,
                "url_registro": stmt.excluded.url_registro,
            },
            where=or_(
                stmt.excluded.data_hora_inicio.is_distinct_from(
                    camara_eventos.c.data_hora_inicio
                ),
                stmt.excluded.data_hora_fim.is_distinct_from(
                    camara_eventos.c.data_hora_fim
                ),
                stmt.excluded.situacao.is_distinct_from(camara_eventos.c.situacao),
                stmt.excluded.descricao_tipo.is_distinct_from(
                    camara_eventos.c.descricao_tipo
                ),
                stmt.excluded.descricao.is_distinct_from(camara_eventos.c.descricao),
                stmt.excluded.local_externo.is_distinct_from(
                    camara_eventos.c.local_externo
                ),
                stmt.excluded.local_nome.is_distinct_from(camara_eventos.c.local_nome),
                stmt.excluded.url_registro.is_distinct_from(
                    camara_eventos.c.url_registro
                ),
            ),
        )

        stmt = stmt.returning(camara_eventos.c.id, text("xmax"))
        result = conn.execute(stmt)
        rows = result.fetchall()
        total = len(data)
        inserted = sum(1 for row in rows if int(row.xmax) == 0)
        updated = sum(1 for row in rows if int(row.xmax) != 0)
        ignored = total - len(rows)

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_eventos.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )

    return


def insert_camara_eventos_orgaos_db(data: list[CamaraEventosOrgaosArg]):
    with get_connection() as conn:
        stmt = (
            insert(camara_eventos_orgaos)
            .values(
                [
                    {
                        "id_lote": item.id_lote,
                        "id_evento": item.id_evento,
                        "id_orgao": item.id_orgao,
                    }
                    for item in data
                ]
            )
            .on_conflict_do_nothing(index_elements=["id_evento", "id_orgao"])
        )

        result = conn.execute(stmt)
        total = len(data)
        inserted = result.rowcount
        updated = 0  # Não atualiza
        ignored = total - inserted

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_eventos_orgaos.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )
    return
