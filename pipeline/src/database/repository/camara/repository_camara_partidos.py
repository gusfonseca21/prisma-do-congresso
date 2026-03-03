from typing import Sequence, Tuple

import sqlalchemy as sa
from sqlalchemy import Row, select
from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_partidos import (
    CamaraPartidos,
    CamaraPartidosArg,
)
from database.repository.logs import insert_log_linhas_db
from utils.db import columns_to_compare, update_dict, where_clause

partidos = CamaraPartidos.__table__


def insert_camara_partidos_db(data: list[CamaraPartidosArg]):
    """
    Carrega os dados de Partidos no Banco de Dados
    """
    with get_connection() as conn:
        stmt = insert(partidos).values(
            [
                {
                    "id_lote": p.id_lote,
                    "id_partido": p.id_partido,
                    "sigla": p.sigla,
                    "nome": p.nome,
                    "status_data": p.status_data,
                    "id_legislatura": p.id_legislatura,
                    "situacao": p.situacao,
                    "total_posse": p.total_posse,
                    "total_membros": p.total_membros,
                    "id_lider": p.id_lider,
                }
                for p in data
            ]
        )

        columns = columns_to_compare(partidos, "id_deputado")

        stmt = stmt.on_conflict_do_update(
            index_elements=["id_partido"],
            set_=update_dict(stmt=stmt, columns_to_compare=columns),
            where=where_clause(table=partidos, stmt=stmt, columns_to_compare=columns),
        )

        stmt = stmt.returning(partidos.c.id, sa.text("xmax"))

        result = conn.execute(stmt)

        rows = result.fetchall()

        for row in rows[:3]:
            print(row.xmax, type(row.xmax))

        total = len(data)
        inserted = sum(1 for row in rows if int(row.xmax) == 0)
        updated = sum(1 for row in rows if int(row.xmax) != 0)
        ignored = total - len(rows)

        print(
            f"rows: {len(rows)}, inserted: {inserted}, updated: {updated}, ignored: {ignored}"
        )

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=partidos.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )


def get_partidos_siglas_db() -> Sequence[Row[Tuple[partidos, partidos]]]:
    """
    Retorna as siglas e os ids dos partidos.
    Bom para inserir o id do partido na tabela camara_deputados
    """
    with get_connection() as conn:
        stmt = select(partidos.c.id_partido, partidos.c.sigla)
        result = conn.execute(stmt)
        rows = result.fetchall()

    return rows
