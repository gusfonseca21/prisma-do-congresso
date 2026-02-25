from typing import Sequence, Tuple

from sqlalchemy import Row, select
from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_partidos import (
    CamaraPartidos,
    CamaraPartidosArgs,
)
from utils.db import columns_to_compare, update_dict, where_clause

partidos = CamaraPartidos.__table__


def insert_camara_partidos(data: list[CamaraPartidosArgs]):
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

        conn.execute(stmt)


def get_partidos_siglas() -> Sequence[Row[Tuple[partidos, partidos]]]:
    """
    Retorna as siglas e os ids dos partidos.
    Bom para inserir o id do partido na tabela camara_deputados
    """
    with get_connection() as conn:
        stmt = select(partidos.c.id_partido, partidos.c.sigla)
        result = conn.execute(stmt)
        rows = result.fetchall()

    return rows
