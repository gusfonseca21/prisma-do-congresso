import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_blocos import (
    CamaraBlocos,
    CamaraBlocosArg,
    CamaraBlocosPartidos,
    CamaraBlocosPartidosArg,
)
from database.repository.logs import insert_log_linhas_db

camara_blocos = CamaraBlocos.__table__
camara_blocos_partidos = CamaraBlocosPartidos.__table__


def insert_camara_blocos_db(data: list[CamaraBlocosArg]):
    with get_connection() as conn:
        stmt = (
            insert(camara_blocos)
            .values(
                [
                    {
                        "id_lote": item.id_lote,
                        "id_bloco": item.id_bloco,
                        "nome": item.nome,
                        "id_legislatura": item.id_legislatura,
                        "federacao": item.federacao,
                    }
                    for item in data
                ]
            )
            .on_conflict_do_nothing(index_elements=["id_bloco"])
        )

        result = conn.execute(stmt)

        total = len(data)
        inserted = result.rowcount
        updated = 0  # Não atualiza
        ignored = total - inserted

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_blocos.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )


def insert_camara_blocos_partidos_db(
    data: list[CamaraBlocosPartidosArg],
):
    with get_connection() as conn:
        stmt = insert(camara_blocos_partidos).values(
            [
                {
                    "id_lote": item.id_lote,
                    "id_bloco": item.id_bloco,
                    "sigla": item.sigla,
                    "nome": item.nome,
                }
                for item in data
            ]
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["sigla"],
            set_={
                "id_bloco": stmt.excluded.id_bloco,
            },
            where=stmt.excluded.id_bloco != camara_blocos_partidos.c.id_bloco,
        )

        stmt = stmt.returning(camara_blocos_partidos.c.id, sa.text("xmax"))

        result = conn.execute(stmt)

        rows = result.fetchall()

        total = len(data)
        inserted = sum(1 for row in rows if int(row.xmax) == 0)
        updated = sum(1 for row in rows if int(row.xmax) != 0)
        ignored = total - len(rows)

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_blocos_partidos.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )
