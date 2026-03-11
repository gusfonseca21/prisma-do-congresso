import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_legislaturas import (
    CamaraLegislaturas,
    CamaraLegislaturasArg,
    CamaraLegislaturasLideres,
    CamaraLegislaturasLideresArg,
    CamaraLegislaturasMesa,
    CamaraLegislaturasMesaArg,
)
from database.repository.logs import insert_log_linhas_db

camara_legislatura = CamaraLegislaturas.__table__
camara_legislaturas_mesa = CamaraLegislaturasMesa.__table__
camara_legislaturas_lideres = CamaraLegislaturasLideres.__table__


def insert_camara_legislaturas_db(data: list[CamaraLegislaturasArg]):
    """
    Carrega os dados da Legislatura no Banco de Dados
    """
    with get_connection() as conn:
        stmt = (
            insert(camara_legislatura)
            .values(
                [
                    {
                        "id_legislatura": item.id_legislatura,
                        "id_lote": item.id_lote,
                        "data_inicio": item.data_inicio,
                        "data_fim": item.data_fim,
                    }
                    for item in data
                ]
            )
            .on_conflict_do_nothing(index_elements=["id_legislatura"])
        )

        result = conn.execute(stmt)

        total = len(data)  # Só carrega uma legislação por vez
        inserted = result.rowcount
        updated = 0  # Não atualiza
        ignored = total - inserted

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_legislatura.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )

        return


def insert_camara_legislaturas_mesa_db(data: list[CamaraLegislaturasMesaArg]):
    with get_connection() as conn:
        stmt = insert(camara_legislaturas_mesa).values(
            [
                {
                    "id_lote": item.id_lote,
                    "id_deputado": item.id_deputado,
                    "data_inicio": item.data_inicio,
                    "data_fim": item.data_fim,
                    "titulo": item.titulo,
                    "id_legislatura": item.id_legislatura,
                }
                for item in data
            ]
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["id_deputado", "titulo", "data_inicio"],
            set_={"data_fim": stmt.excluded.data_fim, "id_lote": stmt.excluded.id_lote},
            where=sa.and_(
                stmt.excluded.data_fim.isnot(None),
                camara_legislaturas_mesa.c.data_fim.is_(None),
            ),
        )

        stmt = stmt.returning(camara_legislaturas_mesa.c.id, sa.text("xmax"))

        result = conn.execute(stmt)
        rows = result.fetchall()

        total = len(data)
        inserted = sum(1 for row in rows if int(row.xmax) == 0)
        updated = sum(1 for row in rows if int(row.xmax) != 0)
        ignored = total - len(rows)

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_legislaturas_mesa.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )


def insert_camara_legislaturas_lideres_db(data: list[CamaraLegislaturasLideresArg]):
    with get_connection() as conn:
        stmt = insert(camara_legislaturas_lideres).values(
            [
                {
                    "id_lote": item.id_lote,
                    "id_deputado": item.id_deputado,
                    "id_legislatura": item.id_legislatura,
                    "titulo": item.titulo,
                    "bancada_tipo": item.bancada_tipo,
                    "bancada_nome": item.bancada_nome,
                    "id_bancada": item.id_bancada,
                    "data_inicio": item.data_inicio,
                    "data_fim": item.data_fim,
                }
                for item in data
            ]
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["id_deputado", "titulo", "data_inicio", "bancada_nome"],
            set_={"data_fim": stmt.excluded.data_fim, "id_lote": stmt.excluded.id_lote},
            where=sa.and_(
                stmt.excluded.data_fim.isnot(None),
                camara_legislaturas_lideres.c.data_fim.is_(None),
            ),
        )

        stmt = stmt.returning(camara_legislaturas_lideres.c.id, sa.text("xmax"))

        result = conn.execute(stmt)
        rows = result.fetchall()

        total = len(data)
        inserted = sum(1 for row in rows if int(row.xmax) == 0)
        updated = sum(1 for row in rows if int(row.xmax) != 0)
        ignored = total - len(rows)

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_legislaturas_lideres.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )
