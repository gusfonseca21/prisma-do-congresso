from sqlalchemy import and_, text
from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_orgaos import (
    CamaraOrgaos,
    CamaraOrgaosArg,
    CamaraOrgaosMembros,
    CamaraOrgaosMembrosArg,
    CamaraOrgaosTipos,
    CamaraOrgaosTiposArg,
)
from database.repository.logs import insert_log_linhas_db

camara_orgaos_tipos = CamaraOrgaosTipos.__table__
camara_orgaos = CamaraOrgaos.__table__
camara_orgaos_membros = CamaraOrgaosMembros.__table__


def insert_camara_orgaos_tipos_db(data: list[CamaraOrgaosTiposArg]):
    with get_connection() as conn:
        stmt = (
            insert(camara_orgaos_tipos)
            .values(
                [
                    {
                        "id_lote": tipo.id_lote,
                        "id_tipo_orgao": tipo.id_tipo_orgao,
                        "nome": tipo.nome,
                    }
                    for tipo in data
                ]
            )
            .on_conflict_do_nothing(index_elements=["id_tipo_orgao", "nome"])
        )

        result = conn.execute(stmt)

        total = len(data)
        inserted = result.rowcount
        updated = 0  # Não atualiza
        ignored = total - inserted

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_orgaos_tipos.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )

    return


def insert_camara_orgaos_db(data: list[CamaraOrgaosArg]):
    with get_connection() as conn:
        stmt = (
            insert(camara_orgaos)
            .values(
                [
                    {
                        "id_lote": orgao.id_lote,
                        "id_orgao": orgao.id_orgao,
                        "sigla": orgao.sigla,
                        "nome": orgao.nome,
                        "apelido": orgao.apelido,
                        "id_tipo_orgao": orgao.id_tipo_orgao,
                        "nome_publicacao": orgao.nome_publicacao,
                        "nome_resumido": orgao.nome_resumido,
                    }
                    for orgao in data
                ]
            )
            .on_conflict_do_nothing(index_elements=["id_orgao"])
        )

        result = conn.execute(stmt)

        total = len(data)
        inserted = result.rowcount
        updated = 0  # Não atualiza
        ignored = total - inserted

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_orgaos.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )

    return


def insert_camara_orgaos_membros_db(data: list[CamaraOrgaosMembrosArg]):
    with get_connection() as conn:
        stmt = insert(camara_orgaos_membros).values(
            [
                {
                    "id_lote": item.id_lote,
                    "id_orgao": item.id_orgao,
                    "id_deputado": item.id_deputado,
                    "id_legislatura": item.id_legislatura,
                    "titulo": item.titulo,
                    "data_inicio": item.data_inicio,
                    "data_fim": item.data_fim,
                }
                for item in data
            ]
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["id_orgao", "id_deputado", "titulo", "data_inicio"],
            set_={"data_fim": stmt.excluded.data_fim, "id_lote": stmt.excluded.id_lote},
            where=and_(
                stmt.excluded.data_fim.isnot(None),
                camara_orgaos_membros.c.data_fim.is_(None),
            ),
        )

        stmt_ocupacoes = stmt.returning(camara_orgaos_membros.c.id, text("xmax"))
        result = conn.execute(stmt_ocupacoes)
        rows = result.fetchall()
        total = len(data)
        inserted = sum(1 for row in rows if int(row.xmax) == 0)
        updated = sum(1 for row in rows if int(row.xmax) != 0)
        ignored = total - len(rows)

        insert_log_linhas_db(
            id_lote=data[0].id_lote,
            table=camara_orgaos_membros.name,
            inserted=inserted,
            updated=updated,
            ignored=ignored,
            total=total,
        )

    return
