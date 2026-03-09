from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_orgaos import (
    CamaraOrgaos,
    CamaraOrgaosArg,
    CamaraTiposOrgaos,
    CamaraTiposOrgaosArg,
)
from database.repository.logs import insert_log_linhas_db

camara_tipos_orgaos = CamaraTiposOrgaos.__table__
camara_orgaos = CamaraOrgaos.__table__


def insert_camara_tipos_orgaos_db(data: list[CamaraTiposOrgaosArg]):
    with get_connection() as conn:
        stmt = (
            insert(camara_tipos_orgaos)
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
            table=camara_tipos_orgaos.name,
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
