from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_orgaos import CamaraTiposOrgaos, CamaraTiposOrgaosArg
from database.repository.logs import insert_log_linhas_db

camara_tipos_orgaos = CamaraTiposOrgaos.__table__


def insert_camara_tipos_orgaos_db(data: list[CamaraTiposOrgaosArg]):
    with get_connection() as conn:
        stmt = (
            insert(camara_tipos_orgaos)
            .values(
                [
                    {
                        "id_lote": tipo.id_lote,
                        "codigo": tipo.codigo,
                        "nome": tipo.nome,
                    }
                    for tipo in data
                ]
            )
            .on_conflict_do_nothing(index_elements=["codigo", "nome"])
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
