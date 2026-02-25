from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.camara.camara_legislatura import (
    CamaraLegislatura,
    CamaraLegislaturaArg,
)

camara_legislatura = CamaraLegislatura.__table__


def insert_camara_legislatura(lote_id: int, data: CamaraLegislaturaArg):
    """
    Carrega os dados da Legislatura no Banco de Dados
    """
    with get_connection() as conn:
        stmt = (
            insert(camara_legislatura)
            .values(
                id_legislatura=data.id_legislatura,
                id_lote=lote_id,
                data_inicio=data.data_inicio,
                data_fim=data.data_fim,
            )
            .on_conflict_do_nothing(index_elements=["id_legislatura"])
        )
        conn.execute(stmt)
