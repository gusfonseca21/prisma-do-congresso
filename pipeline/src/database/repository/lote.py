from datetime import datetime, timezone

from sqlalchemy import insert, update

from database.engine import get_connection
from database.models.models import Lote

lote = Lote.__table__


def start_lote_in_db() -> int:
    """
    Cria um novo registro na tabela Lote para a execução atual da pipeline.
    Recebe como argumentos a data e o tempo de início do novo lote e a data de início para a extração dos dados.
    Retorna o número do novo Lote.
    """
    with get_connection() as conn:
        stmt = insert(lote).returning(lote.c.id)
        result = conn.execute(stmt)
        lote_id = result.scalar()

        if not isinstance(lote_id, int):
            raise ValueError(
                "Não foi retornado um Id válido da tabela de Lote na sua geração."
            )

    return lote_id


def end_lote_in_db(lote_id: int) -> int:
    """
    Finaliza o registro da execução atual da pipeline na tabela Lote.
    Recebe o Id do Lote atual como argumento.
    """
    with get_connection() as conn:
        stmt = (
            update(lote)
            .where(lote.c.id == lote_id)
            .values(data_fim=datetime.now(timezone.utc))
            .returning(lote)
        )
        result = conn.execute(stmt)

        lote_id_end = result.scalar()

        if lote_id_end is None:
            raise ValueError("Não foi possível retornar o Id do Lote finalizado.")

        return lote_id_end
