from datetime import date, datetime, timezone

from sqlalchemy import insert, update

from database.engine import get_connection
from database.models.base import Lote, PipelineParams

lote = Lote.__table__


def start_lote_in_db(
    start_date_extract: date, end_date_extract: date, params: PipelineParams
) -> int:
    """
    Cria um novo registro na tabela Lote para a execução atual da pipeline.
    Recebe como argumentos a data e o tempo de início do novo lote e a data de início para a extração dos dados.
    Retorna o número do novo Lote.
    """
    with get_connection() as conn:
        stmt = (
            insert(lote)
            .values(
                data_inicio_extract=start_date_extract,
                data_fim_extract=end_date_extract,
                resetar_cache=params.refresh_cache,
                tasks_ignoradas=seialize_params_list(params.ignore_tasks),
                flows_ignoradas=seialize_params_list(params.ignore_flows),
                mensagem=params.message,
                use_files=params.use_files,
            )
            .returning(lote.c.id)
        )
        result = conn.execute(stmt)
        id_lote = result.scalar()

        if not isinstance(id_lote, int):
            raise ValueError(
                "Não foi retornado um Id válido da tabela de Lote na sua geração."
            )

    return id_lote


def end_lote_in_db(id_lote: int, all_flows_ok: bool) -> int:
    """
    Finaliza o registro da execução atual da pipeline na tabela Lote.
    Recebe o Id do Lote atual como argumento.
    """
    with get_connection() as conn:
        stmt = (
            update(lote)
            .where(lote.c.id == id_lote)
            .values(
                data_fim_lote=datetime.now(timezone.utc), todos_flows_ok=all_flows_ok
            )
            .returning(lote)
        )
        result = conn.execute(stmt)

        lote_id_end = result.scalar()

        if lote_id_end is None:
            raise ValueError("Não foi possível retornar o Id do Lote finalizado.")

        return lote_id_end


def seialize_params_list(list: list[str]) -> str | None:
    """Converte lista para string separada por vírgula ou None, se vazia"""
    return ",".join(list) if list else None
