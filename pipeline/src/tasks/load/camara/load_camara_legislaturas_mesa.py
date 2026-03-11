from datetime import date
from typing import Any

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_legislaturas import CamaraLegislaturasMesaArg
from database.repository.camara.repository_camara_legislaturas import (
    insert_camara_legislaturas_mesa_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.LEGISLATURAS_MESA,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_legislaturas_mesa(
    mesa: dict,
    id_lote: int,
    ignore_tasks: list[str],
    _load_deputados: Any,
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.LEGISLATURAS_MESA in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.LOAD.LEGISLATURAS_MESA} foi ignorada"
        )
        return
    if not mesa:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.LEGISLATURAS_MESA}' pois o argumento do parâmetro 'mesa' é nulo"
        )
        return

    logger.info("Carregando Legislaturas Mesa da Câmara no Banco de Dados")

    mesa_data: list[CamaraLegislaturasMesaArg] = []

    for item in mesa.get("dados", []):
        mesa_data.append(
            CamaraLegislaturasMesaArg(
                id_lote=id_lote,
                id_deputado=item.get("id"),
                titulo=item.get("titulo"),
                data_inicio=date.fromisoformat(item.get("dataInicio")),
                data_fim=date.fromisoformat(item.get("dataFim"))
                if item.get("dataFim")
                else None,
                id_legislatura=item.get("idLegislatura"),
            )
        )

    insert_camara_legislaturas_mesa_db(data=mesa_data)

    return
