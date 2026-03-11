from datetime import date

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_legislaturas import CamaraLegislaturasArg
from database.repository.camara.repository_camara_legislaturas import (
    insert_camara_legislaturas_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.LEGISLATURAS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_legislaturas(
    id_lote: int, legislaturas: dict | None, ignore_tasks: list[str]
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.LEGISLATURAS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.LEGISLATURAS} foi ignorada")
        return
    if legislaturas is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.LEGISLATURAS}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return

    logger.info("Carregando Legislatura no Banco de Dados")

    data: list[CamaraLegislaturasArg] = []
    legislatura_data = legislaturas.get("dados", [])
    for legislatura in legislatura_data:
        data.append(
            CamaraLegislaturasArg(
                id_lote=id_lote,
                id_legislatura=legislatura.get("id"),
                data_inicio=date.fromisoformat(legislatura.get("dataInicio")),
                data_fim=date.fromisoformat(legislatura.get("dataFim")),
            )
        )

    insert_camara_legislaturas_db(data)

    return
