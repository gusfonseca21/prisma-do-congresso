from datetime import date

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_legislatura import CamaraLegislaturaArg
from database.repository.camara.repository_camara_legislatura import (
    insert_camara_legislatura,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.LOAD_CAMARA_LEGISLATURA,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_legislatura(
    lote_id: int,
    legislatura: dict | None,
):
    logger = get_run_logger()

    logger.info("Carregando Legislatura no Banco de Dados")

    if legislatura is None:
        raise ValueError(
            "Erro ao carregar dados de Legislatura no Banco de Dados: o parâmetro legislatura é Nulo"
        )

    legislatura_data = legislatura.get("dados", [])[0]

    data = CamaraLegislaturaArg(
        id_legislatura=legislatura_data.get("id"),
        data_inicio=date.fromisoformat(legislatura_data.get("dataInicio")),
        data_fim=date.fromisoformat(legislatura_data.get("dataFim")),
    )

    insert_camara_legislatura(lote_id=lote_id, data=data)

    return
