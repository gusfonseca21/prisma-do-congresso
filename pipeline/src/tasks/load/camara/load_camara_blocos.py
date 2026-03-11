from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_blocos import CamaraBlocosArg
from database.repository.camara.repository_camara_blocos import (
    insert_camara_blocos_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.BLOCOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_blocos(
    id_lote: int, blocos: list[dict] | None, ignore_tasks: list[str]
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.BLOCOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.BLOCOS} foi ignorada")
        return
    if not blocos:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.BLOCOS}' pois o argumento do parâmetro 'blocos' é nulo"
        )
        return

    logger.info("Carregando Blocos da Câmara no Banco de Dados")

    blocos_data: list[CamaraBlocosArg] = []

    for item in blocos:
        blocos_dados = item.get("dados", [])
        for bloco in blocos_dados:
            blocos_data.append(
                CamaraBlocosArg(
                    id_lote=id_lote,
                    id_bloco=bloco.get("id"),
                    nome=bloco.get("nome"),
                    id_legislatura=bloco.get("idLegislatura"),
                    federacao=bloco.get("federacao"),
                )
            )

    insert_camara_blocos_db(data=blocos_data)

    return
