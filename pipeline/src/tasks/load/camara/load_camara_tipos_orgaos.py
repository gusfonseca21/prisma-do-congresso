from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_orgaos import CamaraTiposOrgaosArg
from database.repository.camara.repository_camara_orgaos import (
    insert_camara_tipos_orgaos_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.TIPOS_ORGAOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_tipos_orgaos(
    lote_id: int, tipos_orgaos: dict | None, ignore_tasks: list[str]
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.TIPOS_ORGAOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.TIPOS_ORGAOS} foi ignorada")
        return
    if tipos_orgaos is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.TIPOS_ORGAOS}' pois o argumento do parâmetro 'tipos_orgaos' é nulo"
        )
        return

    logger.info("Carregando Tipos de Órgãos no Banco de Dados")

    data: list[CamaraTiposOrgaosArg] = []

    tipos_orgaos_data = tipos_orgaos.get("dados", [])

    for tipo_orgao in tipos_orgaos_data:
        data.append(
            CamaraTiposOrgaosArg(
                id_lote=lote_id,
                id_tipo_orgao=int(tipo_orgao.get("cod")),
                nome=tipo_orgao.get("nome"),
            )
        )

    insert_camara_tipos_orgaos_db(data=data)

    return
