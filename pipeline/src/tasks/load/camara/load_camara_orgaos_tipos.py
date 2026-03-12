from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_orgaos import CamaraOrgaosTiposArg
from database.repository.camara.repository_camara_orgaos import (
    insert_camara_orgaos_tipos_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.ORGAOS_TIPOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_orgaos_tipos(
    id_lote: int, tipos_orgaos: dict | None, ignore_tasks: list[str]
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.ORGAOS_TIPOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.ORGAOS_TIPOS} foi ignorada")
        return
    if tipos_orgaos is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.ORGAOS_TIPOS}' pois o argumento do parâmetro 'tipos_orgaos' é nulo"
        )
        return

    logger.info("Carregando Tipos de Órgãos no Banco de Dados")

    data: list[CamaraOrgaosTiposArg] = []

    tipos_orgaos_data = tipos_orgaos.get("dados", [])

    for tipo_orgao in tipos_orgaos_data:
        data.append(
            CamaraOrgaosTiposArg(
                id_lote=id_lote,
                id_tipo_orgao=int(tipo_orgao.get("cod")),
                nome=tipo_orgao.get("nome"),
            )
        )

    if data:
        insert_camara_orgaos_tipos_db(data)
    else:
        logger.warning(
            f"A lista de dados a serem inseridos no banco de dados na task {TasksNames.CAMARA.LOAD.ORGAOS_TIPOS} está vazia. A função de inserção será ignorada."
        )

    return
