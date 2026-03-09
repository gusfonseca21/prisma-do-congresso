from datetime import date

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_orgaos import CamaraOrgaosMembrosArg
from database.repository.camara.repository_camara_orgaos import (
    insert_camara_membros_orgaos_db,
)
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.MEMBROS_ORGAOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_membros_orgaos(
    lote_id: int, membros_orgaos: dict | None, ignore_tasks: list[str]
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.MEMBROS_ORGAOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.MEMBROS_ORGAOS} foi ignorada")
        return
    if membros_orgaos is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.MEMBROS_ORGAOS}' pois o argumento do parâmetro 'membros_orgaos' é nulo"
        )
        return

    logger.info("Carregando Membros de Órgãos no Banco de Dados")

    data: list[CamaraOrgaosMembrosArg] = []

    for orgao in membros_orgaos:
        href = orgao.get("links", [])[0].get("href")
        id_orgao = get_path_parameter_value(href, "orgaos", None)

        membros = orgao.get("dados", [])
        for membro in membros:
            data_fim = membro.get("dataFim")
            data.append(
                CamaraOrgaosMembrosArg(
                    id_lote=lote_id,
                    id_orgao=id_orgao,
                    id_deputado=membro.get("id"),
                    titulo=membro.get("titulo"),
                    data_inicio=date.fromisoformat(membro.get("dataInicio")),
                    data_fim=date.fromisoformat(data_fim) if data_fim else None,
                )
            )

    insert_camara_membros_orgaos_db(data=data)

    return
