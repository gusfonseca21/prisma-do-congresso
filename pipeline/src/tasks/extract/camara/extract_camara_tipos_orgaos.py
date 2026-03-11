from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.io import fetch_json, load_json, save_json

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.TIPOS_ORGAOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def extract_camara_tipos_orgaos(
    ignore_tasks: list[str], use_files: bool
) -> dict | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.TIPOS_ORGAOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.TIPOS_ORGAOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.TIPOS_ORGAOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_json(ExtractOutDir.CAMARA.TIPOS_ORGAOS)

    TIPOS_ORGAOS_URL = f"{APP_SETTINGS.CAMARA.REST_BASE_URL}referencias/tiposOrgao"

    logger.info(f"Baixando Tipos Órgãos de {TIPOS_ORGAOS_URL}")

    json = fetch_json(
        url=TIPOS_ORGAOS_URL, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )
    json = cast(dict, json)

    save_json(json, ExtractOutDir.CAMARA.TIPOS_ORGAOS)

    return json
