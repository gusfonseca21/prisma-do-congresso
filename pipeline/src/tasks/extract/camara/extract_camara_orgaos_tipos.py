from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.io import fetch_json, load_json, save_json

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.ORGAOS_TIPOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def extract_camara_orgaos_tipos(
    ignore_tasks: list[str], use_files: bool
) -> dict | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.ORGAOS_TIPOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.ORGAOS_TIPOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.ORGAOS_TIPOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_json(ExtractOutDir.CAMARA.ORGAOS_TIPOS)

    ORGAOS_TIPOS_URL = f"{APP_SETTINGS.CAMARA.REST_BASE_URL}referencias/tiposOrgao"

    logger.info(f"Baixando Tipos Órgãos de {len(ORGAOS_TIPOS_URL)}")

    json = fetch_json(
        url=ORGAOS_TIPOS_URL, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )
    json = cast(dict, json)

    save_json(json, ExtractOutDir.CAMARA.ORGAOS_TIPOS)

    return json
