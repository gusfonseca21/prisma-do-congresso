from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.io import fetch_json, load_json, save_json

APP_SETTINGS = load_config()


def get_url() -> str:
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}legislaturas"


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.LEGISLATURAS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def extract_camara_legislaturas(
    lote_id: int, ignore_tasks: list[str], use_files: bool
) -> dict | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.LEGISLATURAS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.LEGISLATURAS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.LEGISLATURAS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_json(ExtractOutDir.CAMARA.LEGISLATURAS)

    LEGISLATURA_URL = get_url()

    logger.info(f"CÂMARA: Baixando Legislaturas de {LEGISLATURA_URL}")

    json = fetch_json(
        url=LEGISLATURA_URL, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )
    json = cast(dict, json)

    save_json(json, ExtractOutDir.CAMARA.LEGISLATURAS)

    return json
