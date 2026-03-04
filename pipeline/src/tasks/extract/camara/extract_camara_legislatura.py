from datetime import date
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.io import fetch_json, load_json, save_json

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.LEGISLATURA,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def extract_legislatura(
    start_date: date, lote_id: int, ignore_tasks: list[str], use_files: bool
) -> dict | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.LEGISLATURA in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.LEGISLATURA} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.LEGISLATURA} irá retornar os dados à partir do arquivo em disco."
        )
        return load_json(ExtractOutDir.CAMARA.LEGISLATURA)

    LEGISLATURA_URL = (
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}legislaturas?data={start_date}"
    )

    logger.info(f"CÂMARA: Baixando Legislatura atual de {LEGISLATURA_URL}")

    json = fetch_json(
        url=LEGISLATURA_URL, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )
    json = cast(dict, json)

    save_json(json, ExtractOutDir.CAMARA.LEGISLATURA)

    return json
