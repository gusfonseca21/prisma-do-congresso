from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.camara import get_current_legislatura
from utils.io import fetch_json, load_json, save_json

APP_SETTINGS = load_config()


def mesa_url(legislaturas: dict) -> str:
    id_legislatura = get_current_legislatura(legislaturas).id
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}legislaturas/{id_legislatura}/mesa"


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.LEGISLATURAS_MESA,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def extract_camara_legislaturas_mesa(
    legislaturas: dict | None, id_lote: int, ignore_tasks: list[str], use_files: bool
) -> dict | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.LEGISLATURAS_MESA in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.EXTRACT.LEGISLATURAS_MESA} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.LEGISLATURAS_MESA} irá retornar os dados à partir do arquivo em disco."
        )
        return load_json(ExtractOutDir.CAMARA.LEGISLATURAS_MESA)
    if not legislaturas:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.LEGISLATURAS_MESA}' pois o argumento do parâmetro 'legislaturas' é nulo"
        )
        return

    logger.info("Baixando Mesa Legislatura Câmara")
    url = mesa_url(legislaturas)
    logger.info(f"Buscando Mesa Legislatura da URL {url}")

    json = fetch_json(url=url, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES)

    json = cast(dict, json)

    save_json(json, Path(ExtractOutDir.CAMARA.LEGISLATURAS_MESA))

    return json
