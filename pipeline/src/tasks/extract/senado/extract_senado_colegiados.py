from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.io import fetch_json, load_json, save_json

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.SENADO.EXTRACT.COLEGIADOS,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
)
def extract_senado_colegiados(
    id_lote: int, use_files: bool, ignore_tasks: list[str]
) -> dict | None:
    logger = get_run_logger()

    if TasksNames.SENADO.EXTRACT.COLEGIADOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.SENADO.EXTRACT.COLEGIADOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.SENADO.EXTRACT.COLEGIADOS} irá retornar os dados à partir do arquivo em disco."
        )
        json = load_json(ExtractOutDir.SENADO.COLEGIADOS)
        return json

    url = f"{APP_SETTINGS.SENADO.REST_BASE_URL}comissao/lista/colegiados"

    logger.info(f"Baixando Colegiados do Senado: {url}")

    json = fetch_json(url=url, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES)

    json = cast(dict, json)

    save_json(json, ExtractOutDir.SENADO.COLEGIADOS)

    num_colegiados = len(
        json.get("ListaColegiados", {}).get("Colegiados", {}).get("Colegiado", [])
    )

    logger.info(f"Número total de colegiados do Senado: {num_colegiados}")

    return json
