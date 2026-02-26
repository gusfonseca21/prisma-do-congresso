from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from utils.io import fetch_json, save_json

APP_SETTINGS = load_config()


def mesa_url(legislatura: dict) -> str:
    id_legislatura = legislatura.get("dados", [])[0].get("id")
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}legislaturas/{id_legislatura}/mesa"


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_LEGISLATURAS_MESA,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def extract_camara_legislaturas_mesa(
    legislatura: dict | None,
    lote_id: int,
    ignore_tasks: list[str],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str | None:
    logger = get_run_logger()

    if not legislatura:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.EXTRACT_CAMARA_LEGISLATURAS_MESA}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return
    if TasksNames.EXTRACT_CAMARA_LEGISLATURAS_MESA in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.EXTRACT_CAMARA_LEGISLATURAS_MESA} foi ignorada"
        )
        return

    logger.info("Baixando Mesa Legislatura Câmara")
    url = mesa_url(legislatura)
    dest = Path(out_dir) / "legislaturas_mesa.json"
    logger.info(f"Buscando Mesa Legislatura da URL {url} -> {dest}")

    json = fetch_json(url=url, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES)

    json = cast(dict, json)

    dest_path = save_json(json, dest)

    return dest_path
