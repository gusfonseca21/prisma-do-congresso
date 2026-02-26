from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def lideres_url(legislatura: dict) -> str:
    id_legislatura = legislatura.get("dados", [])[0].get("id")
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}legislaturas/{id_legislatura}/lideres?itens=100"


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_LEGISLATURAS_LIDERES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_legislaturas_lideres(
    legislatura: dict | None,
    lote_id: int,
    ignore_tasks: list[str],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str | None:
    logger = get_run_logger()

    if not legislatura:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.EXTRACT_CAMARA_LEGISLATURAS_LIDERES}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return
    if TasksNames.EXTRACT_CAMARA_LEGISLATURAS_LIDERES in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.EXTRACT_CAMARA_LEGISLATURAS_LIDERES} foi ignorada"
        )
        return

    logger.info("Baixando Líderes Legislatura Câmara")
    url = lideres_url(legislatura)
    dest = Path(out_dir) / "legislaturas_lideres.ndjson"
    logger.info(f"Buscando Líderes Legislatura da URL {url} -> {dest}")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=True,
        task=TasksNames.EXTRACT_CAMARA_LEGISLATURAS_LIDERES,
        lote_id=lote_id,
    )

    dest_path = save_ndjson(cast(list[dict], jsons), dest)

    return dest_path
