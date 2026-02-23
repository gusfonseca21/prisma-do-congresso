from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def frentes_detalhes_urls(frentes_ids: list[str]) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.EXTRACT_CAMARA_FRENTES_MEMBROS
    )

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    for id in frentes_ids:
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}frentes/{id}")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_DETALHES_FRENTES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_detalhes_frentes(
    frentes_ids: list[str],
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = frentes_detalhes_urls(frentes_ids)
    logger.info(f"Câmara: buscando Detalhes de {len(urls)} Frentes")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        validate_results=True,
        task=TasksNames.EXTRACT_CAMARA_FRENTES_MEMBROS,
        lote_id=lote_id,
    )

    dest = Path(out_dir) / "frentes_detalhes.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)
