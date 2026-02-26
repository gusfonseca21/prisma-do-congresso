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


def mandatos_externos_deputados_urls(deputados_ids: list[int]) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.EXTRACT_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS
    )

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    for id in deputados_ids:
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados/{id}/mandatosExternos")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_mandatos_externos_deputados(
    deputados_ids: list[int] | None,
    lote_id: int,
    ignore_tasks: list[str],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> list[dict] | None:
    logger = get_run_logger()

    if not deputados_ids:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.EXTRACT_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS}' pois o argumento do parâmetro 'deputados_ids' é nulo"
        )
        return
    if TasksNames.EXTRACT_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.EXTRACT_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS} foi ignorada"
        )
        return

    urls = mandatos_externos_deputados_urls(deputados_ids)
    logger.info(
        f"Baixando dados de Mandatos Externos de {len(urls['urls_to_download'])} Deputado"
    )

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=True,
        task=TasksNames.EXTRACT_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS,
        lote_id=lote_id,
    )

    dest = Path(out_dir) / "mandatos_externos_deputados.ndjson"

    save_ndjson(cast(list[dict], jsons), dest)

    return cast(list[dict], jsons)
