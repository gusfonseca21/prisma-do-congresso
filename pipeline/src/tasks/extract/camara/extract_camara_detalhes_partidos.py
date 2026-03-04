from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()

logger = get_run_logger()


def detalhes_partidos_urls(partidos_ids: list[int]) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.CAMARA.EXTRACT.DETALHES_PARTIDOS
    )

    if not_downloaded_urls:
        logger.warning(
            f"A Tasks {TasksNames.CAMARA.EXTRACT.DETALHES_PARTIDOS} possio URLs não baixadas nos lotes anteriores. Elas tentarão ser baixadas agora."
        )
        urls.update([error.url for error in not_downloaded_urls])

    for id in partidos_ids:
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}partidos/{id}")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.DETALHES_PARTIDOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_detalhes_partidos(
    partidos_ids: list[int] | None,
    lote_id: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    if TasksNames.CAMARA.EXTRACT.DETALHES_PARTIDOS in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.EXTRACT.DETALHES_PARTIDOS} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.DETALHES_PARTIDOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.DETALHES_PARTIDOS)
    if not partidos_ids:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.DETALHES_PARTIDOS}' pois o argumento do parâmetro 'partidos_ids' é nulo"
        )
        return

    logger.info("Iniciando Download de Detalhes Partidos Câmara")

    urls = detalhes_partidos_urls(partidos_ids)
    logger.info(f"Baixando detalhes de {len(urls)} partidos da Câmara")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.DETALHES_PARTIDOS,
        lote_id=lote_id,
    )

    save_ndjson(cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.DETALHES_PARTIDOS))

    return cast(list[dict], jsons)
