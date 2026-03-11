from logging import Logger
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.logging.loggers import LoggingAdapter

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()


def frentes_membros_urls(
    frentes_ids: list[str], logger: Logger | LoggingAdapter
) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.CAMARA.EXTRACT.FRENTES_MEMBROS
    )

    if not_downloaded_urls:
        logger.warning(
            f"A Tasks {TasksNames.CAMARA.EXTRACT.FRENTES_MEMBROS} possio URLs não baixadas nos lotes anteriores. Elas tentarão ser baixadas agora."
        )
        urls.update([error.url for error in not_downloaded_urls])

    for id in frentes_ids:
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}frentes/{id}/membros")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.FRENTES_MEMBROS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
async def extract_frentes_membros_camara(
    frentes_ids: list[str] | None,
    id_lote: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.FRENTES_MEMBROS in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.EXTRACT.FRENTES_MEMBROS} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.FRENTES_MEMBROS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.FRENTES_MEMBROS)
    if not frentes_ids:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.FRENTES_MEMBROS}' pois o argumento do parâmetro 'frentes_ids' é nulo"
        )
        return

    urls = frentes_membros_urls(frentes_ids, logger)
    logger.info(f"Câmara: buscando Membros de {len(urls)} Frentes")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.FRENTES_MEMBROS,
        id_lote=id_lote,
    )

    save_ndjson(cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.FRENTES_MEMBROS))

    return cast(list[dict], jsons)
