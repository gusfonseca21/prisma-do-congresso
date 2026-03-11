from logging import Logger
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


def detalhes_senadores_urls(
    senadores_ids: list[str], logger: Logger | LoggingAdapter
) -> UrlsResult:
    urls = set()

    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.SENADO.EXTRACT.DETALHES_SENADORES
    )

    if not_downloaded_urls:
        logger.warning(
            f"A Tasks {TasksNames.SENADO.EXTRACT.DETALHES_SENADORES} possio URLs não baixadas nos lotes anteriores. Elas tentarão ser baixadas agora."
        )
        urls.update([error.url for error in not_downloaded_urls])

    for id in senadores_ids:
        urls.add(f"{APP_SETTINGS.SENADO.REST_BASE_URL}senador/{id}?v=6")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.SENADO.EXTRACT.DETALHES_SENADORES,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
)
async def extract_detalhes_senadores_senado(
    ids_senadores: list[str] | None,
    id_lote: int,
    use_files: bool,
    ignore_tasks: list[str],
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.SENADO.EXTRACT.DETALHES_SENADORES in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.SENADO.EXTRACT.DETALHES_SENADORES} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.SENADO.EXTRACT.DETALHES_SENADORES} irá retornar os dados à partir do arquivo em disco."
        )
        jsons = load_ndjson(ExtractOutDir.SENADO.DETALHES_SENADORES)
        return jsons
    if not ids_senadores:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.SENADO.EXTRACT.DETALHES_SENADORES}' pois o argumento do parâmetro 'ids_senadores' é nulo"
        )
        return

    urls = detalhes_senadores_urls(ids_senadores, logger)

    logger.info(f"Baixando detalhes de {len(urls)} URLs de Senadores")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TasksNames.SENADO.EXTRACT.DETALHES_SENADORES,
        id_lote=id_lote,
    )

    save_ndjson(cast(list[dict], jsons), ExtractOutDir.SENADO.DETALHES_SENADORES)

    return cast(list[dict], jsons)
