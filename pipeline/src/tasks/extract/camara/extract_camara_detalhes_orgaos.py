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


def get_urls(orgaos: list[dict], logger: Logger | LoggingAdapter) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.CAMARA.EXTRACT.DETALHES_ORGAOS
    )

    if not_downloaded_urls:
        logger.warning(
            f"A Tasks {TasksNames.CAMARA.EXTRACT.DETALHES_ORGAOS} possio URLs não baixadas nos lotes anteriores. Elas tentarão ser baixadas agora."
        )
        urls.update([error.url for error in not_downloaded_urls])

    for id in get_orgaos_ids(orgaos):
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}orgaos/{id}")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


def get_orgaos_ids(orgaos: list[dict]) -> list[int]:
    ids = set()
    for item in orgaos:
        d = item.get("dados", [])
        for orgao in d:
            ids.add((orgao.get("id")))
    return list(ids)


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.DETALHES_ORGAOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_detalhes_orgaos(
    orgaos: list[dict] | None,
    lote_id: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.DETALHES_ORGAOS in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.EXTRACT.DETALHES_ORGAOS} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.DETALHES_ORGAOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.DETALHES_ORGAOS)
    if not orgaos:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.DETALHES_ORGAOS}' pois o argumento do parâmetro 'orgaos' é nulo"
        )
        return

    urls = get_urls(orgaos, logger)
    logger.info(f"Baixando Detalhes de Órgãos de {len(urls['urls_to_download'])} URLs")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.DETALHES_ORGAOS,
        lote_id=lote_id,
    )

    save_ndjson(cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.DETALHES_ORGAOS))

    return cast(list[dict], jsons)
