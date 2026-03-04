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


def orientacoes_votacoes_urls(
    votacoes_ids: list[str], logger: Logger | LoggingAdapter
) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.CAMARA.EXTRACT.ORIENTACOES_VOTACOES
    )

    if not_downloaded_urls:
        logger.warning(
            f"A Tasks {TasksNames.CAMARA.EXTRACT.ORIENTACOES_VOTACOES} possio URLs não baixadas nos lotes anteriores. Elas tentarão ser baixadas agora."
        )
        urls.update([error.url for error in not_downloaded_urls])

    for id in votacoes_ids:
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}votacoes/{id}/orientacoes")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.ORIENTACOES_VOTACOES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_orientacoes_votacoes_camara(
    votacoes_ids: list[str] | None,
    lote_id: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.ORIENTACOES_VOTACOES in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.EXTRACT.ORIENTACOES_VOTACOES} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.ORIENTACOES_VOTACOES} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.ORIENTACOES_VOTACOES)
    if not votacoes_ids:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.ORIENTACOES_VOTACOES}' pois o argumento do parâmetro 'votacoes_ids' é nulo"
        )
        return

    urls = orientacoes_votacoes_urls(votacoes_ids, logger)

    logger.info(f"Baixando orientações de votações da Câmara de {len(urls)} URLs")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.ORIENTACOES_VOTACOES,
        lote_id=lote_id,
    )

    save_ndjson(
        cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.ORIENTACOES_VOTACOES)
    )

    return cast(list[dict], jsons)
