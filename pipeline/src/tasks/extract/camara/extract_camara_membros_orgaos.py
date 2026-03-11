from datetime import date
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


def get_urls(
    orgaos: list[dict],
    start_date: date,
    end_date: date,
    logger: Logger | LoggingAdapter,
) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.CAMARA.EXTRACT.MEMBROS_ORGAOS
    )

    if not_downloaded_urls:
        logger.warning(
            f"A Tasks {TasksNames.CAMARA.EXTRACT.MEMBROS_ORGAOS} possio URLs não baixadas nos lotes anteriores. Elas tentarão ser baixadas agora."
        )
        urls.update([error.url for error in not_downloaded_urls])

    for id in get_membros_ids(orgaos):
        urls.add(
            f"{APP_SETTINGS.CAMARA.REST_BASE_URL}orgaos/{id}/membros?dataInicio={start_date}&dataFim={end_date}&itens=100"
        )

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


def get_membros_ids(orgaos: list[dict]) -> list[int]:
    ids = set()
    for item in orgaos:
        d = item.get("dados", [])
        for orgao in d:
            ids.add((orgao.get("id")))
    return list(ids)


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.MEMBROS_ORGAOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_membros_orgaos(
    orgaos: list[dict] | None,
    start_date: date,
    end_date: date,
    id_lote: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.MEMBROS_ORGAOS in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.EXTRACT.MEMBROS_ORGAOS} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.MEMBROS_ORGAOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.MEMBROS_ORGAOS)
    if not orgaos:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.MEMBROS_ORGAOS}' pois o argumento do parâmetro 'orgaos' é nulo"
        )
        return

    urls = get_urls(
        orgaos=orgaos,
        start_date=start_date,
        end_date=end_date,
        logger=logger,
    )
    logger.info(f"Baixando Membros de Órgãos de {len(urls['urls_to_download'])} URLs")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=False,  # Por algum movtivo retorna um resultado a mais
        task=TasksNames.CAMARA.EXTRACT.MEMBROS_ORGAOS,
        id_lote=id_lote,
    )

    save_ndjson(cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.MEMBROS_ORGAOS))

    return cast(list[dict], jsons)
