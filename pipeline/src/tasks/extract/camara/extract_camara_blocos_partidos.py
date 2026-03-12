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


def blocos_membros_urls(
    blocos_ids: list[str], logger: Logger | LoggingAdapter
) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.CAMARA.EXTRACT.BLOCOS_PARTIDOS
    )

    if not_downloaded_urls:
        logger.warning(
            f"A Tasks {TasksNames.CAMARA.EXTRACT.BLOCOS_PARTIDOS} possio URLs não baixadas nos lotes anteriores. Elas tentarão ser baixadas agora."
        )
        urls.update([error.url for error in not_downloaded_urls])

    for id in blocos_ids:
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}blocos/{id}/partidos")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.BLOCOS_PARTIDOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
async def extract_camara_blocos_partidos(
    blocos: list[dict] | None,
    id_lote: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.BLOCOS_PARTIDOS in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.EXTRACT.BLOCOS_PARTIDOS} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.BLOCOS_PARTIDOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.BLOCOS_PARTIDOS)
    if not blocos:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.BLOCOS_PARTIDOS}' pois o argumento do parâmetro 'blocos' é nulo"
        )
        return

    blocos_ids = get_ids_blocos(blocos)

    urls = blocos_membros_urls(blocos_ids, logger)
    logger.info(f"Câmara: buscando Membros de {len(urls)} blocos")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.BLOCOS_PARTIDOS,
        id_lote=id_lote,
    )

    save_ndjson(cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.BLOCOS_PARTIDOS))

    return cast(list[dict], jsons)


def get_ids_blocos(jsons: list[dict]) -> list[str]:
    blocos_ids = set()
    for json in jsons:
        blocos = json.get("dados", [])
        for bloco in blocos:
            blocos_ids.add(str(bloco.get("id")))
    return list(blocos_ids)
