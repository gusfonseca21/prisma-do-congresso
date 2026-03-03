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


def detalhes_proposicoes_urls(proposicoes_ids: list[int]) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES
    )

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    for id in proposicoes_ids:
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}proposicoes/{id}")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_detalhes_proposicoes_camara(
    proposicoes_ids: list[int] | None,
    lote_id: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES} foi ignorada"
        )
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.DETALHES_PROPOSICOES)
    if not proposicoes_ids:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES}' pois o argumento do parâmetro 'proposicoes_ids' é nulo"
        )
        return

    urls = detalhes_proposicoes_urls(proposicoes_ids)

    logger.info(f"Baixando detalhes de {len(urls)} URLs de Proposições da Câmara")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=True,
        task=TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES,
        lote_id=lote_id,
    )

    save_ndjson(
        cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.DETALHES_PROPOSICOES)
    )

    return cast(list[dict], jsons)
