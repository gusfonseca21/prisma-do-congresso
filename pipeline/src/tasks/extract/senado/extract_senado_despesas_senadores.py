from datetime import date, timedelta
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()


def despesas_senadores_urls(start_date: date, end_date: date) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.EXTRACT_SENADO_DESPESAS_SENADORES
    )

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    # Os Senadores têm até 3 meses para apresentar as notas fiscais
    start_date = start_date - timedelta(days=90)

    for year in range(start_date.year, end_date.year + 1):
        # O endpoint não utiliza a URL base do Senado pois é de um domínio diferente.
        urls.add(
            f"https://adm.senado.gov.br/adm-dadosabertos/api/v1/senadores/despesas_ceaps/{year}"
        )

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.EXTRACT_SENADO_DESPESAS_SENADORES,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_despesas_senado(
    start_date: date, end_date: date, lote_id: int, use_files: bool
) -> list[dict]:
    logger = get_run_logger()

    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.EXTRACT_SENADO_DESPESAS_SENADORES} irá retornar os dados à partir do arquivo em disco."
        )
        jsons = load_ndjson(ExtractOutDir.SENADO.DESPESAS_SENADORES)
        return jsons

    urls = despesas_senadores_urls(start_date, end_date)

    logger.info(f"Baixando despesas de senadores de {len(urls)} urls")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TasksNames.EXTRACT_SENADO_DESPESAS_SENADORES,
        lote_id=lote_id,
    )

    save_ndjson(cast(list[dict], jsons), ExtractOutDir.SENADO.DESPESAS_SENADORES)

    return cast(list[dict], jsons)
