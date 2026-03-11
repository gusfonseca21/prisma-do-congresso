from datetime import date
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson
from utils.url_utils import generate_date_urls_senado

APP_SETTINGS = load_config()


def get_votacoes_urls(start_date: date, end_date: date) -> list[str] | None:
    base_url = f"{APP_SETTINGS.SENADO.REST_BASE_URL}votacao?dataInicio=%STARTDATE%&dataFim=%ENDDATE%&v=1"

    base_urls_replaced = generate_date_urls_senado(base_url, start_date, end_date)

    if base_urls_replaced is None:
        raise

    return base_urls_replaced


@task(
    task_run_name=TasksNames.SENADO.EXTRACT.VOTACOES,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
)
async def extract_votacoes_senado(
    start_date: date,
    end_date: date,
    id_lote: int,
    use_files: bool,
    ignore_tasks: list[str],
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.SENADO.EXTRACT.VOTACOES in ignore_tasks:
        logger.warning(f"A Task {TasksNames.SENADO.EXTRACT.VOTACOES} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.SENADO.EXTRACT.VOTACOES} irá retornar os dados à partir do arquivo em disco."
        )
        jsons = load_ndjson(ExtractOutDir.SENADO.VOTACOES)
        return jsons

    urls = get_votacoes_urls(start_date, end_date)

    if urls is None:
        raise

    logger.info(f"Baixando votações do Senado: {urls}")

    jsons = await fetch_many_jsons(
        urls=urls,
        not_downloaded_urls=[],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TasksNames.SENADO.EXTRACT.VOTACOES,
        id_lote=id_lote,
    )

    save_ndjson(cast(list[dict], jsons), ExtractOutDir.SENADO.VOTACOES)

    return cast(list[dict], jsons)
