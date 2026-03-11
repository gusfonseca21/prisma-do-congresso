from datetime import date, timedelta
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()


def get_url(start_date: date, end_date: date) -> str:
    # Vamos pegar os dados de eventos futuros marcados para a Câmara.
    # Geralmente end_date será marcado como a data atual do programa ao ser executado.
    future_date = end_date + timedelta(days=7)

    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}eventos?dataInicio={start_date}&dataFim={future_date}&itens=100"


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.EVENTOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_eventos(
    start_date: date,
    end_date: date,
    id_lote: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.EVENTOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.EVENTOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.EVENTOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.EVENTOS)

    logger.info("Baixando Eventos Câmara")
    url = get_url(start_date=start_date, end_date=end_date)
    logger.info(f"Buscando Eventos da URL {url}")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=False,  # Em caso de baixar dados dos anos anteriores, necessário remover
        task=TasksNames.CAMARA.EXTRACT.EVENTOS,
        id_lote=id_lote,
    )

    save_ndjson(cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.EVENTOS))

    return cast(list[dict], jsons)
