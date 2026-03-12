from datetime import date
from typing import Any

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_json, save_json

APP_SETTINGS = load_config()


def get_processos_url(start_date: date, end_date: date, logger: Any) -> list[str]:
    date_dif = end_date - start_date

    dif_days = date_dif.days

    if dif_days > 30:
        logger.warning(
            "Para download de Processos do Senado as datas têm mais de 30 dias de diferença, serão baixadas todas as proposições que entraram em tramitação na atual legislatura"
        )
        # Se for maior que 30 dias, baixa todos os Processos que tramitaram na Legislatura
        return [
            f"{APP_SETTINGS.SENADO.REST_BASE_URL}processo?tramitouLegislaturaAtual=S"
        ]
    else:
        return [
            f"{APP_SETTINGS.SENADO.REST_BASE_URL}processo?tramitouLegislaturaAtual=S&numdias={dif_days}&v=1"
        ]


@task(
    task_run_name=TasksNames.SENADO.EXTRACT.PROCESSOS,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
)
async def extract_senado_processos(
    start_date: date,
    end_date: date,
    id_lote: int,
    use_files: bool,
    ignore_tasks: list[str],
) -> list[str] | None:
    logger = get_run_logger()

    if TasksNames.SENADO.EXTRACT.PROCESSOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.SENADO.EXTRACT.PROCESSOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.SENADO.EXTRACT.PROCESSOS} irá retornar os dados à partir do arquivo em disco."
        )
        return get_processos_ids(load_json(ExtractOutDir.SENADO.PROCESSOS))

    url = get_processos_url(start_date, end_date, logger)

    logger.info(f"Baixando Processos do Senado: {url}")

    json = await fetch_many_jsons(
        urls=url,
        not_downloaded_urls=[],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TasksNames.SENADO.EXTRACT.PROCESSOS,
        id_lote=id_lote,
    )

    save_json(json, ExtractOutDir.SENADO.PROCESSOS)

    return get_processos_ids(json)


def get_processos_ids(json: Any) -> list[str]:
    if json is None:
        raise ValueError("O dict json é inválido")

    processos = json[0]

    ids = [str(processo.get("id")) for processo in processos]

    return ids
