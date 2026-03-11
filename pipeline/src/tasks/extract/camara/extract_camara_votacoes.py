from datetime import date, timedelta
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()


def generate_urls(start_date: date, end_date: date) -> list[str]:
    # Documentação do endpoint diz que a dataInicio e dataFim só podem ser utilizadas se estiverem no mesmo ano.
    # Votações com dataInicio e dataFim com diferença maior que três meses retorna erro.
    current_start = start_date
    urls = []

    while current_start < end_date:
        current_end = current_start + timedelta(days=90)

        if current_end > end_date:
            current_end = end_date

        if current_end.year > current_start.year:
            current_end = date(current_start.year, 12, 31)

        urls.append(
            f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/votacoes?dataInicio={current_start}&dataFim={current_end}&itens=100"
        )

        current_start = current_end + timedelta(days=1)

    return urls


def get_ids_votacoes(jsons: list[dict]) -> list[str]:
    return list({str(p.get("id")) for j in jsons for p in j.get("dados", [])})  # type: ignore


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.VOTACOES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
async def extract_votacoes_camara(
    start_date: date,
    end_date: date,
    id_lote: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[str] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.VOTACOES in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.VOTACOES} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.VOTACOES} irá retornar os dados à partir do arquivo em disco."
        )
        return get_ids_votacoes(load_ndjson(ExtractOutDir.CAMARA.VOTACOES))

    urls = generate_urls(start_date, end_date)

    logger.info(f"Baixando dados de {len(urls)} URLs")

    jsons = await fetch_many_jsons(
        urls=urls,
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.VOTACOES,
        id_lote=id_lote,
    )

    save_ndjson(cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.VOTACOES))

    ids_votacoes = get_ids_votacoes(cast(list[dict], jsons))

    return ids_votacoes
