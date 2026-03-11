from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.camara import get_current_legislatura
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()


def partidos_url(legislaturas: dict) -> str:
    id_legislatura = get_current_legislatura(legislaturas).id
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}partidos?idLegislatura={id_legislatura}&itens=100"


def get_partidos_ids(jsons: list[dict]) -> list[int]:
    return [
        partido.get("id")
        for json in jsons
        for partido in json.get("dados", [])  # type: ignore
    ]


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.PARTIDOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_partidos(
    legislaturas: dict | None, id_lote: int, ignore_tasks: list[str], use_files: bool
) -> list[int] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.PARTIDOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.PARTIDOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.PARTIDOS} irá retornar os dados à partir do arquivo em disco."
        )
        jsons = load_ndjson(ExtractOutDir.CAMARA.PARTIDOS)
        return get_partidos_ids(jsons)
    if not legislaturas:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.PARTIDOS}' pois o argumento do parâmetro 'legislaturas' é nulo"
        )
        return

    logger.info("Baixando Partidos Câmara")
    url = partidos_url(legislaturas)
    logger.info(f"Buscando Partidos da URL {url}")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.PARTIDOS,
        id_lote=id_lote,
    )

    _dest_path = save_ndjson(
        cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.PARTIDOS)
    )

    return get_partidos_ids(cast(list[dict], jsons))
