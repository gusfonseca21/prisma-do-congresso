from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.camara import get_current_legislatura
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.BLOCOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_blocos(
    legislaturas: dict, id_lote: int, ignore_tasks: list[str], use_files: bool
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.BLOCOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.BLOCOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.BLOCOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.BLOCOS)
    if not legislaturas:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.BLOCOS}' pois o argumento do parâmetro 'legislaturas' é nulo"
        )
        return

    id_legislatura = get_current_legislatura(legislaturas).id
    url = f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/blocos?idLegislatura={id_legislatura}"

    logger.info(f"Congresso: buscando blocos de {url}")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.BLOCOS,
        id_lote=id_lote,
    )
    jsons = cast(list[dict], jsons)

    save_ndjson(jsons, Path(ExtractOutDir.CAMARA.BLOCOS))

    return cast(list[dict], jsons)
