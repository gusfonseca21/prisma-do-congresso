from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()
logger = get_run_logger()


def frentes_url(id_legislatura: int) -> str:
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/frentes?idLegislatura={id_legislatura}"


def get_ids_frentes(jsons: list[dict]) -> list[str]:
    frentes_ids = set()
    for json in jsons:
        frentes = json.get("dados", [])
        for frente in frentes:
            frentes_ids.add(str(frente.get("id")))
    return list(frentes_ids)


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.FRENTES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_frentes_camara(
    legislatura: dict | None, lote_id: int, ignore_tasks: list[str], use_files: bool
) -> list[str] | None:

    if TasksNames.CAMARA.EXTRACT.FRENTES in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.FRENTES} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.FRENTES} irá retornar os dados à partir do arquivo em disco."
        )
        return get_ids_frentes(load_ndjson(ExtractOutDir.CAMARA.FRENTES))
    if not legislatura:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.FRENTES}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return

    id_legislatura = legislatura["dados"][0]["id"]

    url = frentes_url(id_legislatura)

    logger.info(f"Congresso: buscando Frentes de {url}")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=True,
        task=TasksNames.CAMARA.EXTRACT.FRENTES,
        lote_id=lote_id,
    )
    jsons = cast(list[dict], jsons)

    save_ndjson(jsons, Path(ExtractOutDir.CAMARA.FRENTES))

    # Retornando ids das frentes
    frentes_ids = get_ids_frentes(jsons)

    return frentes_ids
