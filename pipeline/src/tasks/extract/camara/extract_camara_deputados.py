from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.io import fetch_json, load_json, save_json

APP_SETTINGS = load_config()

logger = get_run_logger()


def deputados_url(legislatura: dict) -> str:
    id_legislatura = legislatura.get("dados", [])[0].get("id")
    return (
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados?idLegislatura={id_legislatura}"
    )


def get_ids_deputados(json: dict) -> list[int]:
    ids_deputados = set()  # Retirar os ids duplicados. O JSON possui vários registros para os mesmos deputados
    ids_deputados_raw = [deputado.get("id") for deputado in json.get("dados", [])]
    ids_deputados.update(ids_deputados_raw)
    return list(ids_deputados)


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def extract_deputados_camara(
    legislatura: dict | None, lote_id: int, ignore_tasks: list[str], use_files: bool
) -> list[int] | None:

    if TasksNames.CAMARA.EXTRACT.DEPUTADOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.DEPUTADOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.DEPUTADOS} irá retornar os dados à partir do arquivo em disco."
        )
        return get_ids_deputados(load_json(ExtractOutDir.CAMARA.DEPUTADOS))
    if not legislatura:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.EXTRACT.DEPUTADOS}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return

    url = deputados_url(legislatura)
    logger.info(f"Câmara: buscando Deputados de {url}")
    json = fetch_json(url=url, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES)
    json = cast(dict, json)

    save_json(json, Path(ExtractOutDir.CAMARA.DEPUTADOS))

    return get_ids_deputados(json)
