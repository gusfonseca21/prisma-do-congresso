from datetime import date
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()


def get_url(start_date: date, end_date: date) -> str:
    ## Para download de todos os órgãos é preciso ter uma start_date mais antiga como 1980-01-01
    # Ou irá ficar faltando diversos órgãos
    start_date = date(1980, 1, 1)
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}orgaos?dataInicio={start_date}&dataFim={end_date}&itens=100"


@task(
    task_run_name=TasksNames.CAMARA.EXTRACT.ORGAOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_orgaos(
    start_date: date,
    end_date: date,
    id_lote: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[dict] | None:
    logger = get_run_logger()

    if TasksNames.CAMARA.EXTRACT.ORGAOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.EXTRACT.ORGAOS} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.CAMARA.EXTRACT.ORGAOS} irá retornar os dados à partir do arquivo em disco."
        )
        return load_ndjson(ExtractOutDir.CAMARA.ORGAOS)

    logger.info("Baixando Órgãos Câmara")
    url = get_url(start_date=start_date, end_date=end_date)
    logger.info(f"Buscando Órgãos da URL {url}")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=True,  # Em caso de baixar dados dos anos anteriores, necessário remover
        task=TasksNames.CAMARA.EXTRACT.ORGAOS,
        id_lote=id_lote,
    )

    jsons = remove_test_record(cast(list[dict], jsons))

    save_ndjson(cast(list[dict], jsons), Path(ExtractOutDir.CAMARA.ORGAOS))

    return cast(list[dict], jsons)


def remove_test_record(jsons: list[dict]) -> list[dict]:
    """
    Nos dados baixados existe um registro de teste interno da Câmara.
    """
    TEST_RECORD_ID = 539056
    result = []
    for j in jsons:
        dados = j.get("dados", [])
        cleaned_dados = [orgao for orgao in dados if orgao.get("id") != TEST_RECORD_ID]
        result.append({**j, "dados": cleaned_dados})  # preserve all other keys
    return result
