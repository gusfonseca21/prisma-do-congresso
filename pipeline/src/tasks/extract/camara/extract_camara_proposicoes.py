from datetime import date
from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson, save_ndjson

APP_SETTINGS = load_config()
logger = get_run_logger()


def get_ids_proposicoes(jsons: list[dict]) -> list[int]:
    return list({int(p.get("id")) for j in jsons for p in j.get("dados", [])})


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_PROPOSICOES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_proposicoes_camara(
    start_date: date,
    end_date: date,
    lote_id: int,
    ignore_tasks: list[str],
    use_files: bool,
) -> list[int] | None:

    if TasksNames.EXTRACT_CAMARA_PROPOSICOES in ignore_tasks:
        logger.warning(f"A Task {TasksNames.EXTRACT_CAMARA_PROPOSICOES} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.EXTRACT_CAMARA_PROPOSICOES} irá retornar os dados à partir do arquivo em disco."
        )
        return get_ids_proposicoes(load_ndjson(ExtractOutDir.CAMARA.PROPOSICOES))

    # url = f"{APP_SETTINGS.CAMARA.REST_BASE_URL}proposicoes?dataInicio={start_date}&dataFim={end_date}&itens=100&ordem=ASC&ordenarPor=id"
    ## Retiirando dataFim pois está bugando
    url = f"{APP_SETTINGS.CAMARA.REST_BASE_URL}proposicoes?dataInicio={start_date}&itens=100&ordem=ASC&ordenarPor=id"

    logger.info("Buscando proposições da Câmara.")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        validate_results=True,
        task=TasksNames.EXTRACT_CAMARA_PROPOSICOES,
        lote_id=lote_id,
    )

    save_ndjson(cast(list[dict], jsons), ExtractOutDir.CAMARA.PROPOSICOES)

    # OBS: ao atualizar os dados no final do dia, é possível que no meio do caminho novos dados sejam inseridos na API, o que tornará a comparação errônea pois terão mais dados sendo baixados que os contabilizados inicialmente.
    #
    return get_ids_proposicoes(cast(list[dict], jsons))
