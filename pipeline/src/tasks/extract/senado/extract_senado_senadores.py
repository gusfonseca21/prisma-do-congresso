from typing import cast

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.io import fetch_json, load_json, save_json

APP_SETTINGS = load_config()


def get_ids_senadores(json_exercicio: dict, json_afastados: dict) -> list[int]:
    ids_sens_exerc = {
        senador.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar", "")
        for senador in json_exercicio.get("ListaParlamentarEmExercicio", {})
        .get("Parlamentares", {})
        .get("Parlamentar", [])
    }

    ids_sens_afast = {
        senador.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar", "")
        for senador in json_afastados.get("AfastamentoAtual", {})
        .get("Parlamentares", {})
        .get("Parlamentar", [])
    }
    ids_senadores = list(ids_sens_exerc | ids_sens_afast)
    return ids_senadores


@task(
    task_run_name=TasksNames.SENADO.EXTRACT.SENADORES,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
)
def extract_senado_senadores(
    id_lote: int, use_files: bool, ignore_tasks: list[str]
) -> list[int] | None:
    logger = get_run_logger()

    if TasksNames.SENADO.EXTRACT.SENADORES in ignore_tasks:
        logger.warning(f"A Task {TasksNames.SENADO.EXTRACT.SENADORES} foi ignorada")
        return
    if use_files:
        logger.warning(
            f"O parâmetro 'use_files' é verdadeiro, a Task {TasksNames.SENADO.EXTRACT.SENADORES} irá retornar os dados à partir do arquivo em disco."
        )
        json_exercicio = load_json(ExtractOutDir.SENADO.SENADORES_EXERCICIO)
        json_afastados = load_json(ExtractOutDir.SENADO.SENADORES_AFASTADOS)
        ids_senadores = get_ids_senadores(
            json_exercicio=json_exercicio, json_afastados=json_afastados
        )
        return ids_senadores

    url_exerc = f"{APP_SETTINGS.SENADO.REST_BASE_URL}senador/lista/atual?v=4"
    url_afast = f"{APP_SETTINGS.SENADO.REST_BASE_URL}senador/afastados"

    logger.info(f"Baixando Senadores em exercício: {url_exerc}")
    logger.info(f"Baixando Senadores afastados: {url_afast}")

    json_exerc = fetch_json(
        url=url_exerc, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )
    json_afast = fetch_json(
        url=url_afast, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )

    json_exerc = cast(dict, json_exerc)
    json_afast = cast(dict, json_afast)

    save_json(json_exerc, ExtractOutDir.SENADO.SENADORES_EXERCICIO)
    save_json(json_afast, ExtractOutDir.SENADO.SENADORES_AFASTADOS)

    ids_senadores = get_ids_senadores(
        json_exercicio=json_exerc, json_afastados=json_afast
    )

    return ids_senadores
