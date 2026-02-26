from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact

from config.loader import load_config
from config.parameters import TasksNames
from utils.io import fetch_json, save_json

APP_SETTINGS = load_config()


def deputados_url(legislatura: dict) -> str:
    id_legislatura = legislatura.get("dados", [])[0].get("id")
    return (
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados?idLegislatura={id_legislatura}"
    )


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def extract_deputados_camara(
    legislatura: dict | None,
    lote_id: int,
    ignore_tasks: list[str],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> list[int] | None:
    logger = get_run_logger()

    if not legislatura:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.EXTRACT_CAMARA_DEPUTADOS}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return
    if TasksNames.EXTRACT_CAMARA_DEPUTADOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.EXTRACT_CAMARA_DEPUTADOS} foi ignorada")
        return

    url = deputados_url(legislatura)
    dest = Path(out_dir) / "deputados.json"
    logger.info(f"Câmara: buscando Deputados de {url} -> {dest}")
    json = fetch_json(url=url, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES)
    json = cast(dict, json)

    create_table_artifact(
        key="deputados",
        table=generate_artifact(json),
        description="Deputados em uma Legislatura",
    )

    _dest_path = save_json(json, dest)

    ids_deputados = set()  # Retirar os ids duplicados. O JSON possui vários registros para os mesmos deputados
    ids_deputados_raw = [deputado.get("id") for deputado in json.get("dados", [])]
    ids_deputados.update(ids_deputados_raw)

    return list(ids_deputados)


def generate_artifact(json: dict):
    artifact_data = []
    for i, deputado in enumerate(json.get("dados", [])):
        artifact_data.append(
            {
                "index": i,
                "id": deputado.get("id"),
                "nome": deputado.get("nome"),
                "partido": deputado.get("siglaPartido"),
                "uf": deputado.get("siglaUf"),
            }
        )
    return artifact_data
