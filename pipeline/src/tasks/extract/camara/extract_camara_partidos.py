from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from config.parameters import TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def partidos_url(legislatura: dict) -> str:
    id_legislatura = legislatura.get("dados", [])[0].get("id")
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}partidos?idLegislatura={id_legislatura}&itens=100"


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_PARTIDOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_partidos(
    legislatura: dict | None,
    lote_id: int,
    ignore_tasks: list[str],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> list[int] | None:
    logger = get_run_logger()

    if not legislatura:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.EXTRACT_CAMARA_PARTIDOS}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return
    if TasksNames.EXTRACT_CAMARA_PARTIDOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.EXTRACT_CAMARA_PARTIDOS} foi ignorada")
        return

    logger.info("Baixando Partidos Câmara")
    url = partidos_url(legislatura)
    dest = Path(out_dir) / "partidos.ndjson"
    logger.info(f"Buscando Partidos da URL {url} -> {dest}")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=True,
        task=TasksNames.EXTRACT_CAMARA_PARTIDOS,
        lote_id=lote_id,
    )

    await acreate_table_artifact(
        key="camara-partidos",
        table=generate_artifact(jsons),
        description="Partidos Câmara",
    )

    _dest_path = save_ndjson(cast(list[dict], jsons), dest)

    # ids_partidos = [partido.get("id") for partido in json.get("dados", []) for json in jsons]
    ids_partidos = [
        partido.get("id")
        for json in jsons
        for partido in json.get("dados", [])  # type: ignore
    ]

    return ids_partidos


def generate_artifact(jsons: Any):
    artifact_data = []
    for json in jsons:
        for i, partido in enumerate(json.get("dados", [])):
            artifact_data.append(
                {
                    "index": i,
                    "id": partido.get("id"),
                    "sigla": partido.get("sigla"),
                    "nome": partido.get("nome"),
                    "uri": partido.get("uri"),
                }
            )
    return artifact_data
