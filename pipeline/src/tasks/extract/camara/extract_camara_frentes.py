from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from config.parameters import TasksNames
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def frentes_url(id_legislatura: int) -> str:
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/frentes?idLegislatura={id_legislatura}"


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_FRENTES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_frentes_camara(
    legislatura: dict | None,
    lote_id: int,
    ignore_tasks: list[str],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> list[str] | None:
    logger = get_run_logger()

    if not legislatura:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.EXTRACT_CAMARA_FRENTES}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return
    if TasksNames.EXTRACT_CAMARA_FRENTES in ignore_tasks:
        logger.warning(f"A Task {TasksNames.EXTRACT_CAMARA_FRENTES} foi ignorada")
        return

    id_legislatura = legislatura["dados"][0]["id"]

    url = frentes_url(id_legislatura)
    dest = Path(out_dir) / "frentes.ndjson"
    logger.info(f"Congresso: buscando Frentes de {url} -> {dest}")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        validate_results=True,
        task=TasksNames.EXTRACT_CAMARA_FRENTES,
        lote_id=lote_id,
    )
    jsons = cast(list[dict], jsons)

    save_ndjson(jsons, dest)

    # Retornando ids das frentes
    frentes_ids = []
    artifact_data = []
    for json in jsons:
        frentes = json.get("dados", [])
        for frente in frentes:
            frentes_ids.append(frente.get("id"))
            artifact_data.append({"id": frente.get("id"), "nome": frente.get("titulo")})

    await acreate_table_artifact(
        key="frentes-camara-membros", table=artifact_data, description="Frentes Câmara"
    )

    return frentes_ids
