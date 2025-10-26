from pathlib import Path
from prefect import task, get_run_logger
from prefect.artifacts import (
    acreate_progress_artifact,
    aupdate_progress_artifact,
    acreate_table_artifact
)
from typing import cast

from utils.io import fetch_json_many_async, save_ndjson
from config.loader import load_config

APP_SETTINGS = load_config()

def frentes_membros_urls(frentes_ids: list[str]) -> list[str]:
    return [f"{APP_SETTINGS.CAMARA.REST_BASE_URL}frentes/{id}/membros" for id in frentes_ids]

@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT
)
async def extract_frentes_membros(frentes_ids: list[str], out_dir: str | Path = "data/camara") -> str:
    logger = get_run_logger()

    progress_id = await acreate_progress_artifact(
        progress=0.0,
        description="Progresso do download de membros de frentes da Câmara"
    )

    urls = frentes_membros_urls(frentes_ids)
    logger.info(f"Câmara: buscando Membros de {len(urls)} Frentes")

    jsons = await fetch_json_many_async(
        urls=urls,
        concurrency=APP_SETTINGS.CAMARA.CONCURRENCY,
        timeout=APP_SETTINGS.CAMARA.TIMEOUT,
        follow_pagination=True,
        progress_artifact_id=progress_id
    )

    await aupdate_progress_artifact(
        artifact_id=progress_id,
        progress=100.0,
        description="Downloads concluídos"
    )

    # Gerando artefato para validação dos dados
    artifact_data = []
    for i, json in enumerate(jsons):
        json = cast(dict, json)
        link_self = next(l["href"] for l in json.get("links", []) if l.get("rel") == "self")
        id_frente = link_self.split("/")[-2]
        membros = json.get("dados", []) # type: ignore
        artifact_data.append({
            "index": i,
            "id_frente": id_frente,
            "numero_membros": len(membros)
        })
    
    await acreate_table_artifact(
        key="frentes-membros",
        table=artifact_data,
        description="Total de membros encontrados nas frentes."
    )
    
    dest = Path(out_dir) / "frentes_membros.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)
