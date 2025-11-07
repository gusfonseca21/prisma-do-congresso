from pathlib import Path
from prefect import task, get_run_logger
from prefect.artifacts import (
    acreate_progress_artifact,
    aupdate_progress_artifact,
    acreate_table_artifact
)
from typing import cast
from urllib.parse import urlparse, parse_qs
from datetime import date

from utils.io import fetch_json_many_async, save_ndjson
from config.loader import load_config

APP_SETTINGS = load_config()

def urls_discursos(deputados_ids: list[int], date: date) -> list[str]:
    return [f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados/{id}/discursos?dataInicio={date}&itens=1000" for id in deputados_ids]

@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT
)
async def extract_discursos_deputados(deputados_ids: list[int], date: date, out_dir: str | Path = "data/camara") -> str:
    logger = get_run_logger()

    progress_id = await acreate_progress_artifact(
        progress=0.0,
        description="Progresso do download de Discursos de Deputados"
    )

    urls = urls_discursos(deputados_ids, date)
    logger.info(f"Câmara: buscando discursos de {len(urls)} deputados")

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
        discurso = json.get("dados", []) # type: ignore

        # Pegando o id do deputado
        link_self = next(l["href"] for l in json.get("links", []) if l.get("rel") == "self")
        parsed_url = urlparse(link_self)
        path_parts = [p for p in parsed_url.path.split('/') if p]  # Remove partes vazias


        # artifact_data.append({
        #     "index": i,
        #     "id": deputado.get("id"),
        #     "nome": deputado.get("nome"),
        #     "situacao": deputado.get("ultimoStatus", {}).get("situacao"),
        #     "condicao_eleitoral": deputado.get("ultimoStatus", {}).get("condicaoEleitoral")
        # })

    # await acreate_table_artifact(
    #     key="discursos-deputados",
    #     table=artifact_data,
    #     description="Detalhes de deputados"
    # )

    dest = Path(out_dir) / "discursos.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)