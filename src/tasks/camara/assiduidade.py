from pathlib import Path
from prefect import task, get_run_logger
from prefect.artifacts import (
    acreate_progress_artifact,
    aupdate_progress_artifact,
    acreate_table_artifact
)
from typing import cast
import re
from selectolax.parser import HTMLParser
from datetime import date, timedelta

from utils.io import fetch_html_many_async, save_ndjson
from config.loader import load_config

APP_SETTINGS = load_config()

def assiduidade_urls(deputados_ids: list[str], year: int) -> list[str]:
    return [
        f"{APP_SETTINGS.CAMARA.PORTAL_BASE_URL}deputados/{id}/presenca-plenario/{year}"
        for id in deputados_ids
    ]

@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=1800,
    cache_key_fn=lambda _, year: None if year == date.today().year else f"assiduidade:{year}",
    cache_expiration=timedelta(days=180)
)
async def extract_assiduidade_deputados(
        deputados_ids: list[str],
        legislatura_year: int,
        out_dir: str | Path = "data/camara"
) -> str:
    logger = get_run_logger()

    progress_id = await acreate_progress_artifact(
        progress=0.0,
        description="Progresso do download da assiduiadde de deputados"
    )

    urls = assiduidade_urls(deputados_ids, legislatura_year)
    logger.info(f"Câmara: buscando assiduidade de {len(deputados_ids)} deputados do ano {legislatura_year}")

    htmls = await fetch_html_many_async(
        urls=urls,
        concurrency=APP_SETTINGS.CAMARA.CONCURRENCY,
        timeout=APP_SETTINGS.CAMARA.TIMEOUT,
        progress_artifact_id=progress_id
    )

    await aupdate_progress_artifact(
        artifact_id=progress_id,
        progress=100.0,
        description="Downloads concluídos"
    )
    
    href_pattern = re.compile(r'https://www\.camara\.leg\.br/deputados/\d+')
    id_ano_pattern= r'/deputados/(?P<id>\d+)\?.*ano=(?P<ano>\d+)'

    # Montando os resultados JSON e o artefato
    artifact_data = []
    json_results = []
    for html in htmls:
        tree = HTMLParser(cast(str, html))
        all_links = tree.css("a")
        for link in all_links:
            href = link.attributes.get("href", "")
            if isinstance(href, str):
                if href_pattern.match(href):
                    match = re.search(id_ano_pattern, href)
                    if match:
                        deputado_id = int(match.group('id'))
                        year = int(match.group('ano'))

                        json_results.append({
                            "deputado_id": deputado_id,
                            "ano": year,
                            "html": html
                        })

                        tables = tree.css('table.table.table-bordered')
                        name = tree.css_first("h1.titulo-internal")
                        name_text = name.text(strip=True) if name else None
                        artifact_row = {
                            "id": deputado_id,
                            "nome": name_text,
                            "ano": year
                        }
                        if tables:
                            artifact_row["possui_dados"] = "Sim"
                        else:
                            artifact_row["possui_dados"] = "Não"
                        artifact_data.append(artifact_row)
                    else:
                        logger.warning(f"Não foram encontrados dados suficientes na página HTML")
            else:
                logger.warning(f"O href {href} não é string")

    await acreate_table_artifact(
        key="assiduidade",
        table=artifact_data,
        description="Assiduidade de deputados"
    )

    dest = Path(out_dir) / "assiduidade.ndjson"

    dest_path = save_ndjson(json_results, dest)

    return dest_path