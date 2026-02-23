import re
from datetime import date
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from selectolax.parser import HTMLParser

from config.loader import load_config
from config.parameters import TasksNames
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.io import fetch_html_many_async, save_htmls_in_zip

APP_SETTINGS = load_config()


def assiduidade_urls(
    deputados_ids: list[int], start_date: date, end_date: date
) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.EXTRACT_CAMARA_ASSIDUIDADE_COMISSOES
    )

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    for id in deputados_ids:
        for year in range(start_date.year, end_date.year + 1):
            urls.add(
                f"{APP_SETTINGS.CAMARA.PORTAL_BASE_URL}deputados/{id}/presenca-comissoes/{year}"
            )

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_ASSIDUIDADE_COMISSOES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_camara_assiduidade_comissoes(
    deputados_ids: list[int],
    start_date: date,
    end_date: date,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    """
    Baixa páginas HTML com os dados sobre a assiduidade dos Deputados em Comissões
    """
    logger = get_run_logger()

    urls = assiduidade_urls(deputados_ids, start_date, end_date)

    logger.info(
        f"Câmara: buscando Assiduidade Comissões de {len(deputados_ids)} deputados."
    )

    htmls = await fetch_html_many_async(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        lote_id=lote_id,
        task=TasksNames.EXTRACT_CAMARA_ASSIDUIDADE_COMISSOES,
    )

    href_pattern = re.compile(r"https://www\.camara\.leg\.br/deputados/\d+")
    id_ano_pattern = r"/deputados/(?P<id>\d+)\?.*ano=(?P<ano>\d+)"

    htmls_list = []

    # Caso ocorra erro na hora do download do HTML, eles podem retornar como None
    # Para evitar erro, limpar os None. O erro já deve ter sido pêgo no download.
    htmls = list(filter(None, htmls))

    for html in htmls:
        tree = HTMLParser(cast(str, html))
        all_links = tree.css("a")
        for link in all_links:
            href = link.attributes.get("href", "")
            if isinstance(href, str):
                if href_pattern.match(href):
                    match = re.search(id_ano_pattern, href)
                    if match:
                        deputado_id = int(match.group("id"))
                        year = int(match.group("ano"))

                        htmls_list.append(
                            {"deputado_id": deputado_id, "ano": year, "html": html}
                        )

                    else:
                        logger.warning(
                            "Não foram encontrados dados suficientes na página HTML"
                        )
            else:
                logger.warning(f"O href {href} não é string")

    dest = Path(out_dir) / "assiduidade_comissoes.zip"

    dest_path = save_htmls_in_zip(htmls_list, dest)

    return str(dest_path)
