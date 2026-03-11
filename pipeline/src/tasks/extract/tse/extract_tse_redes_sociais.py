from datetime import timedelta
from pathlib import Path

from prefect import get_run_logger, task

from config.loader import CACHE_POLICY_MAP, load_config
from config.parameters import ExtractOutDir, TasksNames
from utils.io import download_stream

APP_SETTINGS = load_config()


def cache_by_year(_ctx, params):
    return f"extract_candidatos:{params['year']}"


@task(
    name="Extract TSE Redes Sociais",
    task_run_name=TasksNames.TSE.EXTRACT.REDES_SOCIAIS + "_{uf}_{year}",
    cache_key_fn=cache_by_year,
    description="Faz o download e gravação de tabelas de redes sociais de candidatos do TSE.",
    retries=APP_SETTINGS.TSE.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.TSE.TASK_RETRY_DELAY,
    # cache_policy=CACHE_POLICY_MAP[APP_SETTINGS.TSE.CACHE_POLICY],
    cache_expiration=timedelta(days=APP_SETTINGS.TSE.CACHE_EXPIRATION),
    log_prints=True,
    persist_result=True,
)
def extract_redes_sociais(
    year: int, uf: str, id_lote: int, ignore_tasks: list[str]
) -> str | None:
    logger = get_run_logger()

    if TasksNames.TSE.EXTRACT.REDES_SOCIAIS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.TSE.EXTRACT.REDES_SOCIAIS} foi ignorada")
        return

    url = f"{APP_SETTINGS.TSE.BASE_URL}consulta_cand/rede_social_candidato_{year}_{uf}.zip"

    dir_dest_path = Path(ExtractOutDir.TSE.REDES_SOCIAIS) / str(year)

    file_dest_path = dir_dest_path / f"{uf}_{year}.zip"

    logger.info(
        f"Fazendo download das tabelas de redes sociais dos candidatos do estado {uf} da eleição de {year}: {url}"
    )

    _tmp_zip_dest_path = download_stream(
        url=url,
        dest_path=file_dest_path,
        unzip=True,
        task=f"{TasksNames.TSE.EXTRACT.REDES_SOCIAIS}_{uf}_{year}",
        id_lote=id_lote,
    )

    return str(dir_dest_path)
