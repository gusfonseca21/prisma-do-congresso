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
    name="Extract TSE Votação",
    task_run_name=TasksNames.TSE.EXTRACT.VOTACAO + "_{year}",
    cache_key_fn=cache_by_year,
    description="Faz o download e gravação de tabelas de resultado de votação da eleição do TSE.",
    retries=APP_SETTINGS.TSE.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.TSE.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.TSE.TASK_TIMEOUT,
    # cache_policy=CACHE_POLICY_MAP[APP_SETTINGS.TSE.CACHE_POLICY],
    cache_expiration=timedelta(days=APP_SETTINGS.TSE.CACHE_EXPIRATION),
    log_prints=True,
    persist_result=True,
)
def extract_votacao(year: int, id_lote: int, ignore_tasks: list[str]) -> str | None:
    logger = get_run_logger()

    if TasksNames.TSE.EXTRACT.VOTACAO in ignore_tasks:
        logger.warning(f"A Task {TasksNames.TSE.EXTRACT.VOTACAO} foi ignorada")
        return

    url = f"{APP_SETTINGS.TSE.BASE_URL}votacao_candidato_munzona/votacao_candidato_munzona_{year}.zip"

    dir_dest_path = Path(ExtractOutDir.TSE.VOTACOES) / str(year)

    file_dest_path = dir_dest_path / f"{year}.zip"

    logger.info(
        f"Fazendo download das tabelas de resultado de votação da eleição de {year}: {url}"
    )

    _tmp_zip_dest_path = download_stream(
        url=url,
        dest_path=file_dest_path,
        unzip=True,
        task=f"{TasksNames.TSE.EXTRACT.VOTACAO}_{year}",
        id_lote=id_lote,
    )

    return str(dir_dest_path)
