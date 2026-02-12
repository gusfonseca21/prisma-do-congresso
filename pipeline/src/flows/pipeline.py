from datetime import date, datetime, timedelta

from prefect import flow, get_run_logger
from prefect.futures import resolve_futures_to_results

from config.loader import load_config
from config.parameters import FlowsNames
from database.repository.lote import end_lote_in_db, start_lote_in_db

from .camara import run_camara_flow
from .senado import run_senado_flow
from .tse import run_tse_flow

APP_SETTINGS = load_config()


@flow(
    name="Pipeline Flow",
    flow_run_name="pipeline_flow",
    description="Onde os outros Flows são chamados e coordenados.",
    log_prints=True,
)
def pipeline(
    start_date: date = datetime.now().date()
    - timedelta(days=APP_SETTINGS.FLOW.DATE_LOOKBACK),
    end_date: date = datetime.now().date(),
    refresh_cache: bool = False,
    ignore_tasks: list[str] = [],
    ignore_flows: list[str] = [],
):
    logger = get_run_logger()
    logger.info("Iniciando Pipeline ETL.")

    lote_id = start_lote_in_db()
    logger.info(f"Lote {lote_id} iniciou.")

    tse_flow = None
    if FlowsNames.TSE not in ignore_flows:
        tse_flow = run_tse_flow.submit(start_date, refresh_cache, ignore_tasks)

    camara_flow = None
    if FlowsNames.CAMARA not in ignore_flows:
        camara_flow = run_camara_flow.submit(start_date, end_date, ignore_tasks)

    senado_flow = None
    if FlowsNames.SENADO not in ignore_flows:
        senado_flow = run_senado_flow.submit(start_date, end_date, ignore_tasks)

    resolve_futures_to_results([tse_flow, camara_flow, senado_flow])
    lote_id_end = end_lote_in_db(lote_id)
    logger.info(f"Lote {lote_id_end} finalizou com sucesso")
