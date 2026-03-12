from datetime import date, datetime, timedelta

from prefect import flow, get_run_logger
from prefect.futures import resolve_futures_to_states
from prefect.runtime import flow_run

from config.loader import load_config
from config.parameters import FlowsNames
from database.models.base import PipelineParams
from database.repository.lote import end_lote_in_db, start_lote_in_db
from utils.logs import save_logs

from .camara import run_camara_flow
from .senado import run_senado_flow
from .tse import run_tse_flow

APP_SETTINGS = load_config()


@flow(
    name="Pipeline Flow",
    flow_run_name=FlowsNames.PIPELINE.value,
    description="Onde os outros Flows são chamados e coordenados.",
    log_prints=True,
    timeout_seconds=APP_SETTINGS.FLOW.TIMEOUT,
)
def pipeline(
    start_date: date = datetime.now().date()
    - timedelta(days=APP_SETTINGS.FLOW.DATE_LOOKBACK),
    end_date: date = datetime.now().date(),
    refresh_cache: bool = False,
    ignore_tasks: list[str] = [
        ## ----> CAMARA <----
        ### EXTTRACT ###
        "extract_camara_partidos",
        "extract_camara_partidos_detalhes",
        "extract_camara_deputados_detalhes",
        "extract_camara_deputados_historico",
        "extract_camara_deputados_mandatos_externos",
        "extract_camara_deputados_ocupacoes",
        "extract_camara_deputados_profissoes",
        "extract_camara_legislaturas_mesa",
        "extract_camara_legislaturas_lideres",
        "extract_camara_blocos",
        "extract_camara_blocos_partidos",
        "extract_camara_orgaos_tipos",
        "extract_camara_orgaos",
        "extract_camara_orgaos_membros",
        "extract_camara_orgaos_detalhes",
        # "extract_camara_eventos",
        "extract_camara_deputados_assiduidade_plenario",
        "extract_camara_deputados_assiduidade_comissoes",
        "extract_camara_frentes",
        "extract_camara_frentes_detalhes",
        "extract_camara_frentes_membros",
        "extract_camara_deputados_discursos",
        "extract_camara_proposicoes",
        "extract_camara_proposicoes_detalhes",
        "extract_camara_proposicoes_autores",
        "extract_camara_votacoes",
        "extract_camara_votacoes_detalhes",
        "extract_camara_votacoes_orientacoes",
        "extract_camara_votacoes_votos",
        "extract_camara_deputados_despesas",
        ### LOAD ###
        # "load_camara_legislaturas",
        # "load_camara_partidos",
        # "load_camara_deputados",
        # "load_camara_deputados_historico",
        # "load_camara_deputados_mandatos_externos",
        # "load_camara_deputados_ocupacoes",
        # "load_camara_deputados_profissoes",
        # "load_camara_legislaturas_mesa",
        # "load_camara_legislaturas_lideres",
        # "load_camara_orgaos_tipos",
        # "load_camara_orgaos",
        # "load_camara_orgaos_membros",
        # "load_camara_orgaos_detalhes",
        # "load_camara_blocos",
        # "load_camara_blocos_partidos",
        ## ----> SENADO <----
        # "extract_colegiados_senado",
        # "extract_senado_senadores",
        # "extract_senado_senadores_detalhes",
        # "extract_senado_senadores_discursos",
        # "extract_senado_despesas_senadores",
        # "extract_senado_processos",
        # "extract_senado_processos_detalhes",
        # "extract_senado_votacoes",
    ],
    ignore_flows: list[str] = ["tse", "senado"],
    message: str | None = None,
    use_files: bool = False,
):
    logger = get_run_logger()
    logger.info("Iniciando Pipeline ETL.")

    id_lote = start_lote_in_db(
        start_date_extract=start_date,
        end_date_extract=end_date,
        params=PipelineParams(
            refresh_cache=refresh_cache,
            ignore_tasks=ignore_tasks,
            ignore_flows=ignore_flows,
            message=message,
            use_files=use_files,
        ),
    )
    logger.info(f"Lote {id_lote} iniciou.")

    futures = []

    futures.append(
        run_tse_flow.submit(
            start_date=start_date,
            refresh_cache=refresh_cache,
            ignore_tasks=ignore_tasks,
            id_lote=id_lote,
            use_files=use_files,
            ignore_flows=ignore_flows,
        )
    )

    futures.append(
        run_camara_flow.submit(
            start_date=start_date,
            end_date=end_date,
            ignore_tasks=ignore_tasks,
            id_lote=id_lote,
            use_files=use_files,
            ignore_flows=ignore_flows,
        )
    )

    futures.append(
        run_senado_flow.submit(
            start_date=start_date,
            end_date=end_date,
            ignore_tasks=ignore_tasks,
            id_lote=id_lote,
            use_files=use_files,
            ignore_flows=ignore_flows,
        )
    )

    ## Bloquea a execução do código até que todos os flows sejam finalizados
    states = resolve_futures_to_states(futures)

    all_flows_ok = all(s.is_completed() for s in states)  # type:ignore

    id_lote_end = end_lote_in_db(id_lote, all_flows_ok)
    logger.info(f"Lote {id_lote_end} finalizou com sucesso")

    save_logs(
        flow_run_name=FlowsNames.PIPELINE.value,
        flow_run_id=flow_run.id,
        id_lote=id_lote,
    )

    return
