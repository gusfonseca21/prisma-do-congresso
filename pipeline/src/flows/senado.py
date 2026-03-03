from datetime import date

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from config.parameters import FlowsNames
from tasks.extract.senado import (
    extract_colegiados,
    extract_despesas_senado,
    extract_detalhes_processos_senado,
    extract_detalhes_senadores_senado,
    extract_discursos_senado,
    extract_processos_senado,
    extract_senadores_senado,
    extract_votacoes_senado,
)
from utils.logs import save_logs


@flow(
    name="Senado Flow",
    flow_run_name="senado_flow",
    description="Orquestramento de tasks do endpoint Senado.",
    log_prints=True,
)
def senado_flow(
    start_date: date, end_date: date, ignore_tasks: list[str], lote_id, use_files: bool
):
    logger = get_run_logger()
    logger.info(f"Iniciando execução da Flow do Senado - Lote {lote_id}")

    futures = []

    ## COLEGIADOS
    extract_colegiados_senado_f = extract_colegiados.submit(
        lote_id=lote_id,
        use_files=use_files,
        ignore_tasks=ignore_tasks,
    )
    futures.append(extract_colegiados_senado_f)

    ## SENADORES
    extract_senadores_senado_f = extract_senadores_senado.submit(
        lote_id=lote_id,
        use_files=use_files,
        ignore_tasks=ignore_tasks,
    )
    extract_senadores_senado_f.result()

    ## DETALHES SENADORES
    extract_detalhes_senadores_senado_f = extract_detalhes_senadores_senado.submit(
        ids_senadores=extract_senadores_senado_f,  # type: ignore
        lote_id=lote_id,
        use_files=use_files,
        ignore_tasks=ignore_tasks,
    )
    extract_detalhes_senadores_senado_f.result()  # type: ignore

    ## DISCURSOS SENADORES
    extract_discursos_senado_f = extract_discursos_senado.submit(
        ids_senadores=extract_senadores_senado_f,  # type: ignore
        start_date=start_date,
        end_date=end_date,
        lote_id=lote_id,
        use_files=use_files,
        ignore_tasks=ignore_tasks,
    )
    extract_discursos_senado_f.result()  # type: ignore

    ## DESPESAS SENADORES
    extract_despesas_senado_f = extract_despesas_senado.submit(
        start_date=start_date,
        end_date=end_date,
        lote_id=lote_id,
        use_files=use_files,
        ignore_tasks=ignore_tasks,
    )
    futures.append(extract_despesas_senado_f)

    ## PROCESSOS SENADO
    extract_processos_senado_f = extract_processos_senado.submit(
        start_date=start_date,
        end_date=end_date,
        lote_id=lote_id,
        use_files=use_files,
        ignore_tasks=ignore_tasks,
    )
    extract_processos_senado_f.result()  # type: ignore

    ## DETALHES PROCESSOS
    extract_detalhes_processos_senado_f = extract_detalhes_processos_senado.submit(
        ids_processos=extract_processos_senado_f,  # type: ignore
        lote_id=lote_id,
        use_files=use_files,
        ignore_tasks=ignore_tasks,
    )
    extract_detalhes_processos_senado_f.result()  # type: ignore

    ## VOTACOES
    extract_votacoes_senado_f = extract_votacoes_senado.submit(
        start_date=start_date,
        end_date=end_date,
        lote_id=lote_id,
        use_files=use_files,
        ignore_tasks=ignore_tasks,
    )
    futures.append(extract_votacoes_senado_f)

    # Para finalizar o Flow corretamente na GUI do servidor, é preciso resolver os futures dos endpoints que não foram passados para outras tasks.
    for future in futures:
        future.result()

    save_logs(
        flow_run_name=FlowsNames.SENADO.value,
        flow_run_id=flow_run.id,
        lote_id=lote_id,
    )

    return


@task(
    name="Run Senado Flow",
    task_run_name="run_senado_flow",
    description="Task que permite executar o Flow do Senado de forma concorrente em relação às outras flows.",
)
def run_senado_flow(
    start_date: date,
    end_date: date,
    ignore_tasks: list[str],
    lote_id: int,
    use_files: bool,
    ignore_flows: list[str],
):
    if FlowsNames.SENADO.value not in ignore_flows:
        senado_flow(start_date, end_date, ignore_tasks, lote_id, use_files)
