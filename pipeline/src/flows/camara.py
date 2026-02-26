from datetime import date
from typing import cast

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from config.parameters import FlowsNames
from tasks.extract.camara import (
    extract_autores_proposicoes_camara,
    extract_camara_assiduidade_comissoes,
    extract_camara_assiduidade_plenario,
    extract_camara_detalhes_frentes,
    extract_camara_detalhes_partidos,
    extract_camara_historico_deputados,
    extract_camara_legislaturas_lideres,
    extract_camara_legislaturas_mesa,
    extract_camara_mandatos_externos_deputados,
    extract_camara_ocupacoes_deputados,
    extract_camara_partidos,
    extract_camara_profissoes_deputados,
    extract_deputados_camara,
    extract_despesas_camara,
    extract_detalhes_deputados_camara,
    extract_detalhes_proposicoes_camara,
    extract_detalhes_votacoes_camara,
    extract_discursos_deputados_camara,
    extract_frentes_camara,
    extract_frentes_membros_camara,
    extract_legislatura,
    extract_orientacoes_votacoes_camara,
    extract_proposicoes_camara,
    extract_votacoes_camara,
    extract_votos_votacoes_camara,
)
from tasks.load.camara import (
    load_camara_deputados,
    load_camara_legislatura,
    load_camara_partidos,
)
from utils.logs import save_logs


@flow(
    name="Câmara Flow",
    flow_run_name="camara_flow",
    description="Orquestramento de tasks do endpoint Câmara.",
    log_prints=True,
)
def camara_flow(
    start_date: date, end_date: date, ignore_tasks: list[str], lote_id: int
):
    logger = get_run_logger()
    logger.info(f"Iniciando execução da Flow da Câmara - Lote {lote_id}")

    futures = []

    ## EXTRACT LEGISLATURA
    extract_camara_legislatura_f = extract_legislatura(
        start_date=start_date, lote_id=lote_id, ignore_tasks=ignore_tasks
    )

    ## LOAD LEGISLATURA
    load_camara_legislatura_f = load_camara_legislatura.submit(
        lote_id=lote_id,
        legislatura=extract_camara_legislatura_f,
        ignore_tasks=ignore_tasks,
    )
    futures.append(load_camara_legislatura_f)

    ## EXTRACT PARTIDOS
    extract_camara_partidos_f = extract_camara_partidos.submit(
        legislatura=extract_camara_legislatura_f,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_partidos_f.result()  # type: ignore

    ## EXTRACT DETALHES PARTIDOS
    extract_camara_detalhes_partidos_f = extract_camara_detalhes_partidos.submit(
        partidos_ids=extract_camara_partidos_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_detalhes_partidos_f.result()  # type: ignore

    ## LOAD PARTIDOS
    load_camara_partidos_f = load_camara_partidos.submit(
        lote_id=lote_id,
        partidos=cast(list[dict], extract_camara_detalhes_partidos_f),
        ignore_tasks=ignore_tasks,
    )
    futures.append(load_camara_partidos_f)

    ## EXTRACT DEPUTADOS
    extract_camara_deputados_f = extract_deputados_camara.submit(
        legislatura=extract_camara_legislatura_f,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_deputados_f = extract_camara_deputados_f.result()  # type: ignore

    ## EXTRACT DETALHES DEPUTADOS
    extract_camara_detalhes_deputados_f = extract_detalhes_deputados_camara.submit(
        deputados_ids=extract_camara_deputados_f,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_detalhes_deputados_f.result()  # type: ignore

    ## LOAD DEPUTADOS
    load_camara_deputados_f = load_camara_deputados.submit(
        lote_id=lote_id,
        deputados=cast(list[dict], extract_camara_detalhes_deputados_f),
        ignore_tasks=ignore_tasks,
    )
    futures.append(load_camara_deputados_f)

    ## EXTRACT HISTORICO DEPUTADOS
    extract_camara_historico_deputados_f = extract_camara_historico_deputados.submit(
        lote_id=lote_id,
        deputados_ids=extract_camara_deputados_f,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_historico_deputados_f.result()  # type: ignore

    ## EXTRACT MANDATOS EXTERNOS DEPUTADOS
    extract_camara_mandatos_externos_deputados_f = (
        extract_camara_mandatos_externos_deputados.submit(
            lote_id=lote_id,
            deputados_ids=extract_camara_deputados_f,
            ignore_tasks=ignore_tasks,
        )
    )
    extract_camara_mandatos_externos_deputados_f.result()  # type: ignore

    ## EXTRACT OCUPAÇÕES DEPUTADOS
    extract_camara_ocupacoes_deputados_f = extract_camara_ocupacoes_deputados.submit(
        lote_id=lote_id,
        deputados_ids=extract_camara_deputados_f,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_ocupacoes_deputados_f.result()  # type: ignore

    ## EXTRACT PROFISSÕES DEPUTADOS
    extract_camara_profissoes_deputados_f = extract_camara_profissoes_deputados.submit(
        lote_id=lote_id,
        deputados_ids=extract_camara_deputados_f,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_profissoes_deputados_f.result()  # type: ignore

    ## EXTRACT ASSIDUIDADE PLENÁRIO
    extract_camara_assiduidade_plenario_f = extract_camara_assiduidade_plenario.submit(
        deputados_ids=extract_camara_deputados_f,
        start_date=start_date,
        end_date=end_date,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    futures.append(extract_camara_assiduidade_plenario_f)

    ## EXTRACT ASSIDUIDADE COMISSÕES
    extract_camara_assiduidade_comissoes_f = (
        extract_camara_assiduidade_comissoes.submit(
            deputados_ids=extract_camara_deputados_f,
            start_date=start_date,
            end_date=end_date,
            lote_id=lote_id,
            ignore_tasks=ignore_tasks,
        )
    )
    futures.append(extract_camara_assiduidade_comissoes_f)

    ## EXTRACT FRENTES
    extract_camara_frentes_f = extract_frentes_camara.submit(
        legislatura=extract_camara_legislatura_f,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_frentes_f.result()  # type: ignore

    ## EXTRACT FRENTES DETALHES
    extract_camara_detalhes_frentes_f = extract_camara_detalhes_frentes.submit(
        frentes_ids=extract_camara_frentes_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_detalhes_frentes_f.result()  # type: ignore

    ## EXTRACT FRENTES MEMBROS
    extract_camara_frentes_membros_f = extract_frentes_membros_camara.submit(
        frentes_ids=extract_camara_frentes_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_frentes_membros_f.result()  # type: ignore

    ## EXTRACT DISCURSOS DEPUTADOS
    extract_camara_discursos_deputados_f = extract_discursos_deputados_camara.submit(
        deputados_ids=extract_camara_deputados_f,
        start_date=start_date,
        end_date=end_date,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_discursos_deputados_f.result()  # type: ignore

    ## EXTRACT PROPOSIÇÕES
    extract_camara_proposicoes_f = extract_proposicoes_camara.submit(
        start_date=start_date,
        end_date=end_date,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_proposicoes_f.result()  # type: ignore

    ## EXTRACT DETALHES PROPOSIÇÕES
    extract_camara_detalhes_proposicoes_f = extract_detalhes_proposicoes_camara.submit(
        proposicoes_ids=extract_camara_proposicoes_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_detalhes_proposicoes_f.result()  # type: ignore

    ## EXTRACT AUTORES PROPOSIÇÕES
    extract_camara_autores_proposicoes_f = extract_autores_proposicoes_camara.submit(
        proposicoes_ids=extract_camara_proposicoes_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_autores_proposicoes_f.result()  # type: ignore

    ## EXTRACT VOTAÇÕES CÂMARA
    extract_camara_votacoes_f = extract_votacoes_camara.submit(
        start_date=start_date,
        end_date=end_date,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_votacoes_f.result()  # type: ignore

    ## EXTRACT DETALHES VOTAÇÕES
    extract_camara_detalhes_votacoes_f = extract_detalhes_votacoes_camara.submit(
        votacoes_ids=extract_camara_votacoes_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_detalhes_votacoes_f.result()  # type: ignore

    ## EXTRACT ORIENTAÇÕES VOTAÇÕES
    extract_camara_orientacoes_votacoes_f = extract_orientacoes_votacoes_camara.submit(
        votacoes_ids=extract_camara_votacoes_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_orientacoes_votacoes_f.result()  # type: ignore

    ## EXTRACT VOTOS VOTAÇÕES CÂMARA
    extract_camara_votos_votacoes_f = extract_votos_votacoes_camara.submit(
        votacoes_ids=extract_camara_votacoes_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_votos_votacoes_f.result()  # type: ignore

    ## EXTRACT DESPESAS DEPUTADOS
    extract_camara_despesas_deputados_f = extract_despesas_camara.submit(
        deputados_ids=extract_camara_deputados_f,  # type: ignore
        start_date=start_date,
        end_date=end_date,
        legislatura=extract_camara_legislatura_f,  # type: ignore
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_despesas_deputados_f.result()  # type: ignore

    ## EXTRACT LÍDERES LEGISLATURA
    extract_camara_legislaturas_lideres_f = extract_camara_legislaturas_lideres.submit(
        legislatura=extract_camara_legislatura_f,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_legislaturas_lideres_f.result()  # type: ignore

    ## MESA LEGISLATURAS
    extract_camara_legislaturas_mesa_f = extract_camara_legislaturas_mesa.submit(
        legislatura=extract_camara_legislatura_f,
        lote_id=lote_id,
        ignore_tasks=ignore_tasks,
    )
    extract_camara_legislaturas_mesa_f.result()

    ### FINALIZANDO FLOW ###

    for future in futures:
        future.result()

    save_logs(
        flow_run_name=FlowsNames.CAMARA.value,
        flow_run_id=flow_run.id,
        lote_id=lote_id,
    )

    return


@task(
    name="Run Câmara Flow",
    task_run_name="run_camara_flow",
    description="Task que permite executar o Flow da Câmara de forma concorrente em relação às outras flows.",
)
def run_camara_flow(
    start_date: date, end_date: date, ignore_tasks: list[str], lote_id: int
):
    camara_flow(start_date, end_date, ignore_tasks, lote_id)
