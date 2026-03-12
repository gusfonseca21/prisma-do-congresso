from datetime import date
from typing import cast

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from config.parameters import FlowsNames
from tasks.extract.camara import (
    extract_camara_blocos,
    extract_camara_blocos_partidos,
    extract_camara_deputados,
    extract_camara_deputados_assiduidade_comissoes,
    extract_camara_deputados_assiduidade_plenario,
    extract_camara_deputados_despesas,
    extract_camara_deputados_detalhes,
    extract_camara_deputados_discursos,
    extract_camara_deputados_historico,
    extract_camara_deputados_mandatos_externos,
    extract_camara_deputados_ocupacoes,
    extract_camara_deputados_profissoes,
    extract_camara_eventos,
    extract_camara_frentes,
    extract_camara_frentes_detalhes,
    extract_camara_frentes_membros,
    extract_camara_legislaturas,
    extract_camara_legislaturas_lideres,
    extract_camara_legislaturas_mesa,
    extract_camara_orgaos,
    extract_camara_orgaos_detalhes,
    extract_camara_orgaos_membros,
    extract_camara_orgaos_tipos,
    extract_camara_partidos,
    extract_camara_partidos_detalhes,
    extract_camara_proposicoes,
    extract_camara_proposicoes_autores,
    extract_camara_proposicoes_detalhes,
    extract_camara_votacoes,
    extract_camara_votacoes_detalhes,
    extract_camara_votacoes_orientacoes,
    extract_camara_votacoes_votos,
)
from tasks.load.camara import (
    load_camara_blocos,
    load_camara_blocos_partidos,
    load_camara_deputados,
    load_camara_deputados_historico,
    load_camara_deputados_mandatos_externos,
    load_camara_deputados_ocupacoes,
    load_camara_deputados_profissoes,
    load_camara_legislaturas,
    load_camara_legislaturas_lideres,
    load_camara_legislaturas_mesa,
    load_camara_orgaos,
    load_camara_orgaos_detalhes,
    load_camara_orgaos_membros,
    load_camara_orgaos_tipos,
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
    start_date: date,
    end_date: date,
    ignore_tasks: list[str],
    id_lote: int,
    use_files: bool,
):
    logger = get_run_logger()
    logger.info(f"Iniciando execução da Flow da Câmara - Lote {id_lote}")

    futures = []

    ## EXTRACT LEGISLATURA
    extract_camara_legislaturas_f = extract_camara_legislaturas(
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    # futures.append(extract_camara_legislaturas_f)

    ## LOAD LEGISLATURA
    load_camara_legislaturas_f = load_camara_legislaturas.submit(
        id_lote=id_lote,
        legislaturas=extract_camara_legislaturas_f,
        ignore_tasks=ignore_tasks,
    )
    load_camara_legislaturas_f.result()

    ## EXTRACT PARTIDOS
    extract_camara_partidos_f = extract_camara_partidos.submit(
        legislaturas=extract_camara_legislaturas_f,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_partidos_f.result()  # type: ignore

    ## EXTRACT DETALHES PARTIDOS
    extract_camara_partidos_detalhes_f = extract_camara_partidos_detalhes.submit(
        partidos_ids=extract_camara_partidos_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_partidos_detalhes_f.result()  # type: ignore

    ## LOAD PARTIDOS
    load_camara_partidos_f = load_camara_partidos.submit(
        id_lote=id_lote,
        partidos=cast(list[dict], extract_camara_partidos_detalhes_f),
        ignore_tasks=ignore_tasks,
    )
    futures.append(load_camara_partidos_f)

    # ## EXTRACT DEPUTADOS
    extract_camara_deputados_f = extract_camara_deputados.submit(
        legislaturas=extract_camara_legislaturas_f,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_deputados_f = extract_camara_deputados_f.result()  # type: ignore

    ## EXTRACT DETALHES DEPUTADOS
    extract_camara_detalhes_deputados_f = extract_camara_deputados_detalhes.submit(
        deputados_ids=extract_camara_deputados_f,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_detalhes_deputados_f.result()  # type: ignore

    ## LOAD DEPUTADOS
    load_camara_deputados_f = load_camara_deputados.submit(
        id_lote=id_lote,
        deputados=cast(list[dict], extract_camara_detalhes_deputados_f),
        ignore_tasks=ignore_tasks,
        _load_partidos=load_camara_partidos_f,
    )
    futures.append(load_camara_deputados_f)

    ## EXTRACT HISTORICO DEPUTADOS
    extract_camara_deputados_historico_f = extract_camara_deputados_historico.submit(
        id_lote=id_lote,
        deputados_ids=extract_camara_deputados_f,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_deputados_historico_f.result()  # type: ignore

    ## LOAD HISTORICO DEPUTADOS
    load_camara_deputados_historico_f = load_camara_deputados_historico.submit(
        id_lote=id_lote,
        historico_deputados=cast(list[dict], extract_camara_deputados_historico_f),
        ignore_tasks=ignore_tasks,
        _load_deputados=load_camara_deputados_f,
    )
    futures.append(load_camara_deputados_historico_f)

    ## EXTRACT MANDATOS EXTERNOS DEPUTADOS
    extract_camara_deputados_mandatos_externos_f = (
        extract_camara_deputados_mandatos_externos.submit(
            id_lote=id_lote,
            deputados_ids=extract_camara_deputados_f,
            ignore_tasks=ignore_tasks,
            use_files=use_files,
        )
    )
    extract_camara_deputados_mandatos_externos_f.result()  # type: ignore

    # LOAD MANDATOS EXTERNOS DEPUTADOS
    load_camara_deputados_mandatos_externos_f = (
        load_camara_deputados_mandatos_externos.submit(
            id_lote=id_lote,
            mandatos_externos=cast(
                list[dict], extract_camara_deputados_mandatos_externos_f
            ),
            ignore_tasks=ignore_tasks,
            load_deputados=load_camara_deputados_f,
        )
    )
    futures.append(load_camara_deputados_mandatos_externos_f)

    ## EXTRACT OCUPAÇÕES DEPUTADOS
    extract_camara_deputados_ocupacoes_f = extract_camara_deputados_ocupacoes.submit(
        id_lote=id_lote,
        deputados_ids=extract_camara_deputados_f,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_deputados_ocupacoes_f.result()  # type: ignore

    ## LOAD OCUPAÇÕES DEPUTADOS
    load_camara_deputados_ocupacoes_f = load_camara_deputados_ocupacoes.submit(
        id_lote=id_lote,
        ocupacoes=cast(list[dict], extract_camara_deputados_ocupacoes_f),
        ignore_tasks=ignore_tasks,
        load_deputados=load_camara_deputados_f,
    )
    futures.append(load_camara_deputados_ocupacoes_f)

    ## EXTRACT PROFISSÕES DEPUTADOS
    extract_camara_deputados_profissoes_f = extract_camara_deputados_profissoes.submit(
        id_lote=id_lote,
        deputados_ids=extract_camara_deputados_f,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_deputados_profissoes_f.result()  # type: ignore

    ## LOAD PROFISSÕES DEPUTADOS
    load_camara_deputados_profissoes_f = load_camara_deputados_profissoes.submit(
        id_lote=id_lote,
        profissoes=cast(list[dict], extract_camara_deputados_profissoes_f),
        ignore_tasks=ignore_tasks,
        _load_deputados=load_camara_deputados_f,
    )
    futures.append(load_camara_deputados_profissoes_f)

    ## EXTRACT LÍDERES LEGISLATURA
    extract_camara_legislaturas_lideres_f = extract_camara_legislaturas_lideres.submit(
        legislaturas=extract_camara_legislaturas_f,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_legislaturas_lideres_f.result()  # type: ignore

    ## LOAD LÍDERES LEGISLATURA
    load_camara_legislaturas_lideres_f = load_camara_legislaturas_lideres.submit(
        lideres=extract_camara_legislaturas_lideres_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        _load_deputados=load_camara_deputados_f,
    )
    futures.append(load_camara_legislaturas_lideres_f)

    ## EXTRACT MESA LEGISLATURAS
    extract_camara_legislaturas_mesa_f = extract_camara_legislaturas_mesa.submit(
        legislaturas=extract_camara_legislaturas_f,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_legislaturas_mesa_f.result()

    ## LOAD MESA LEGISLATURAS
    load_camara_legislaturas_mesa_f = load_camara_legislaturas_mesa.submit(
        mesa=extract_camara_legislaturas_mesa_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        _load_deputados=load_camara_deputados_f,
    )
    futures.append(load_camara_legislaturas_mesa_f)

    ## EXTRACT BLOCOS
    extract_camara_blocos_f = extract_camara_blocos.submit(
        legislaturas=extract_camara_legislaturas_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    futures.append(extract_camara_blocos_f)

    ## LOAD BLOCOS
    load_camara_blocos_f = load_camara_blocos.submit(
        id_lote=id_lote,
        blocos=extract_camara_blocos_f,  # type: ignore
        ignore_tasks=ignore_tasks,
    )
    futures.append(load_camara_blocos_f)

    ## EXTRACT PARTIDOS BLOCOS
    extract_camara_blocos_partidos_f = extract_camara_blocos_partidos.submit(
        blocos=extract_camara_blocos_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    futures.append(extract_camara_blocos_partidos_f)

    ## LOAD PARTIDOS BLOCOS
    load_camara_blocos_partidos_f = load_camara_blocos_partidos.submit(
        id_lote=id_lote,
        partidos_blocos=extract_camara_blocos_partidos_f,  # type: ignore
        ignore_tasks=ignore_tasks,
    )
    futures.append(load_camara_blocos_partidos_f)

    ## EXTRACT TIPOS ÓRGÃOS
    extract_camara_orgaos_tipos_f = extract_camara_orgaos_tipos.submit(
        ignore_tasks=ignore_tasks, use_files=use_files
    )
    futures.append(extract_camara_orgaos_tipos_f)

    ## LOAD TIPO ÓRGÃOS
    load_camara_orgaos_tipos_f = load_camara_orgaos_tipos.submit(
        id_lote=id_lote,
        tipos_orgaos=extract_camara_orgaos_tipos_f,  # type: ignore
        ignore_tasks=ignore_tasks,
    )
    futures.append(load_camara_orgaos_tipos_f)

    ## EXTRACT ORGAOS
    extract_camara_orgaos_f = extract_camara_orgaos.submit(
        start_date=start_date,
        end_date=end_date,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    futures.append(extract_camara_orgaos_f)

    ## LOAD ORGAOS
    load_camara_orgaos_f = load_camara_orgaos.submit(
        id_lote=id_lote,
        orgaos=extract_camara_orgaos_f,  # type: ignore
        _load_tipos_orgaos=load_camara_orgaos_tipos_f,  # type: ignore
        ignore_tasks=ignore_tasks,
    )
    futures.append(load_camara_orgaos_f)

    ## EXTRACT MEMBROS ÓRGÃOS
    extract_camara_orgaos_membros_f = extract_camara_orgaos_membros.submit(
        orgaos=extract_camara_orgaos_f,  # type: ignore
        start_date=start_date,
        end_date=end_date,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    futures.append(extract_camara_orgaos_membros_f)

    ## LOAD MEMBROS ÓRGÃOS
    load_camara_orgaos_membros_f = load_camara_orgaos_membros.submit(
        membros_orgaos=extract_camara_orgaos_membros_f,  # type: ignore
        legislaturas=extract_camara_legislaturas_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        _load_orgaos=load_camara_orgaos_f,
        _load_deputados=load_camara_deputados_f,
    )
    futures.append(load_camara_orgaos_membros_f)

    ## EXTRACT DETALHES ÓRGÃOS
    extract_camara_orgaos_detalhes_f = extract_camara_orgaos_detalhes.submit(
        orgaos=extract_camara_orgaos_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_orgaos_detalhes_f.result()  # type: ignore

    ## LOAD DETALHES ÓRGÃOS
    load_camara_orgaos_detalhes_f = load_camara_orgaos_detalhes.submit(
        detalhes_orgaos=extract_camara_orgaos_detalhes_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        _load_orgaos=load_camara_orgaos_f,
    )
    futures.append(load_camara_orgaos_detalhes_f)

    ## EXTRACT EVENTOS
    extract_camara_eventos_f = extract_camara_eventos.submit(
        start_date=start_date,
        end_date=end_date,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    futures.append(extract_camara_eventos_f)

    ## LOAD EVENTOS

    ## EXTRACT ASSIDUIDADE PLENÁRIO
    extract_camara_deputados_assiduidade_plenario_f = (
        extract_camara_deputados_assiduidade_plenario.submit(
            deputados_ids=extract_camara_deputados_f,
            start_date=start_date,
            end_date=end_date,
            id_lote=id_lote,
            ignore_tasks=ignore_tasks,
            use_files=use_files,
        )
    )
    futures.append(extract_camara_deputados_assiduidade_plenario_f)

    ## EXTRACT ASSIDUIDADE COMISSÕES
    extract_camara_deputados_assiduidade_comissoes_f = (
        extract_camara_deputados_assiduidade_comissoes.submit(
            deputados_ids=extract_camara_deputados_f,
            start_date=start_date,
            end_date=end_date,
            id_lote=id_lote,
            ignore_tasks=ignore_tasks,
            use_files=use_files,
        )
    )
    futures.append(extract_camara_deputados_assiduidade_comissoes_f)

    ## EXTRACT FRENTES
    extract_camara_frentes_f = extract_camara_frentes.submit(
        legislaturas=extract_camara_legislaturas_f,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_frentes_f.result()  # type: ignore

    ## EXTRACT FRENTES DETALHES
    extract_camara_frentes_detalhes_f = extract_camara_frentes_detalhes.submit(
        frentes_ids=extract_camara_frentes_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_frentes_detalhes_f.result()  # type: ignore

    ## EXTRACT FRENTES MEMBROS
    extract_camara_frentes_membros_f = extract_camara_frentes_membros.submit(
        frentes_ids=extract_camara_frentes_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_frentes_membros_f.result()  # type: ignore

    ## EXTRACT DISCURSOS DEPUTADOS
    extract_camara_discursos_deputados_f = extract_camara_deputados_discursos.submit(
        deputados_ids=extract_camara_deputados_f,
        start_date=start_date,
        end_date=end_date,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_discursos_deputados_f.result()  # type: ignore

    ## EXTRACT PROPOSIÇÕES
    extract_camara_proposicoes_f = extract_camara_proposicoes.submit(
        start_date=start_date,
        end_date=end_date,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_proposicoes_f.result()  # type: ignore

    ## EXTRACT DETALHES PROPOSIÇÕES
    extract_camara_detalhes_proposicoes_f = extract_camara_proposicoes_detalhes.submit(
        proposicoes_ids=extract_camara_proposicoes_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_detalhes_proposicoes_f.result()  # type: ignore

    ## EXTRACT AUTORES PROPOSIÇÕES
    extract_camara_autores_proposicoes_f = extract_camara_proposicoes_autores.submit(
        proposicoes_ids=extract_camara_proposicoes_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_autores_proposicoes_f.result()  # type: ignore

    ## EXTRACT VOTAÇÕES CÂMARA
    extract_camara_votacoes_f = extract_camara_votacoes.submit(
        start_date=start_date,
        end_date=end_date,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_votacoes_f.result()  # type: ignore

    ## EXTRACT DETALHES VOTAÇÕES
    extract_camara_detalhes_votacoes_f = extract_camara_votacoes_detalhes.submit(
        votacoes_ids=extract_camara_votacoes_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_detalhes_votacoes_f.result()  # type: ignore

    ## EXTRACT ORIENTAÇÕES VOTAÇÕES
    extract_camara_orientacoes_votacoes_f = extract_camara_votacoes_orientacoes.submit(
        votacoes_ids=extract_camara_votacoes_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_orientacoes_votacoes_f.result()  # type: ignore

    ## EXTRACT VOTOS VOTAÇÕES CÂMARA
    extract_camara_votos_votacoes_f = extract_camara_votacoes_votos.submit(
        votacoes_ids=extract_camara_votacoes_f,  # type: ignore
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_votos_votacoes_f.result()  # type: ignore

    ## EXTRACT DESPESAS DEPUTADOS
    extract_camara_despesas_deputados_f = extract_camara_deputados_despesas.submit(
        deputados_ids=extract_camara_deputados_f,  # type: ignore
        start_date=start_date,
        end_date=end_date,
        id_lote=id_lote,
        ignore_tasks=ignore_tasks,
        use_files=use_files,
    )
    extract_camara_despesas_deputados_f.result()  # type: ignore

    ### FINALIZANDO FLOW ###

    for future in futures:
        future.result()

    save_logs(
        flow_run_name=FlowsNames.CAMARA.value,
        flow_run_id=flow_run.id,
        id_lote=id_lote,
    )

    return


@task(
    name="Run Câmara Flow",
    task_run_name="run_camara_flow",
    description="Task que permite executar o Flow da Câmara de forma concorrente em relação às outras flows.",
)
def run_camara_flow(
    start_date: date,
    end_date: date,
    ignore_tasks: list[str],
    id_lote: int,
    use_files: bool,
    ignore_flows: list[str],
):
    if FlowsNames.CAMARA.value not in ignore_flows:
        camara_flow(start_date, end_date, ignore_tasks, id_lote, use_files)
