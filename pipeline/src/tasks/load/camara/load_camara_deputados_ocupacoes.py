from typing import Any

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_deputados import (
    CamaraDeputadosOcupacoesArg,
)
from database.repository.camara.repository_camara_deputados import (
    insert_camara_ocupacoes_deputados_db,
)
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.DEPUTADOS_OCUPACOES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_deputados_ocupacoes(
    id_lote: int,
    ocupacoes: list[dict] | None,
    ignore_tasks: list[str],
    load_deputados: Any,
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.DEPUTADOS_OCUPACOES in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.LOAD.DEPUTADOS_OCUPACOES} foi ignorada"
        )
        return
    if not ocupacoes:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.DEPUTADOS_OCUPACOES}' pois o argumento do parâmetro 'ocupacoes' é nulo"
        )
        return

    logger.info("Carregando Ocupações de Deputados da Câmara no Banco de Dados")

    ocupacoes_data: list[CamaraDeputadosOcupacoesArg] = []

    ## OCUPAÇÕES
    for o_data in ocupacoes:
        href = o_data.get("links", [])[0].get("href")
        id_deputado = get_path_parameter_value(href, "deputados", None)

        ocupacoes_dados = o_data.get("dados", [])
        for ocupacao in ocupacoes_dados:
            titulo = ocupacao.get("titulo", None)
            ano_inicio = ocupacao.get("anoInicio", None)
            if not titulo or not ano_inicio:
                continue
            ocupacoes_data.append(
                CamaraDeputadosOcupacoesArg(
                    id_lote=id_lote,
                    id_deputado=id_deputado,
                    titulo=titulo,
                    entidade=ocupacao.get("entidade"),
                    entidade_uf=ocupacao.get("entidadeUF"),
                    entidade_pais=ocupacao.get("entidadePais"),
                    ano_inicio=int(ocupacao.get("anoInicio")),
                    ano_fim=ocupacao.get("anoFim"),
                )
            )

    # Limpa registros duplicados
    ocupacoes_data = list(
        {
            (ocupacao.id_deputado, ocupacao.titulo, ocupacao.ano_inicio): ocupacao
            for ocupacao in ocupacoes_data
        }.values()
    )

    insert_camara_ocupacoes_deputados_db(ocupacoes_data=ocupacoes_data)

    return
