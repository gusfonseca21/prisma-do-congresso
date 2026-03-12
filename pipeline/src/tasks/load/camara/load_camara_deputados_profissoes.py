from datetime import datetime
from typing import Any

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_deputados import CamaraDeputadosProfissoesArg
from database.repository.camara.repository_camara_deputados import (
    insert_camara_profissoes_deputados_db,
)
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.DEPUTADOS_PROFISSOES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_deputados_profissoes(
    id_lote: int,
    profissoes: list[dict] | None,
    ignore_tasks: list[str],
    _load_deputados: Any,
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.DEPUTADOS_PROFISSOES in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.LOAD.DEPUTADOS_PROFISSOES} foi ignorada"
        )
        return
    if not profissoes:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.DEPUTADOS_PROFISSOES}' pois o argumento do parâmetro 'profissoes' é nulo"
        )
        return

    logger.info("Carregando Profissões de Deputados da Câmara no Banco de Dados")

    data: list[CamaraDeputadosProfissoesArg] = []

    ## PROFISSÕES
    for p_data in profissoes:
        href = p_data.get("links", [])[0].get("href")
        id_deputado = get_path_parameter_value(href, "deputados", None)

        profissoes_dados = p_data.get("dados", [])
        for profissao in profissoes_dados:
            data_hora = profissao.get("dataHora")
            titulo = profissao.get("titulo")
            if not titulo:
                continue

            data.append(
                CamaraDeputadosProfissoesArg(
                    id_lote=id_lote,
                    id_deputado=id_deputado,
                    data_hora=datetime.fromisoformat(data_hora) if data_hora else None,
                    titulo=profissao.get("titulo"),
                )
            )

    if data:
        insert_camara_profissoes_deputados_db(data)

    else:
        logger.warning(
            f"A lista de dados a serem inseridos no banco de dados na task {TasksNames.CAMARA.LOAD.DEPUTADOS_PROFISSOES} está vazia. A função de inserção será ignorada."
        )

    return
