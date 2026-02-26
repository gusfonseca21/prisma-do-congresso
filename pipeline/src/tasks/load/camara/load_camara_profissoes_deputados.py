from datetime import datetime

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_deputados import CamaraDeputadosProfissoesArg
from database.repository.camara.repository_camara_deputados import (
    insert_camara_profissoes_deputados,
)
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.LOAD_CAMARA_PROFISSOES_DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_profissoes_deputados(
    lote_id: int,
    profissoes: list[dict] | None,
):
    logger = get_run_logger()

    logger.info("Carregando Profissões de Deputados da Câmara no Banco de Dados")

    if profissoes is None:
        raise ValueError(
            "Erro ao carregar dados de Profissões de Deputados no Banco de Dados: o parâmetro 'profissoes' é Nulo"
        )

    profissoes_data: list[CamaraDeputadosProfissoesArg] = []

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

            profissoes_data.append(
                CamaraDeputadosProfissoesArg(
                    id_lote=lote_id,
                    id_deputado=id_deputado,
                    data_hora=datetime.fromisoformat(data_hora) if data_hora else None,
                    titulo=profissao.get("titulo"),
                )
            )

    insert_camara_profissoes_deputados(profissoes_data=profissoes_data)

    return
