from datetime import datetime
from typing import Any

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_orgaos import CamaraOrgaosDetalhesArg
from database.repository.camara.repository_camara_orgaos import (
    insert_camara_detalhes_orgaos_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.ORGAOS_DETALHES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_orgaos_detalhes(
    id_lote: int,
    detalhes_orgaos: dict | None,
    ignore_tasks: list[str],
    _load_orgaos: Any,
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.ORGAOS_DETALHES in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.ORGAOS_DETALHES} foi ignorada")
        return
    if detalhes_orgaos is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.ORGAOS_DETALHES}' pois o argumento do parâmetro 'detalhes_orgaos' é nulo"
        )
        return

    logger.info("Carregando Detalhes de Órgãos no Banco de Dados")

    data: list[CamaraOrgaosDetalhesArg] = []

    for j in detalhes_orgaos:
        detalhes_orgao_data = j.get("dados", [])
        data_inicio = detalhes_orgao_data.get("dataInicio")
        data_instalacao = detalhes_orgao_data.get("dataInstalacao")
        data_fim = detalhes_orgao_data.get("dataFim")
        data_fim_original = detalhes_orgao_data.get("dataFimOriginal")

        data.append(
            CamaraOrgaosDetalhesArg(
                id_lote=id_lote,
                id_orgao=detalhes_orgao_data.get("id"),
                data_inicio=datetime.fromisoformat(data_inicio),
                data_instalacao=datetime.fromisoformat(data_instalacao)
                if data_instalacao
                else None,
                data_fim=datetime.fromisoformat(data_fim) if data_fim else None,
                data_fim_original=datetime.fromisoformat(data_fim_original)
                if data_fim_original
                else None,
                url_website=detalhes_orgao_data.get("urlWebsite"),
            )
        )

    insert_camara_detalhes_orgaos_db(data=data)

    return
