from datetime import datetime
from typing import Any

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_orgaos import CamaraOrgaosArg
from database.repository.camara.repository_camara_orgaos import (
    insert_camara_orgaos_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.ORGAOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_orgaos(
    id_lote: int,
    orgaos: list[dict] | None,
    ignore_tasks: list[str],
    _load_tipos_orgaos: Any,
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.ORGAOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.ORGAOS} foi ignorada")
        return
    if orgaos is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.ORGAOS}' pois o argumento do parâmetro 'orgaos' é nulo"
        )
        return

    logger.info("Carregando Órgãos no Banco de Dados")

    data: list[CamaraOrgaosArg] = []

    for item in orgaos:
        orgao = item.get("dados", [])

        data_inicio = orgao.get("dataInicio")
        data_instalacao = orgao.get("dataInstalacao")
        data_fim = orgao.get("dataFim")
        data_fim_original = orgao.get("dataFimOriginal")

        data.append(
            CamaraOrgaosArg(
                id_lote=id_lote,
                id_orgao=orgao.get("id"),
                sigla=orgao.get("sigla"),
                nome=orgao.get("nome"),
                apelido=orgao.get("apelido"),
                id_tipo_orgao=orgao.get("codTipoOrgao"),
                nome_publicacao=orgao.get("nomePublicacao"),
                nome_resumido=orgao.get("nomeResumido"),
                data_inicio=datetime.fromisoformat(data_inicio)
                if data_inicio
                else None,
                data_instalacao=datetime.fromisoformat(data_instalacao)
                if data_instalacao
                else None,
                data_fim=datetime.fromisoformat(data_fim) if data_fim else None,
                data_fim_original=datetime.fromisoformat(data_fim_original)
                if data_fim_original
                else None,
                url_website=orgao.get("urlWebsite"),
            )
        )

    if data:
        insert_camara_orgaos_db(data=data)
    else:
        logger.warning(
            f"A lista de dados a serem inseridos no banco de dados na task {TasksNames.CAMARA.LOAD.ORGAOS} está vazia. A função de inserção será ignorada."
        )

    return
