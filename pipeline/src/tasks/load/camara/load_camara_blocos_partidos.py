from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_blocos import CamaraBlocosPartidosArg
from database.repository.camara.repository_camara_blocos import (
    insert_camara_blocos_partidos_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.BLOCOS_PARTIDOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_blocos_partidos(
    id_lote: int, partidos_blocos: list[dict] | None, ignore_tasks: list[str]
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.BLOCOS_PARTIDOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.BLOCOS_PARTIDOS} foi ignorada")
        return
    if not partidos_blocos:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.BLOCOS_PARTIDOS}' pois o argumento do parâmetro 'partidos_blocos' é nulo"
        )
        return

    logger.info("Carregando Partidos de Blocos da Câmara no Banco de Dados")

    data: list[CamaraBlocosPartidosArg] = []

    for item in partidos_blocos:
        dados = item.get("dados", [])
        for item in dados:
            data.append(
                CamaraBlocosPartidosArg(
                    id_lote=id_lote,
                    id_bloco=item.get("id"),
                    sigla=item.get("sigla"),
                    nome=item.get("nome"),
                )
            )

    # Limpa registros duplicados
    data = list(
        {(partido.id_bloco, partido.sigla): partido for partido in data}.values()
    )

    if data:
        insert_camara_blocos_partidos_db(data)
    else:
        logger.warning(
            f"A lista de dados a serem inseridos no banco de dados na task {TasksNames.CAMARA.LOAD.BLOCOS_PARTIDOS} está vazia. A função de inserção será ignorada."
        )

    return
