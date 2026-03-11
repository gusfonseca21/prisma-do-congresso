from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_blocos import CamaraBlocosPartidosArg
from database.repository.camara.repository_camara_blocos import (
    insert_camara_blocos_partidos_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.PARTIDOS_BLOCOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_partidos_blocos(
    id_lote: int, partidos_blocos: list[dict] | None, ignore_tasks: list[str]
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.PARTIDOS_BLOCOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.PARTIDOS_BLOCOS} foi ignorada")
        return
    if not partidos_blocos:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.PARTIDOS_BLOCOS}' pois o argumento do parâmetro 'partidos_blocos' é nulo"
        )
        return

    logger.info("Carregando Partidos de Blocos da Câmara no Banco de Dados")

    blocos_partidos_data: list[CamaraBlocosPartidosArg] = []

    for item in partidos_blocos:
        data = item.get("dados", [])
        for item in data:
            blocos_partidos_data.append(
                CamaraBlocosPartidosArg(
                    id_lote=id_lote,
                    id_bloco=item.get("id"),
                    sigla=item.get("sigla"),
                    nome=item.get("nome"),
                )
            )

    # Limpa registros duplicados
    blocos_partidos_data = list(
        {
            (partido.id_bloco, partido.sigla): partido
            for partido in blocos_partidos_data
        }.values()
    )

    insert_camara_blocos_partidos_db(data=blocos_partidos_data)

    return
