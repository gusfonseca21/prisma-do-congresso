from datetime import datetime

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_partidos import CamaraPartidosArg
from database.repository.camara.repository_camara_partidos import insert_camara_partidos
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.LOAD_CAMARA_PARTIDOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_partidos(
    lote_id: int, partidos: list[dict] | None, ignore_tasks: list[str]
):
    logger = get_run_logger()

    if TasksNames.LOAD_CAMARA_PARTIDOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.LOAD_CAMARA_PARTIDOS} foi ignorada")
        return
    if partidos is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.LOAD_CAMARA_PARTIDOS}' pois o argumento do parâmetro 'legislatura' é nulo"
        )
        return

    logger.info("Carregando Partidos da Câmara no Banco de Dados")

    partidos_data: list[CamaraPartidosArg] = []
    lideres_partidos = []

    for lp in partidos:
        p = lp.get("dados", [])
        p_status = p.get("status")

        id_lider = get_path_parameter_value(
            p_status.get("lider").get("uri"), "deputados", None
        )

        if id_lider is not None:
            id_lider = int(id_lider)
            lideres_partidos.append((id_lider, p.get("id")))

        status_data = p_status.get("data")

        if status_data is not None:
            status_data = datetime.fromisoformat(status_data)

        partidos_data.append(
            CamaraPartidosArg(
                id_lote=lote_id,
                id_partido=p.get("id"),
                sigla=p.get("sigla"),
                nome=p.get("nome"),
                status_data=status_data,
                id_legislatura=int(p_status.get("idLegislatura")),
                situacao=p_status.get("situacao"),
                total_posse=int(p_status.get("totalPosse")),
                total_membros=int(p_status.get("totalMembros")),
                id_lider=None,  # Evitar erros de constraint. Atualizamos depois de adicionar deputados
            )
        )

    insert_camara_partidos(data=partidos_data)

    return lideres_partidos
