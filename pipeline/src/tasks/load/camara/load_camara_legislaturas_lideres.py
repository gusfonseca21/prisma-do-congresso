from datetime import date
from typing import Any

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_legislaturas import CamaraLegislaturasLideresArg
from database.repository.camara.repository_camara_legislaturas import (
    insert_camara_legislaturas_lideres_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.LEGISLATURAS_LIDERES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_legislaturas_lideres(
    lideres: list[dict],
    lote_id: int,
    ignore_tasks: list[str],
    _load_deputados: Any,
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.LEGISLATURAS_LIDERES in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.LOAD.LEGISLATURAS_LIDERES} foi ignorada"
        )
        return
    if not lideres:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.LEGISLATURAS_LIDERES}' pois o argumento do parâmetro 'lideres' é nulo"
        )
        return

    logger.info("Carregando Legislaturas Líderes da Câmara no Banco de Dados")

    lider_data: list[CamaraLegislaturasLideresArg] = []

    for item in lideres:
        for lider in item.get("dados", []):
            parlamentar = lider.get("parlamentar")
            bancada = lider.get("bancada")
            bancada_uri = bancada.get("uri")

            id_bancada = None
            if bancada_uri:
                id_bancada = get_id_bancada(bancada_uri)

            lider_data.append(
                CamaraLegislaturasLideresArg(
                    id_lote=lote_id,
                    id_deputado=parlamentar.get("id"),
                    id_legislatura=parlamentar.get("idLegislatura"),
                    titulo=lider.get("titulo"),
                    bancada_tipo=bancada.get("tipo"),
                    bancada_nome=bancada.get("nome"),
                    id_bancada=id_bancada,
                    data_inicio=date.fromisoformat(lider.get("dataInicio")),
                    data_fim=date.fromisoformat(lider.get("dataFim"))
                    if lider.get("dataFim")
                    else None,
                )
            )

    # Limpa registros duplicados
    lider_data = list(
        {
            (
                lider.id_deputado,
                lider.titulo,
                lider.data_inicio,
                lider.bancada_nome,
            ): lider
            for lider in lider_data
        }.values()
    )

    insert_camara_legislaturas_lideres_db(data=lider_data)

    return


def get_id_bancada(uri: str) -> int:
    """
    Como nesse endpoint são retornados tanto as URIs de partidos quanto para outros tipos de "bancadas", precisamos de uma função mais genérica
    """
    return int(uri.split("/")[-1])
