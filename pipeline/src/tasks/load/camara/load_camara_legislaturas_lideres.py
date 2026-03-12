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


def get_id_bancada(uri: str) -> int:
    """
    Como nesse endpoint são retornados tanto as URIs de partidos quanto para outros tipos de "bancadas", precisamos de uma função mais genérica
    """
    return int(uri.split("/")[-1])


@task(
    task_run_name=TasksNames.CAMARA.LOAD.LEGISLATURAS_LIDERES,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_legislaturas_lideres(
    lideres: list[dict],
    id_lote: int,
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

    data: list[CamaraLegislaturasLideresArg] = []

    for item in lideres:
        for lider in item.get("dados", []):
            parlamentar = lider.get("parlamentar")
            bancada = lider.get("bancada")
            bancada_uri = bancada.get("uri")

            id_bancada = None
            if bancada_uri:
                id_bancada = get_id_bancada(bancada_uri)

            data.append(
                CamaraLegislaturasLideresArg(
                    id_lote=id_lote,
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
    data = list(
        {
            (
                lider.id_deputado,
                lider.titulo,
                lider.data_inicio,
                lider.bancada_nome,
            ): lider
            for lider in data
        }.values()
    )

    if data:
        insert_camara_legislaturas_lideres_db(data)
    else:
        logger.warning(
            f"A lista de dados a serem inseridos no banco de dados na task {TasksNames.CAMARA.LOAD.LEGISLATURAS_LIDERES} está vazia. A função de inserção será ignorada."
        )

    return
