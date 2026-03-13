from datetime import datetime
from typing import Any

import pandas as pd
from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_eventos import (
    CamaraEventosArg,
    CamaraEventosOrgaosArg,
)
from database.repository.camara.repository_camara_eventos import (
    insert_camara_eventos_db,
    insert_camara_eventos_orgaos_db,
)

APP_SETTINGS = load_config()


def remove_duplicates(
    data_evento: list[CamaraEventosArg],
    data_evento_orgaos: list[CamaraEventosOrgaosArg],
) -> tuple[list[CamaraEventosArg], list[CamaraEventosOrgaosArg]]:
    df_evento = pd.DataFrame([vars(x) for x in data_evento])
    df_evento_orgaos = pd.DataFrame([vars(x) for x in data_evento_orgaos])

    ### APARENTEMENTE O ERRO PARA O LOAD ESTA AQUI, VALORES DE COLUNAS DE NONE PARA NAN AO GERAR O DATAFRAME

    df_evento = df_evento.drop_duplicates(subset=["id_evento"])
    df_evento_orgaos = df_evento_orgaos.drop_duplicates(
        subset=["id_evento", "id_orgao"]
    )

    data_evento = [CamaraEventosArg(**row) for row in df_evento.to_dict("records")]
    data_evento_orgaos = [
        CamaraEventosOrgaosArg(**row) for row in df_evento_orgaos.to_dict("records")
    ]

    return data_evento, data_evento_orgaos


@task(
    task_run_name=TasksNames.CAMARA.LOAD.EVENTOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_eventos(
    id_lote: int, eventos: list[dict] | None, ignore_tasks: list[str], _load_orgaos: Any
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.EVENTOS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.EVENTOS} foi ignorada")
        return
    if eventos is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.EVENTOS}' pois o argumento do parâmetro 'eventos' é nulo"
        )
        return

    logger.info("Carregando Eventos no Banco de Dados")

    data_evento: list[CamaraEventosArg] = []
    data_evento_orgaos: list[CamaraEventosOrgaosArg] = []

    for item in eventos:
        d = item.get("dados", [])
        for evento in d:
            local_camara = evento.get("localCamara")

            data_hora_inicio = evento.get("dataHoraInicio")
            data_hora_fim = evento.get("dataHoraFim")
            local_externo = evento.get("localExterno")

            data_evento.append(
                CamaraEventosArg(
                    id_lote=id_lote,
                    id_evento=evento.get("id"),
                    data_hora_inicio=datetime.fromisoformat(data_hora_inicio),
                    data_hora_fim=datetime.fromisoformat(data_hora_fim)
                    if data_hora_fim
                    else None,
                    situacao=evento.get("situacao"),
                    descricao_tipo=evento.get("descricaoTipo"),
                    descricao=evento.get("descricao"),
                    local_externo=str(local_externo)
                    if local_externo is not None
                    else None,
                    local_nome=local_camara.get("nome"),
                    url_registro=evento.get("urlRegistro"),
                )
            )

            orgaos = evento.get("orgaos")
            for orgao in orgaos:
                data_evento_orgaos.append(
                    CamaraEventosOrgaosArg(
                        id_lote=id_lote,
                        id_evento=evento.get("id"),
                        id_orgao=orgao.get("id"),
                    )
                )

    print(len(data_evento_orgaos))

    ## Removendo registros duplicados
    data_evento = list({(evento.id_evento): evento for evento in data_evento}.values())
    data_evento_orgaos = list(
        {
            (evento_orgao.id_evento, evento_orgao.id_orgao): evento_orgao
            for evento_orgao in data_evento_orgaos
        }.values()
    )

    if not data_evento:
        logger.warning(
            f"A lista de dados de EVENTOS a serem inseridos no banco de dados na task {TasksNames.CAMARA.LOAD.EVENTOS} está vazia. A função de inserção será ignorada."
        )
    if not data_evento_orgaos:
        logger.warning(
            f"A lista de dados de EVENTOS ÓRGÃOS a serem inseridos no banco de dados na task {TasksNames.CAMARA.LOAD.EVENTOS} está vazia. A função de inserção será ignorada."
        )
    else:
        insert_camara_eventos_db(data_evento)
        insert_camara_eventos_orgaos_db(data_evento_orgaos)

    return
