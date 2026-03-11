import hashlib
from datetime import datetime
from typing import Any

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_deputados import (
    CamaraDeputadosHistoricoArg,
)
from database.repository.camara.repository_camara_deputados import (
    insert_camara_historico_deputados_db,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.CAMARA.LOAD.HISTORICO_DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_historico_deputados(
    id_lote: int,
    historico_deputados: list[dict] | None,
    ignore_tasks: list[str],
    _load_deputados: Any,
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.HISTORICO_DEPUTADOS in ignore_tasks:
        logger.warning(
            f"A Task {TasksNames.CAMARA.LOAD.HISTORICO_DEPUTADOS} foi ignorada"
        )
        return
    if historico_deputados is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.HISTORICO_DEPUTADOS}' pois o argumento do parâmetro 'historico_deputados' é nulo"
        )
        return

    logger.info("Carregando Histórico de Deputados da Câmara no Banco de Dados")

    historico_deputados_data: list[CamaraDeputadosHistoricoArg] = []

    for h_data in historico_deputados:
        historico_dados = h_data.get("dados", [])

        for historico in historico_dados:
            pre_hash_content = f"{historico.get('id')}|{historico.get('nome')}|{historico.get('nomeEleitoral')}|{historico.get('siglaPartido')}|{historico.get('siglaUf')}|{historico.get('idLegislatura')}|{historico.get('dataHora')}|{historico.get('situacao')}|{historico.get('condicaoEleitoral')}|{historico.get('descricaoStatus')}"

            hash = hashlib.md5(pre_hash_content.encode()).hexdigest()

            historico_deputados_data.append(
                CamaraDeputadosHistoricoArg(
                    id_lote=id_lote,
                    id_deputado=historico.get("id"),
                    nome=historico.get("nome"),
                    nome_eleitoral=historico.get("nomeEleitoral"),
                    sigla_partido=historico.get("siglaPartido"),
                    sigla_uf=historico.get("siglaUf"),
                    id_legislatura=historico.get("idLegislatura"),
                    data_hora=datetime.fromisoformat(historico.get("dataHora")),
                    situacao=historico.get("situacao"),
                    condicao_eleitoral=historico.get("condicaoEleitoral"),
                    descricao_status=historico.get("descricaoStatus"),
                    hash=hash,
                )
            )

    insert_camara_historico_deputados_db(
        historico_deputados_data=historico_deputados_data
    )

    return
