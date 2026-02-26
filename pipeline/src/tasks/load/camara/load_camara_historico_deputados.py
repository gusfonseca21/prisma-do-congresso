import hashlib
from datetime import datetime

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_deputados import (
    CamaraDeputadosHistoricoArg,
)
from database.repository.camara.repository_camara_deputados import (
    insert_camara_historico_deputados,
)

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.LOAD_CAMARA_HISTORICO_DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_historico_deputados(
    lote_id: int,
    historico_deputados: list[dict] | None,
):
    logger = get_run_logger()

    logger.info("Carregando Histórico de Deputados da Câmara no Banco de Dados")

    if historico_deputados is None:
        raise ValueError(
            "Erro ao carregar dados de Histórico de Deputados no Banco de Dados: o parâmetro 'historico_deputados' é Nulo"
        )

    historico_deputados_data: list[CamaraDeputadosHistoricoArg] = []

    for h_data in historico_deputados:
        historico_dados = h_data.get("dados", [])

        for historico in historico_dados:
            pre_hash_content = f"{historico.get('id')}|{historico.get('nome')}|{historico.get('nomeEleitoral')}|{historico.get('siglaPartido')}|{historico.get('siglaUf')}|{historico.get('idLegislatura')}|{historico.get('dataHora')}|{historico.get('situacao')}|{historico.get('condicaoEleitoral')}|{historico.get('descricaoStatus')}"

            hash = hashlib.md5(pre_hash_content.encode()).hexdigest()

            historico_deputados_data.append(
                CamaraDeputadosHistoricoArg(
                    id_lote=lote_id,
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

    insert_camara_historico_deputados(historico_deputados_data=historico_deputados_data)

    return
