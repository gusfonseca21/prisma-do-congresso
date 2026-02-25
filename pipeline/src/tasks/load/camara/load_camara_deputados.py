import hashlib
from datetime import date, datetime

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_deputados import (
    CamaraDeputadosArg,
    CamaraDeputadosHistoricoArg,
    CamaraDeputadosRedesSociaisArg,
)
from database.repository.camara.repository_camara_deputados import (
    insert_camara_deputados,
)
from database.repository.camara.repository_camara_partidos import get_partidos_siglas

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.LOAD_CAMARA_DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_deputados(
    lote_id: int,
    legislatura: dict,
    deputados: list[dict] | None,
    historico_deputados: list[dict] | None,
):
    logger = get_run_logger()

    logger.info("Carregando Deputados da Câmara no Banco de Dados")

    if deputados is None:
        raise ValueError(
            "Erro ao carregar dados de Deputados no Banco de Dados: o parâmetro 'deputados' é Nulo"
        )
    if historico_deputados is None:
        raise ValueError(
            "Erro ao carregar dados de Deputados no Banco de Dados: o parâmetro 'historico_deputados' é Nulo"
        )

    deputados_data: list[CamaraDeputadosArg] = []
    redes_sociais_data: list[CamaraDeputadosRedesSociaisArg] = []
    historico_deputados_data: list[CamaraDeputadosHistoricoArg] = []

    id_sigla_partidos = get_partidos_siglas()

    map_partidos: dict[str, int] = {p.sigla: p.id_partido for p in id_sigla_partidos}

    for data in deputados:
        dados = data.get("dados", {})
        ultimo_status = dados.get("ultimoStatus")
        gabinete = ultimo_status.get("gabinete")
        redes_sociais = dados.get("redeSocial", [])
        id_deputado = dados.get("id")
        data_falescimento = dados.get("dataFalecimento")

        id_partido = map_partidos.get(ultimo_status.get("siglaPartido"))
        if id_partido is None:
            raise ValueError(
                f"Erro ao inserir na tabela camara_deputados. O valor de id_partido é nulo para o Deputado de Id: {id_deputado}"
            )

        deputados_data.append(
            CamaraDeputadosArg(
                id_lote=lote_id,
                id_deputado=id_deputado,
                nome_civil=dados.get("nomeCivil"),
                nome=ultimo_status.get("nome"),
                id_partido=id_partido,
                sigla_uf=ultimo_status.get("siglaUf"),
                id_legislatura=ultimo_status.get("idLegislatura"),
                url_foto=ultimo_status.get("urlFoto"),
                email=gabinete.get("email"),
                data_ultimo_status=date.fromisoformat(ultimo_status.get("data")),
                nome_eleitoral=ultimo_status.get("nomeEleitoral"),
                gabinete_nome=gabinete.get("nome"),
                gabinete_predio=gabinete.get("predio"),
                gabinete_sala=gabinete.get("sala"),
                gabinete_andar=gabinete.get("andar"),
                gabinete_telefone=gabinete.get("telefone"),
                situacao=ultimo_status.get("situacao"),
                condicao_eleitoral=ultimo_status.get("condicaoEleitoral"),
                descricao_status=ultimo_status.get("descricaoStatus"),
                cpf=dados.get("cpf"),
                sexo=dados.get("sexo"),
                data_nascimento=date.fromisoformat(dados.get("dataNascimento")),
                data_falecimento=date.fromisoformat(data_falescimento)
                if data_falescimento
                else None,
                uf_nascimento=dados.get("ufNascimento"),
                municipio_nascimento=dados.get("municipioNascimento"),
                escolaridade=dados.get("escolaridade"),
            )
        )

        for url in redes_sociais:
            redes_sociais_data.append(
                CamaraDeputadosRedesSociaisArg(
                    id_lote=lote_id, id_deputado=id_deputado, url=url
                )
            )

    # O Endpoint de Histórico de Deputados trás dados de legislaturas anteriores. Iremos descartá-las.
    id_legislatura_atual = legislatura.get("dados", [])[0].get("id")

    for h_data in historico_deputados:
        historico_dados = h_data.get("dados", [])

        for historico in historico_dados:
            if historico.get("idLegislatura") != id_legislatura_atual:
                continue

            id_partido = map_partidos.get(historico.get("siglaPartido"))
            if id_partido is None:
                raise ValueError(
                    f"Erro ao inserir na tabela camara_deputados. O valor de id_partido é nulo para o Deputado de Id {historico.get('id')}"
                )

            pre_hash_content = f"{historico.get('id')}|{historico.get('nome')}|{historico.get('nomeEleitoral')}|{id_partido}|{historico.get('siglaUf')}|{historico.get('idLegislatura')}|{historico.get('dataHora')}|{historico.get('situacao')}|{historico.get('condicaoEleitoral')}|{historico.get('descricaoStatus')}"

            hash = hashlib.md5(pre_hash_content.encode()).hexdigest()

            historico_deputados_data.append(
                CamaraDeputadosHistoricoArg(
                    id_lote=lote_id,
                    id_deputado=historico.get("id"),
                    nome=historico.get("nome"),
                    nome_eleitoral=historico.get("nomeEleitoral"),
                    id_partido=id_partido,
                    sigla_uf=historico.get("siglaUf"),
                    id_legislatura=historico.get("idLegislatura"),
                    data_hora=datetime.fromisoformat(historico.get("dataHora")),
                    situacao=historico.get("situacao"),
                    condicao_eleitoral=historico.get("condicaoEleitoral"),
                    descricao_status=historico.get("descricaoStatus"),
                    hash=hash,
                )
            )

    insert_camara_deputados(
        lote_id=lote_id,
        deputados_data=deputados_data,
        redes_sociais_data=redes_sociais_data,
        historico_deputados_data=historico_deputados_data,
    )

    return
