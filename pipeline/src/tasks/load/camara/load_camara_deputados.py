import hashlib
from datetime import date, datetime

from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_deputados import (
    CamaraDeputadosArg,
    CamaraDeputadosHistoricoArg,
    CamaraDeputadosMandatosExternosArg,
    CamaraDeputadosRedesSociaisArg,
)
from database.repository.camara.repository_camara_deputados import (
    insert_camara_deputados,
)
from database.repository.camara.repository_camara_partidos import get_partidos_siglas
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.LOAD_CAMARA_DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_deputados(
    lote_id: int,
    deputados: list[dict] | None,
    historico_deputados: list[dict] | None,
    mandatos_externos: list[dict] | None,
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
    if mandatos_externos is None:
        raise ValueError(
            "Erro ao carregar dados de Deputados no Banco de Dados: o parâmetro 'mandatos_externos' é Nulo"
        )

    deputados_data: list[CamaraDeputadosArg] = []
    redes_sociais_data: list[CamaraDeputadosRedesSociaisArg] = []
    historico_deputados_data: list[CamaraDeputadosHistoricoArg] = []
    mandatos_externos_data: list[CamaraDeputadosMandatosExternosArg] = []

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
            logger.warning(map_partidos)
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

    for me_data in mandatos_externos:
        href = me_data.get("links", [])[0].get("href")
        id_deputado = get_path_parameter_value(href, "deputados", None)

        mandatos_dados = me_data.get("dados", [])
        for mandato in mandatos_dados:
            mandatos_externos_data.append(
                CamaraDeputadosMandatosExternosArg(
                    id_lote=lote_id,
                    id_deputado=id_deputado,
                    cargo=mandato.get("cargo"),
                    sigla_uf=mandato.get("siglaUf"),
                    municipio=mandato.get("municipio"),
                    ano_inicio=int(mandato.get("anoInicio")),
                    ano_fim=int(mandato.get("anoFim"))
                    if mandato.get("anoFim")
                    else None,
                    sigla_partido=mandato.get("siglaPartidoEleicao"),
                )
            )

    # Limpa registros duplicados
    mandatos_externos_data = list(
        {
            (mandato.id_deputado, mandato.cargo, mandato.ano_inicio): mandato
            for mandato in mandatos_externos_data
        }.values()
    )

    insert_camara_deputados(
        lote_id=lote_id,
        deputados_data=deputados_data,
        redes_sociais_data=redes_sociais_data,
        historico_deputados_data=historico_deputados_data,
        mandatos_externos_data=mandatos_externos_data,
    )

    return
