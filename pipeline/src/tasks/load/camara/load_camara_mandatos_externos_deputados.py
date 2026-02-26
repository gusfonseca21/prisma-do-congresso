from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_deputados import (
    CamaraDeputadosMandatosExternosArg,
)
from database.repository.camara.repository_camara_deputados import (
    insert_camara_mandatos_externos_deputados,
)
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.LOAD_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def load_camara_mandatos_externos_deputados(
    lote_id: int,
    mandatos_externos: list[dict] | None,
):
    logger = get_run_logger()

    logger.info("Carregando Mandatos Externos de Deputados da Câmara no Banco de Dados")

    if mandatos_externos is None:
        raise ValueError(
            "Erro ao carregar dados de Mandatos Externos de Deputados no Banco de Dados: o parâmetro 'mandatos_externos' é Nulo"
        )

    mandatos_externos_data: list[CamaraDeputadosMandatosExternosArg] = []

    ## MANDATOS EXTERNOS
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

    insert_camara_mandatos_externos_deputados(
        mandatos_externos_data=mandatos_externos_data,
    )

    return
