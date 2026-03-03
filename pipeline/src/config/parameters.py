from enum import Enum

from config.loader import load_config

APP_SETTINGS = load_config()


class FlowsNames(str, Enum):
    PIPELINE = "pipeline"
    TSE = "tse"
    CAMARA = "camara"
    SENADO = "senado"


class TasksNames:
    # TSE
    EXTRACT_TSE_CANDIDATOS = "extract_tse_candidatos"
    EXTRACT_TSE_PRESTACAO_CONTAS = "extract_tse_prestacao_contas"
    EXTRACT_TSE_REDES_SOCIAIS = "extract_tse_redes_sociais"
    EXTRACT_TSE_VOTACAO = "extract_tse_votacao"
    # CAMARA
    ## EXTRACT
    EXTRACT_CAMARA_LEGISLATURA = "extract_camara_legislatura"
    EXTRACT_CAMARA_PARTIDOS = "extract_camara_partidos"
    EXTRACT_CAMARA_DETALHES_PARTIDOS = "extract_camara_detalhes_partidos"
    EXTRACT_CAMARA_DEPUTADOS = "extract_camara_deputados"
    EXTRACT_CAMARA_DETALHES_DEPUTADOS = "extract_camara_detalhes_deputados"
    EXTRACT_CAMARA_HISTORICO_DEPUTADOS = "extract_camara_historico_deputados"
    EXTRACT_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS = (
        "extract_camara_mandatos_externos_deputados"
    )
    EXTRACT_CAMARA_OCUPACOES_DEPUTADOS = "extract_camara_ocupacoes_deputados"
    EXTRACT_CAMARA_PROFISSOES_DEPUTADOS = "extract_camara_profissoes_deputados"
    EXTRACT_CAMARA_ASSIDUIDADE_PLENARIO = "extract_camara_assiduidade_plenario"
    EXTRACT_CAMARA_ASSIDUIDADE_COMISSOES = "extract_camara_assiduidade_comissoes"
    EXTRACT_CAMARA_FRENTES = "extract_camara_frentes"
    EXTRACT_CAMARA_DETALHES_FRENTES = "extract_camara_detalhes_frentes"
    EXTRACT_CAMARA_FRENTES_MEMBROS = "extract_camara_frentes_membros"
    EXTRACT_CAMARA_DISCURSOS_DEPUTADOS = "extract_camara_discursos_deputados"
    EXTRACT_CAMARA_PROPOSICOES = "extract_camara_proposicoes"
    EXTRACT_CAMARA_DETALHES_PROPOSICOES = "extract_camara_detalhes_proposicoes"
    EXTRACT_CAMARA_AUTORES_PROPOSICOES = "extract_camara_autores_proposicoes"
    EXTRACT_CAMARA_DESPESAS_DEPUTADOS = "extract_camara_despesas_deputados"
    EXTRACT_CAMARA_VOTACOES = "extract_camara_votacoes"
    EXTRACT_CAMARA_DETALHES_VOTACOES = "extract_camara_detalhes_votacoes"
    EXTRACT_CAMARA_ORIENTACOES_VOTACOES = "extract_camara_orientacoes_votacoes"
    EXTRACT_CAMARA_VOTOS_VOTACOES = "extract_camara_votos_votacoes"
    EXTRACT_CAMARA_LEGISLATURAS_LIDERES = "extract_camara_legislaturas_lideres"
    EXTRACT_CAMARA_LEGISLATURAS_MESA = "extract_camara_legislaturas_mesa"
    ## LOAD
    LOAD_CAMARA_LEGISLATURA = "load_camara_legislatura"
    LOAD_CAMARA_PARTIDOS = "load_camara_partidos"
    LOAD_CAMARA_DEPUTADOS = "load_camara_deputados"
    LOAD_CAMARA_HISTORICO_DEPUTADOS = "load_camara_historico_deputados"
    LOAD_CAMARA_MANDATOS_EXTERNOS_DEPUTADOS = "load_camara_mantados_externos_deputados"
    LOAD_CAMARA_OCUPACOES_DEPUTADOS = "load_camara_ocupacoes_deputados"
    LOAD_CAMARA_PROFISSOES_DEPUTADOS = "load_camara_profissoes_deputados"
    # SENADO
    EXTRACT_SENADO_COLEGIADOS = "extract_senado_colegiados"
    EXTRACT_SENADO_SENADORES = "extract_senado_senadores"
    EXTRACT_SENADO_DETALHES_SENADORES = "extract_senado_detalhes_senadores"
    EXTRACT_SENADO_DISCURSOS_SENADORES = "extract_senado_discursos_senadores"
    EXTRACT_SENADO_DESPESAS_SENADORES = "extract_senado_despesas_senadores"
    EXTRACT_SENADO_PROCESSOS = "extract_senado_processos"
    EXTRACT_SENADO_DETALHES_PROCESSOS = "extract_senado_detalhes_processos"
    EXTRACT_SENADO_VOTACOES = "extract_senado_votacoes"


class ExtractOutDir:
    class TSE:
        # Esses endpoints serão baixados em zip e depois extraídos na localização aqui definida
        CANDIDATOS = (
            f"{APP_SETTINGS.TSE.OUTPUT_EXTRACT_DIR}/candidatos"  # COMPLEMENTO: /year
        )
        PRESTACAO_CONTAS = f"{APP_SETTINGS.TSE.OUTPUT_EXTRACT_DIR}/prestacao_contas"  # COMPLEMENTO: /year
        REDES_SOCIAIS = (
            f"{APP_SETTINGS.TSE.OUTPUT_EXTRACT_DIR}/redes_sociais"  # COMPLEMENTO: /year
        )
        VOTACOES = (
            f"{APP_SETTINGS.TSE.OUTPUT_EXTRACT_DIR}/votacoes"  # COMPLEMENTO: /year
        )

    class CAMARA:
        LEGISLATURA = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/legislatura.json"
        PARTIDOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/partidos.ndjson"
        DETALHES_PARTIDOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/detalhes_partidos.ndjson"
        )
        DEPUTADOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados.json"
        DETALHES_DEPUTADOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/detalhes_deputados.ndjson"
        )
        HISTORICO_DEPUTADOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/historico_deputados.ndjson"
        )
        MANDATOS_EXTERNOS_DEPUTADOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/mandatos_externos_deputados.ndjson"
        OCUPACOES_DEPUTADOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/ocupacoes_deputados.ndjson"
        )
        PROFISSOES_DEPUTADOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/profissoes_deputados.ndjson"
        )
        ASSIDUIDADE_PLENARIO = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/assiduidade_plenario.zip"
        )
        ASSIDUIDADE_COMISSOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/assiduidade_comissoes.zip"
        )
        FRENTES = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/frentes.ndjson"
        FRENTES_MEMBROS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/frentes_membros.ndjson"
        )
        DETALHES_FRENTES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/detalhes_frentes.ndjson"
        )
        DISCURSOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/discursos.ndjson"
        PROPOSICOES = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/proposicoes.ndjson"
        DETALHES_PROPOSICOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/detalhes_proposicoes.ndjson"
        )
        AUTORES_PROPOSICOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/autores_proposicoes.ndjson"
        )
        VOTACOES = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/votacoes.ndjson"
        DETALHES_VOTACOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/detalhes_votacoes.ndjson"
        )
        ORIENTACOES_VOTACOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/orientacoes_votacoes.ndjson"
        )
        VOTOS_VOTACOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/votos_votacoes.ndjson"
        )
        DESPESAS_DEPUTADOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/despesas_deputados.ndjson"
        )
        LEGISLATURAS_LIDERES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/legislaturas_lideres.ndjson"
        )
        LEGISLATURAS_MESA = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/legislaturas_mesa.json"
        )

    class SENADO:
        COLEGIADOS = f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/colegiados.json"
        SENADORES_EXERCICIO = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/senadores_exercicio.json"
        )
        SENADORES_AFASTADOS = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/senadores_afastados.json"
        )
        DETALHES_SENADORES = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/detalhes_senadores.ndjson"
        )
        DISCURSOS_SENADORES = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/discursos_senadores.ndjson"
        )
        DESPESAS_SENADORES = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/despesas_senadores.ndjson"
        )
        PROCESSOS = f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/processos.json"
        DETALHES_PROCESSOS = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/detalhes_processos.ndjson"
        )
        VOTACOES = f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/votacoes.ndjson"
