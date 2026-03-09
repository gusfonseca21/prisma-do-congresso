from enum import Enum

from config.loader import load_config

APP_SETTINGS = load_config()


class FlowsNames(str, Enum):
    PIPELINE = "pipeline"
    TSE = "tse"
    CAMARA = "camara"
    SENADO = "senado"


class TasksNames:
    class TSE:
        class EXTRACT:
            CANDIDATOS = "extract_tse_candidatos"
            PRESTACAO_CONTAS = "extract_tse_prestacao_contas"
            REDES_SOCIAIS = "extract_tse_redes_sociais"
            VOTACAO = "extract_tse_votacao"

    class CAMARA:
        class EXTRACT:
            LEGISLATURA = "extract_camara_legislatura"
            PARTIDOS = "extract_camara_partidos"
            DETALHES_PARTIDOS = "extract_camara_detalhes_partidos"
            DEPUTADOS = "extract_camara_deputados"
            DETALHES_DEPUTADOS = "extract_camara_detalhes_deputados"
            HISTORICO_DEPUTADOS = "extract_camara_historico_deputados"
            MANDATOS_EXTERNOS_DEPUTADOS = "extract_camara_mandatos_externos_deputados"
            OCUPACOES_DEPUTADOS = "extract_camara_ocupacoes_deputados"
            PROFISSOES_DEPUTADOS = "extract_camara_profissoes_deputados"
            LEGISLATURAS_LIDERES = "extract_camara_legislaturas_lideres"
            LEGISLATURAS_MESA = "extract_camara_legislaturas_mesa"
            BLOCOS = "extract_camara_blocos"
            PARTIDOS_BLOCOS = "extract_camara_partidos_blocos"
            TIPOS_ORGAOS = "extract_tipos_orgaos"
            ORGAOS = "extract_camara_orgaos"
            DETALHES_ORGAOS = "extract_camara_detalhes_orgaos"
            EVENTOS = "extract_camara_eventos"
            ASSIDUIDADE_PLENARIO = "extract_camara_assiduidade_plenario"
            ASSIDUIDADE_COMISSOES = "extract_camara_assiduidade_comissoes"
            FRENTES = "extract_camara_frentes"
            DETALHES_FRENTES = "extract_camara_detalhes_frentes"
            FRENTES_MEMBROS = "extract_camara_frentes_membros"
            DISCURSOS_DEPUTADOS = "extract_camara_discursos_deputados"
            PROPOSICOES = "extract_camara_proposicoes"
            DETALHES_PROPOSICOES = "extract_camara_detalhes_proposicoes"
            AUTORES_PROPOSICOES = "extract_camara_autores_proposicoes"
            DESPESAS_DEPUTADOS = "extract_camara_despesas_deputados"
            VOTACOES = "extract_camara_votacoes"
            DETALHES_VOTACOES = "extract_camara_detalhes_votacoes"
            ORIENTACOES_VOTACOES = "extract_camara_orientacoes_votacoes"
            VOTOS_VOTACOES = "extract_camara_votos_votacoes"

        class LOAD:
            LEGISLATURA = "load_camara_legislatura"
            PARTIDOS = "load_camara_partidos"
            DEPUTADOS = "load_camara_deputados"
            HISTORICO_DEPUTADOS = "load_camara_historico_deputados"
            MANDATOS_EXTERNOS_DEPUTADOS = "load_camara_mantados_externos_deputados"
            OCUPACOES_DEPUTADOS = "load_camara_ocupacoes_deputados"
            PROFISSOES_DEPUTADOS = "load_camara_profissoes_deputados"
            LEGISLATURAS_LIDERES = "load_camara_legislaturas_lideres"
            LEGISLATURAS_MESA = "load_camara_legislaturas_mesa"
            BLOCOS = "load_camara_blocos"
            PARTIDOS_BLOCOS = "load_camara_partidos_blocos"
            TIPOS_ORGAOS = "load_camara_tipos_orgaos"
            ORGAOS = "load_camara_orgaos"
            DETALHES_ORGAOS = "load_camara_detalhes_orgaos"

    class SENADO:
        class EXTRACT:
            COLEGIADOS = "extract_senado_colegiados"
            SENADORES = "extract_senado_senadores"
            DETALHES_SENADORES = "extract_senado_detalhes_senadores"
            DISCURSOS_SENADORES = "extract_senado_discursos_senadores"
            DESPESAS_SENADORES = "extract_senado_despesas_senadores"
            PROCESSOS = "extract_senado_processos"
            DETALHES_PROCESSOS = "extract_senado_detalhes_processos"
            VOTACOES = "extract_senado_votacoes"


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
        LEGISLATURAS_LIDERES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/legislaturas_lideres.ndjson"
        )
        LEGISLATURAS_MESA = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/legislaturas_mesa.json"
        )
        BLOCOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/blocos.ndjson"
        PARTIDOS_BLOCOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/partidos_blocos.ndjson"
        )
        EVENTOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/eventos.ndjson"
        TIPOS_ORGAOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/tipos_orgaos.json"
        ORGAOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/orgaos.ndjson"
        DETALHES_ORGAOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/detalhes_orgaos.ndjson"
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
