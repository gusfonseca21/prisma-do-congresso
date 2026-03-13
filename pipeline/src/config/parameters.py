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
            LEGISLATURAS = "extract_camara_legislaturas"
            PARTIDOS = "extract_camara_partidos"
            PARTIDOS_DETALHES = "extract_camara_partidos_detalhes"
            DEPUTADOS = "extract_camara_deputados"
            DEPUTADOS_DETALHES = "extract_camara_deputados_detalhes"
            DEPUTADOS_HISTORICO = "extract_camara_deputados_historico"
            DEPUTADOS_MANDATOS_EXTERNOS = "extract_camara_deputados_mandatos_externos"
            DEPUTADOS_OCUPACOES = "extract_camara_deputados_ocupacoes"
            DEPUTADOS_PROFISSOES = "extract_camara_deputados_profissoes"
            LEGISLATURAS_LIDERES = "extract_camara_legislaturas_lideres"
            LEGISLATURAS_MESA = "extract_camara_legislaturas_mesa"
            BLOCOS = "extract_camara_blocos"
            BLOCOS_PARTIDOS = "extract_camara_blocos_partidos"
            ORGAOS_TIPOS = "extract_camara_orgaos_tipos"
            ORGAOS = "extract_camara_orgaos"
            ORGAOS_DETALHES = "extract_camara_orgaos_detalhes"
            ORGAOS_MEMBROS = "extract_camara_orgaos_membros"
            EVENTOS = "extract_camara_eventos"
            DEPUTADOS_ASSIDUIDADE_PLENARIO = (
                "extract_camara_deputados_assiduidade_plenario"
            )
            DEPUTADOS_ASSIDUIDADE_COMISSOES = (
                "extract_camara_deputados_assiduidade_comissoes"
            )
            FRENTES = "extract_camara_frentes"
            FRENTES_DETALHES = "extract_camara_frentes_detalhes"
            FRENTES_MEMBROS = "extract_camara_frentes_membros"
            DEPUTADOS_DISCURSOS = "extract_camara_deputados_discursos"
            PROPOSICOES = "extract_camara_proposicoes"
            PROPOSICOES_DETALHES = "extract_camara_proposicoes_detalhes"
            PROPOSICOES_AUTORES = "extract_camara_proposicoes_autores"
            DEPUTADOS_DESPESAS = "extract_camara_deputados_despesas"
            VOTACOES = "extract_camara_votacoes"
            VOTACOES_DETALHES = "extract_camara_votacoes_detalhes"
            VOTACOES_ORIENTACOES = "extract_camara_votacoes_orientacoes"
            VOTACOES_VOTOS = "extract_camara_votacoes_votos"

        class LOAD:
            LEGISLATURAS = "load_camara_legislaturas"
            PARTIDOS = "load_camara_partidos"
            DEPUTADOS = "load_camara_deputados"
            DEPUTADOS_HISTORICO = "load_camara_deputados_historico"
            DEPUTADOS_MANDATOS_EXTERNOS = "load_camara_deputados_mandatos_externos"
            DEPUTADOS_OCUPACOES = "load_camara_deputados_ocupacoes"
            DEPUTADOS_PROFISSOES = "load_camara_deputados_profissoes"
            LEGISLATURAS_LIDERES = "load_camara_legislaturas_lideres"
            LEGISLATURAS_MESA = "load_camara_legislaturas_mesa"
            BLOCOS = "load_camara_blocos"
            BLOCOS_PARTIDOS = "load_camara_blocos_partidos"
            ORGAOS_TIPOS = "load_camara_orgaos_tipos"
            ORGAOS = "load_camara_orgaos"
            ORGAOS_MEMBROS = "load_camara_orgaos_membros"

    class SENADO:
        class EXTRACT:
            COLEGIADOS = "extract_senado_colegiados"
            SENADORES = "extract_senado_senadores"
            SENADORES_DETALHES = "extract_senado_senadores_detalhes"
            SENADORES_DISCURSOS = "extract_senado_senadores_discursos"
            SENADORES_DESPESAS = "extract_senado_despesas_senadores"
            PROCESSOS = "extract_senado_processos"
            PROCESSOS_DETALHES = "extract_senado_processos_detalhes"
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
        LEGISLATURAS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/legislaturas.json"
        PARTIDOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/partidos.ndjson"
        PARTIDOS_DETALHES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/partidos_detalhes.ndjson"
        )
        DEPUTADOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados.json"
        DEPUTADOS_DETALHES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_detalhes.ndjson"
        )
        DEPUTADOS_HISTORICO = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_historico.ndjson"
        )
        DEPUTADOS_MANDATOS_EXTERNOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_mandatos_externos.ndjson"
        DEPUTADOS_OCUPACOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_ocupacoes.ndjson"
        )
        DEPUTADOS_PROFISSOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_profissoes.ndjson"
        )
        LEGISLATURAS_LIDERES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/legislaturas_lideres.ndjson"
        )
        LEGISLATURAS_MESA = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/legislaturas_mesa.json"
        )
        BLOCOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/blocos.ndjson"
        BLOCOS_PARTIDOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/blocos_partidos.ndjson"
        )
        EVENTOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/eventos.ndjson"
        ORGAOS_TIPOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/orgaos_tipos.json"
        ORGAOS = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/orgaos.ndjson"
        ORGAOS_DETALHES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/orgaos_detalhes.ndjson"
        )
        ORGAOS_MEMBROS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/orgaos_membros.ndjson"
        )
        DEPUTADOS_ASSIDUIDADE_PLENARIO = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_assiduidade_plenario.zip"
        DEPUTADOS_ASSIDUIDADE_COMISSOES = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_assiduidade_comissoes.zip"
        FRENTES = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/frentes.ndjson"
        FRENTES_MEMBROS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/frentes_membros.ndjson"
        )
        FRENTES_DETALHES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/frentes_detalhes.ndjson"
        )
        DEPUTADOS_DISCURSOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_discursos.ndjson"
        )
        PROPOSICOES = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/proposicoes.ndjson"
        PROPOSICOES_DETALHES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/proposicoes_detalhes.ndjson"
        )
        PROPOSICOES_AUTORES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/proposicoes_autores.ndjson"
        )
        VOTACOES = f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/votacoes.ndjson"
        VOTACOES_DETALHES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/votacoes_detalhes.ndjson"
        )
        VOTACOES_ORIENTACOES = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/votacoes_orientacoes.ndjson"
        )
        VOTACOES_VOTOS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/votacoes_votos.ndjson"
        )
        DEPUTADOS_DESPESAS = (
            f"{APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR}/deputados_despesas.ndjson"
        )

    class SENADO:
        COLEGIADOS = f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/colegiados.json"
        SENADORES_EXERCICIO = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/senadores_exercicio.json"
        )
        SENADORES_AFASTADOS = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/senadores_afastados.json"
        )
        SENADORES_DETALHES = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/senadores_detalhes.ndjson"
        )
        SENADORES_DISCURSOS = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/senadores_discursos.ndjson"
        )
        SENADORES_DESPESAS = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/senadores_despesas.ndjson"
        )
        PROCESSOS = f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/processos.json"
        PROCESSOS_DETALHES = (
            f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/processos_detalhes.ndjson"
        )
        VOTACOES = f"{APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR}/votacoes.ndjson"
