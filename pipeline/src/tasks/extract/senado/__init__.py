from .extract_senado_colegiados import extract_senado_colegiados
from .extract_senado_processos import extract_senado_processos
from .extract_senado_processos_detalhes import extract_senado_processos_detalhes
from .extract_senado_senadores import extract_senado_senadores
from .extract_senado_senadores_despesas import extract_senado_senadores_despesas
from .extract_senado_senadores_detalhes import extract_senado_senadores_detalhes
from .extract_senado_senadores_discursos import extract_senado_senadores_discursos
from .extract_senado_votacoes import extract_senado_votacoes

__all__ = [
    "extract_senado_colegiados",
    "extract_senado_senadores",
    "extract_senado_senadores_detalhes",
    "extract_senado_senadores_discursos",
    "extract_senado_senadores_despesas",
    "extract_senado_processos",
    "extract_senado_processos_detalhes",
    "extract_senado_votacoes",
]
