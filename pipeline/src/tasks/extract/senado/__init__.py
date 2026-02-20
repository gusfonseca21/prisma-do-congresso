from .extract_colegiados_senado import extract_colegiados
from .extract_senado_despesas_senadores import extract_despesas_senado
from .extract_senado_detalhes_processos import extract_detalhes_processos_senado
from .extract_senado_detalhes_senadores import extract_detalhes_senadores_senado
from .extract_senado_discursos_senadores import extract_discursos_senado
from .extract_senado_processos import extract_processos_senado
from .extract_senado_senadores import extract_senadores_senado
from .extract_senado_votacoes import extract_votacoes_senado

__all__ = [
    "extract_colegiados",
    "extract_senadores_senado",
    "extract_detalhes_senadores_senado",
    "extract_discursos_senado",
    "extract_despesas_senado",
    "extract_processos_senado",
    "extract_detalhes_processos_senado",
    "extract_votacoes_senado",
]
