## IMPORTAÇÕES PARA QUE O ALEMBIC POSSA GERAR AUTOMATICAMENTE AS TABELAS

from .camara_blocos import (
    CamaraBlocos,
    CamaraBlocosArg,
    CamaraBlocosPartidos,
    CamaraBlocosPartidosArg,
)
from .camara_deputados import (
    CamaraDeputados,
    CamaraDeputadosArg,
    CamaraDeputadosMandatosExternos,
    CamaraDeputadosMandatosExternosArg,
    CamaraDeputadosOcupacoes,
    CamaraDeputadosProfissoes,
    CamaraDeputadosRedesSociais,
    CamaraDeputadosRedesSociaisArg,
)
from .camara_eventos import (
    CamaraEventos,
    CamaraEventosArg,
    CamaraEventosOrgaos,
    CamaraEventosOrgaosArg,
)
from .camara_legislaturas import CamaraLegislaturas, CamaraLegislaturasArg
from .camara_orgaos import (
    CamaraOrgaos,
    CamaraOrgaosArg,
    CamaraOrgaosMembros,
    CamaraOrgaosMembrosArg,
    CamaraOrgaosTipos,
    CamaraOrgaosTiposArg,
)
from .camara_partidos import CamaraPartidos, CamaraPartidosArg

__all__ = [
    "CamaraLegislaturasArg",
    "CamaraLegislaturas",
    "CamaraDeputados",
    "CamaraDeputadosArg",
    "CamaraDeputadosRedesSociais",
    "CamaraDeputadosRedesSociaisArg",
    "CamaraPartidos",
    "CamaraPartidosArg",
    "CamaraDeputadosMandatosExternos",
    "CamaraDeputadosMandatosExternosArg",
    "CamaraDeputadosOcupacoes",
    "CamaraDeputadosProfissoes",
    "CamaraBlocos",
    "CamaraBlocosArg",
    "CamaraBlocosPartidos",
    "CamaraBlocosPartidosArg",
    "CamaraOrgaosTipos",
    "CamaraOrgaosTiposArg",
    "CamaraOrgaos",
    "CamaraOrgaosArg",
    "CamaraOrgaosMembros",
    "CamaraOrgaosMembrosArg",
    "CamaraEventos",
    "CamaraEventosArg",
    "CamaraEventosOrgaos",
    "CamaraEventosOrgaosArg",
]
