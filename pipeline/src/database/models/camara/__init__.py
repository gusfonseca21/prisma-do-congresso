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
from .camara_legislatura import CamaraLegislatura, CamaraLegislaturaArg
from .camara_orgaos import CamaraOrgaosTipos, CamaraOrgaosTiposArg
from .camara_partidos import CamaraPartidos, CamaraPartidosArg

__all__ = [
    "CamaraLegislaturaArg",
    "CamaraLegislatura",
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
]
