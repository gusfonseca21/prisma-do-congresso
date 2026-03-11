from datetime import date, datetime
from typing import Literal

from pydantic.main import BaseModel

LegislaturaKeys = Literal["id", "dataInicio", "dataFim"]


class LegislaturaReturn(BaseModel):
    id: int
    dataInicio: date
    dataFim: date


def get_current_legislatura(legislaturas: dict) -> LegislaturaReturn:
    """
    Retorna informações sobre a Legislatura Atual.
    Recebe o dict extraído do endpoint de Legislaturas.
    """
    current_date = date.today()
    dados = legislaturas.get("dados")
    if not dados:
        raise ValueError("Não foram econtrados dados no arquivo de Legislaturas")
    for leg in dados:
        l_start_date = date.fromisoformat(leg["dataInicio"])
        l_end_date = date.fromisoformat(leg["dataFim"])
        if l_start_date <= current_date and l_end_date >= current_date:
            return LegislaturaReturn(
                id=leg["id"],
                dataInicio=l_start_date,
                dataFim=l_end_date,
            )
    raise ValueError(f"Nenhuma Legislatura foi encontrada para a data '{current_date}'")


def get_legislatura_data(
    legislatura_dict: dict, property: LegislaturaKeys
) -> int | date:
    """
    Extrai e converte dados de uma propriedade específica relacionadas ao objeto Legislatura (CÂMARA).
    """
    # Verificando se existe a chave 'dados' dentro do objeto Legislatura
    leg_data = legislatura_dict.get("dados", [])
    if len(leg_data) < 1:
        raise ValueError(
            f"Não foram encontrados dados sobre Legislatura no objeto passado: {leg_data}"
        )

    prop_data = leg_data[0].get(property, None)
    if prop_data is None:
        raise ValueError(
            f"A propriedade '{property}' não existe dentro de Legislatura. Propriedades disponíveis: {prop_data}"
        )

    if property == "id":
        try:
            return int(prop_data)
        except ValueError:
            raise ValueError(
                f"O valor de '{property}' ('{prop_data}') não é conversível para um número inteiro."
            )
    else:
        date_format = "%Y-%m-%d"
        try:
            return datetime.strptime(str(prop_data), date_format).date()
        except ValueError:
            raise ValueError(
                f"O valor de '{property}' ('{prop_data}') não é conversível para um objeto de data."
            )
