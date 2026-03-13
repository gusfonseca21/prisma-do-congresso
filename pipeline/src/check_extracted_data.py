import asyncio

import pandas as pd

from config.parameters import ExtractOutDir
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import load_ndjson


async def start():
    jsons_orgaos = load_ndjson(ExtractOutDir.CAMARA.ORGAOS)
    jsons_eventos = load_ndjson(ExtractOutDir.CAMARA.EVENTOS)

    lista_orgaos = []
    lista_eventos = []
    for item in jsons_orgaos:
        for orgao in item.get("dados", []):
            lista_orgaos.append(orgao)

    for item in jsons_eventos:
        for evento in item.get("dados", []):
            lista_eventos.append(evento)

    df_orgaos = pd.json_normalize(lista_orgaos)
    df_eventos = pd.json_normalize(lista_eventos)

    df_eventos_orgaos = df_eventos.explode("orgaos")
    df_eventos_orgaos["id_orgao"] = df_eventos_orgaos["orgaos"].apply(
        lambda x: x.get("id") if isinstance(x, dict) else None
    )
    ids_missing = (
        df_eventos_orgaos.loc[
            ~df_eventos_orgaos["id_orgao"].isin(df_orgaos["id"]), "id_orgao"
        ]
        .dropna()
        .unique()
    )

    print(ids_missing)


if __name__ == "__main__":
    asyncio.run(start())

    # jsons = await fetch_many_jsons(
    #     urls=[
    #         "https://dadosabertos.camara.leg.br/api/v2/orgaos?itens=100&ordem=ASC&ordenarPor=id"
    #     ],
    #     not_downloaded_urls=[],
    #     task="",
    #     id_lote=1,
    #     follow_pagination=True,
    # )
