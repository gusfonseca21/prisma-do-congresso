import pandas as pd

from config.parameters import ExtractOutDir
from utils.io import load_ndjson


def start():
    jsons = load_ndjson(ExtractOutDir.CAMARA.MEMBROS_ORGAOS)

    lista_membros = []
    for orgao in jsons:
        href = orgao.get("links", [])[0].get("href")
        for membro in orgao.get("dados", []):
            membro["orgao"] = href
            lista_membros.append(membro)

    df = pd.DataFrame(lista_membros)
    duplicates = df[
        df.duplicated(subset=["id", "orgao", "titulo", "dataInicio"], keep=False)
    ]

    print(f"DF DESDUPLICADO {len(df)}")
    print(f"DUPLCADOS {len(duplicates)}")

    print(duplicates)

    df.to_csv("duplicates.csv", index=False)


if __name__ == "__main__":
    start()
