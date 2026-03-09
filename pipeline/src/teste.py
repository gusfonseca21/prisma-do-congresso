import pandas as pd

from config.parameters import ExtractOutDir
from utils.io import load_ndjson


def start():
    jsons = load_ndjson(ExtractOutDir.CAMARA.DETALHES_ORGAOS)

    data = []
    for orgao in jsons:
        data.append(orgao.get("dados", []))

    df = pd.DataFrame(data)

    print(df[df["dataInicio"].isna()])
    # print(df)


if __name__ == "__main__":
    start()
