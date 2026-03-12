import pandas as pd

from config.parameters import ExtractOutDir
from utils.io import load_ndjson


def start():
    jsons = load_ndjson(ExtractOutDir.CAMARA.EVENTOS)

    lista_eventos = []
    for orgao in jsons:
        for evento in orgao.get("dados", []):
            lista_eventos.append(evento)

    df_eventos = pd.json_normalize(lista_eventos)
    df_eventos = pd.json_normalize(lista_eventos).drop(columns=["orgaos"])

    print(f"Total: {len(df_eventos)}")
    print(f"descricao: {len(df_eventos[df_eventos['descricao'].isna()])}")
    print(f"localExterno: {len(df_eventos[df_eventos['localExterno'].isna()])}")
    print(f"urlRegistro: {len(df_eventos[df_eventos['urlRegistro'].isna()])}")
    print(f"localCamara.nome: {len(df_eventos[df_eventos['localCamara.nome'].isna()])}")
    print(
        f"localCamara.predio: {len(df_eventos[df_eventos['localCamara.predio'].isna()])}"
    )
    print(f"localCamara.sala: {len(df_eventos[df_eventos['localCamara.sala'].isna()])}")
    print(
        f"localCamara.andar: {len(df_eventos[df_eventos['localCamara.andar'].isna()])}"
    )

    df_orgaos = pd.json_normalize(
        lista_eventos, record_path="orgaos", meta=["id"], meta_prefix="evento_"
    )
    print(f"Órgão - id: {len(df_orgaos[df_orgaos['id'].isna()])}")
    print(f"Órgão - sigla: {len(df_orgaos[df_orgaos['sigla'].isna()])}")
    print(f"Órgão - nome: {len(df_orgaos[df_orgaos['nome'].isna()])}")
    print(f"Órgão - apelido: {len(df_orgaos[df_orgaos['apelido'].isna()])}")
    print(f"Órgão - codTipoOrgao: {len(df_orgaos[df_orgaos['codTipoOrgao'].isna()])}")
    print(f"Órgão - tipoOrgao: {len(df_orgaos[df_orgaos['tipoOrgao'].isna()])}")
    print(
        f"Órgão - nomePublicacao: {len(df_orgaos[df_orgaos['nomePublicacao'].isna()])}"
    )
    print(f"Órgão - nomeResumido: {len(df_orgaos[df_orgaos['nomeResumido'].isna()])}")
    print(f"Órgão - evento_id: {len(df_orgaos[df_orgaos['evento_id'].isna()])}")


if __name__ == "__main__":
    start()
