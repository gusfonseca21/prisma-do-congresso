from datetime import date
from typing import Any

import pandas as pd
from prefect import get_run_logger, task

from config.loader import load_config
from config.parameters import TasksNames
from database.models.camara.camara_orgaos import CamaraOrgaosMembrosArg
from database.repository.camara.repository_camara_orgaos import (
    insert_camara_membros_orgaos_db,
)
from utils.camara import get_current_legislatura
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


def deduplicate_membros(
    data: list[CamaraOrgaosMembrosArg], legislaturas: dict
) -> list[CamaraOrgaosMembrosArg]:
    df = pd.DataFrame([item.__dict__ for item in data])

    # Por algum motivo, na Leg 57 retorna esse Deputado da legislatura 54
    ID_LEGISLATURA_ATUAL = get_current_legislatura(legislaturas).id

    df_sorted = df.sort_values("data_fim", na_position="last")  # None vai pro final
    df_sorted = pd.DataFrame(
        df_sorted[df_sorted["id_legislatura"] == ID_LEGISLATURA_ATUAL]
    )  # Filtrar o id de dep de leg. anterior
    df_dedup = df_sorted.drop_duplicates(
        subset=["id_orgao", "id_deputado", "titulo", "data_inicio"],
        keep="first",  # mantém o primeiro = o que tem data_fim preenchida
    )

    return [CamaraOrgaosMembrosArg(**row) for row in df_dedup.to_dict(orient="records")]


@task(
    task_run_name=TasksNames.CAMARA.LOAD.ORGAOS_MEMBROS,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
)
def load_camara_orgaos_membros(
    id_lote: int,
    membros_orgaos: dict | None,
    legislaturas: dict,
    ignore_tasks: list[str],
    _load_orgaos: Any,
    _load_deputados: Any,
):
    logger = get_run_logger()

    if TasksNames.CAMARA.LOAD.ORGAOS_MEMBROS in ignore_tasks:
        logger.warning(f"A Task {TasksNames.CAMARA.LOAD.ORGAOS_MEMBROS} foi ignorada")
        return
    if membros_orgaos is None:
        logger.warning(
            f"Não foi possível executar a task '{TasksNames.CAMARA.LOAD.ORGAOS_MEMBROS}' pois o argumento do parâmetro 'membros_orgaos' é nulo"
        )
        return

    logger.info("Carregando Membros de Órgãos no Banco de Dados")

    data: list[CamaraOrgaosMembrosArg] = []

    for orgao in membros_orgaos:
        href = orgao.get("links", [])[0].get("href")
        id_orgao = get_path_parameter_value(href, "orgaos", None)

        membros = orgao.get("dados", [])
        for membro in membros:
            data_fim = membro.get("dataFim")
            data.append(
                CamaraOrgaosMembrosArg(
                    id_lote=id_lote,
                    id_orgao=id_orgao,
                    id_deputado=membro.get("id"),
                    id_legislatura=membro.get("idLegislatura"),
                    titulo=membro.get("titulo"),
                    data_inicio=date.fromisoformat(membro.get("dataInicio")),
                    data_fim=date.fromisoformat(data_fim) if data_fim else None,
                )
            )

    data = deduplicate_membros(data, legislaturas)

    insert_camara_membros_orgaos_db(data=data)

    return
