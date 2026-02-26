from datetime import date
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact

from config.loader import load_config
from config.parameters import TasksNames
from utils.io import fetch_json, save_json

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.EXTRACT_CAMARA_LEGISLATURA,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
def extract_legislatura(
    start_date: date,
    lote_id: int,
    ignore_tasks: list[str],
    out_dir: str = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> dict | None:
    logger = get_run_logger()

    if TasksNames.EXTRACT_CAMARA_LEGISLATURA in ignore_tasks:
        logger.warning(f"A Task {TasksNames.EXTRACT_CAMARA_LEGISLATURA} foi ignorada")
        return

    LEGISLATURA_URL = (
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}legislaturas?data={start_date}"
    )

    logger.info(f"CÂMARA: Baixando Legislatura atual de {LEGISLATURA_URL} -> {out_dir}")

    dest = Path(out_dir) / "legislatura.json"

    json = fetch_json(
        url=LEGISLATURA_URL, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )
    json = cast(dict, json)

    save_json(json, dest)

    dados = json.get("dados", [])[0]
    create_table_artifact(
        key="legislatura",
        table=[
            {
                "data": start_date.isoformat(),
                "id_legislatura": dados.get("id"),
                "data_inicio": dados.get("dataInicio"),
                "data_fim": dados.get("dataFim"),
            }
        ],
        description="Legislatura atual",
    )

    return json
