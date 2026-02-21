import asyncio
from asyncio.tasks import sleep
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import LogFilter, LogFilterFlowRunId, LogFilterLevel
from prefect.client.schemas.sorting import LogSort

from config.parameters import FlowsNames


def save_logs(flow_run_name: str, flow_run_id: UUID, lote_id: int):
    """
    Resgata os logs das flows e as grava no banco de dados.
    """
    asyncio.run(
        async_save_logs(
            flow_run_name=flow_run_name, flow_run_id=flow_run_id, lote_id=lote_id
        )
    )


async def async_save_logs(
    flow_run_name: str,
    flow_run_id: UUID,
    lote_id: int,
    quiet_required=5,
    sleep_required=1,
    timeout=90,
):
    """
    Os logs da Flow Run só ficam disponíveis depois de um tempo, é necessário esperar.
    """
    async with get_client() as client:
        num_logs_available = 0
        quiet = 0
        waited = 0
        while True:
            # Fetch logs for this flow run (optionally filter WARNING+ with level=LogFilterLevel(ge_=30))
            all_logs = await client.read_logs(
                log_filter=LogFilter(
                    flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]),
                    # level=LogFilterLevel(ge_=10),
                ),
                # sort=LogSort.TIMESTAMP_ASC,
            )

            num_logs_now = len(all_logs)

            if num_logs_now == num_logs_available and num_logs_now > 0:
                quiet += 1
            elif num_logs_now > num_logs_available:
                quiet = 0

            num_logs_available = num_logs_now

            if quiet >= quiet_required:
                break

            if waited >= timeout:
                # Escape de segurança
                break

            waited += sleep_required
            await asyncio.sleep(sleep_required)

        for log in all_logs:
            print(f"-->>{log.timestamp} [{log.level}] {log.message}")
