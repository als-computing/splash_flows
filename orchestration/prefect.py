import asyncio
import datetime
import logging

from prefect import get_run_logger, task
from prefect import get_client

from prefect.states import Scheduled
import pytz

logger = logging.getLogger("orchestration.prefect")


async def schedule(
    deployment_name,
    flow_run_name,
    parameters,
    duration_from_now: datetime.timedelta,
    logger=logger,
):
    async with get_client() as client:
        deployment = await client.read_deployment_by_name(deployment_name)
        assert (
            deployment
        ), f"No deployment found in config for deployment_name {deployment_name}"
        timezone = pytz.timezone("America/Los_Angeles")  # Adjust the timezone as needed
        now = datetime.datetime.now(timezone)
        date_time_tz = now + duration_from_now
        await client.create_flow_run_from_deployment(
            deployment.id,
            state=Scheduled(scheduled_time=date_time_tz),
            parameters=parameters,
            name=flow_run_name,
        )
    return


@task(name="Schedule Prefect Flow")
async def schedule_prefect_flow(
    deployment_name, flow_run_name, parameters, duration_from_now: datetime.timedelta
):
    logger = get_run_logger()
    await schedule(deployment_name, flow_run_name, parameters, duration_from_now, logger)
    return


if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()

    asyncio.run(
        schedule_prefect_flow.fn(
            "test_832_transfers/test_transfers_832", "local test", None, 1
        )
    )
