import datetime
import os
from pathlib import Path
import uuid

from globus_sdk import TransferClient
from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON

from orchestration.flows.scicat.ingest import ingest_dataset
from orchestration.flows.bl832.config import Config832
from orchestration.globus.transfer import GlobusEndpoint, start_transfer
from orchestration.prefect import schedule_prefect_flow
from orchestration.prometheus_utils import PrometheusMetrics


API_KEY = os.getenv("API_KEY")
TOMO_INGESTOR_MODULE = "orchestration.flows.bl832.ingest_tomo832"


@task(name="transfer_spot_to_data")
def transfer_spot_to_data(
    file_path: str,
    transfer_client: TransferClient,
    spot832: GlobusEndpoint,
    data832: GlobusEndpoint,
):
    logger = get_run_logger()

    # if source_file begins with "/", it will mess up os.path.join
    if file_path[0] == "/":
        file_path = file_path[1:]

    source_path = os.path.join(spot832.root_path, file_path)
    dest_path = os.path.join(data832.root_path, file_path)
    success, _ = start_transfer(
        transfer_client,
        spot832,
        source_path,
        data832,
        dest_path,
        max_wait_seconds=600,
        logger=logger,
    )
    logger.info(f"spot832 to data832 globus task_id: {task}")
    return success


@task(name="transfer_data_to_nersc")
def transfer_data_to_nersc(
    file_path: str,
    transfer_client: TransferClient,
    data832: GlobusEndpoint,
    nersc832: GlobusEndpoint,
):
    logger = get_run_logger()

    # if source_file begins with "/", it will mess up os.path.join
    if file_path[0] == "/":
        file_path = file_path[1:]

    # Initialize config
    config = Config832()

    # Import here to avoid circular imports
    from orchestration.transfer_controller import get_transfer_controller, CopyMethod

    # Change prometheus_metrics=None if do not want to push metrics
    # prometheus_metrics = None
    prometheus_metrics = PrometheusMetrics()
    # Get a Globus transfer controller
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config,
        prometheus_metrics=prometheus_metrics
    )

    # Use transfer controller to copy the file
    # The controller automatically handles metrics collection and pushing
    logger.info(f"Transferring {file_path} from data832 to nersc")
    success = transfer_controller.copy(
        file_path=file_path,
        source=data832,
        destination=nersc832
    )

    return success


<<<<<<< HEAD
@flow(name="new_832_file_flow")
def process_new_832_file_flow(
    file_path: str,
    is_export_control=False,
    send_to_nersc=True,
    config=None
):
    process_new_832_file_task(
        file_path=file_path,
        is_export_control=is_export_control,
        send_to_nersc=send_to_nersc,
        config=config
    )


@task(name="new_832_file_task")
def process_new_832_file_task(
    file_path: str,
    is_export_control=False,
    send_to_nersc=True,
    config=None
):
=======
@flow(name="new_832_file_flow", flow_run_name="process_new-{{ file_path | basename }}")
def process_new_832_file(file_path: str,
                         is_export_control=False,
                         send_to_nersc=True,
                         config=None):
>>>>>>> af01bc5 (Prefect 3 upgrade: flows, tests, dispatcher/BL832 updates, requirements, and Prefect YAML)
    """
    Sends a file along a path:
        - Copy from spot832 to data832
        - Copy from data832 to NERSC
        - Ingest into SciCat
        - Schedule a job to delete from spot832 in the future
        - Schedule a job to delete from data832 in the future

    The is_export_control and send_to_nersc flags are functionally identical, but
    they are separate options at the beamlines, so we leave them as separate parameters
    in case the desired behavior changes in the future.

    :param file_path: path to file on spot832
    :param is_export_control: if True, do not send to NERSC ingest into SciCat
    :param send_to_nersc: if True, send to NERSC and ingest into SciCat
    """

    logger = get_run_logger()
    logger.info("starting flow")
    if not config:
        config = Config832()

    # paths come in from the app on spot832 as /global/raw/...
    # remove 'global' so that all paths start with 'raw', which is common
    # to all 3 systems.
    logger.info(f"Transferring {file_path} from spot to data")
    relative_path = file_path.split("/global")[1]
    transfer_spot_to_data(relative_path, config.tc, config.spot832, config.data832)

    logger.info(f"Transferring {file_path} to spot to data")

    if not is_export_control and send_to_nersc:
        transfer_data_to_nersc(
            relative_path, config.tc, config.data832, config.nersc832
        )
        logger.info(
            f"File successfully transferred from data832 to NERSC {file_path}. Task {task}"
        )
        flow_name = f"ingest scicat: {Path(file_path).name}"
        logger.info(f"Ingesting {file_path} with {TOMO_INGESTOR_MODULE}")
        try:
            ingest_dataset(file_path, TOMO_INGESTOR_MODULE)
        except Exception as e:
            logger.error(f"SciCat ingest failed with {e}")

        # schedule_prefect_flow(
        #     "ingest_scicat/ingest_scicat",
        #     flow_name,
        #     {"relative_path": relative_path},
        #     datetime.timedelta(0.0),
        # )

    bl832_settings = JSON.load("bl832-settings").value

    flow_name = f"delete spot832: {Path(file_path).name}"
    schedule_spot832_delete_days = bl832_settings["delete_spot832_files_after_days"]
    schedule_data832_delete_days = bl832_settings["delete_data832_files_after_days"]
    schedule_prefect_flow(
        "prune_spot832/prune_spot832",
        flow_name,
        {
            "relative_path": relative_path,
            "source_endpoint": config.spot832,
            "check_endpoint": config.data832,
         },

        datetime.timedelta(days=schedule_spot832_delete_days),
    )
    logger.info(
        f"Scheduled delete from spot832 at {datetime.timedelta(days=schedule_spot832_delete_days)}"
    )

    flow_name = f"delete data832: {Path(file_path).name}"
    schedule_prefect_flow(
        "prune_data832/prune_data832",
        flow_name,
        {
            "relative_path": relative_path,
            "source_endpoint": config.data832,
            "check_endpoint": config.nersc832,
        },
        datetime.timedelta(days=schedule_data832_delete_days),
    )
    logger.info(
        f"Scheduled delete from data832 at {datetime.timedelta(days=schedule_data832_delete_days)}"
    )
    return


@flow(name="test_832_transfers")
def test_transfers_832(file_path: str = "/raw/transfer_tests/test.txt"):
    logger = get_run_logger()
    config = Config832()
    # test_scicat(config)
    logger.info(f"{str(uuid.uuid4())}{file_path}")
    # copy file to a uniquely-named file in the same folder
    file = Path(file_path)
    new_file = str(file.with_name(f"test_{str(uuid.uuid4())}.txt"))
    logger.info(new_file)
    success, _ = start_transfer(
        config.tc, config.spot832, file_path, config.spot832, new_file, logger=logger
    )
    logger.info(success)
    spot832_path = transfer_spot_to_data(
        new_file, config.tc, config.spot832, config.data832
    )
    logger.info(f"Transferred {spot832_path} to spot to data")

    task = transfer_data_to_nersc(new_file, config.tc, config.data832, config.nersc832)
    logger.info(
        f"File successfully transferred from data832 to NERSC {spot832_path}. Task {task}"
    )


@flow(name="test_832_transfers_grafana")
def test_transfers_832_grafana(file_path: str = "/raw/transfer_tests/test/"):
    logger = get_run_logger()
    config = Config832()

    task = transfer_data_to_nersc(file_path, config.tc, config.data832, config.nersc_alsdev)

    logger.info(
        f"File successfully transferred from data832 to NERSC {file_path}. Task {task}"
    )
