import datetime
import logging
import os
from pathlib import Path
import uuid

from prefect import flow, task
from prefect.blocks.system import JSON

from orchestration.flows.scicat.ingest import ingest_dataset
from orchestration.flows.bl832.config import Config832
from orchestration.globus.transfer import start_transfer
from orchestration.prune_controller import get_prune_controller, PruneMethod
from orchestration.transfer_controller import get_transfer_controller, CopyMethod

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_KEY = os.getenv("API_KEY")
TOMO_INGESTOR_MODULE = "orchestration.flows.bl832.ingest_tomo832"


@flow(name="new_832_file_flow")
def process_new_832_file(
    file_path: str,
    send_to_nersc=True,
    config: Config832 = None
) -> None:
    """
    Sends a file along a path:
        - Copy from spot832 to data832
        - Copy from data832 to NERSC
        - Ingest into SciCat
        - Schedule a job to delete from spot832 in the future
        - Schedule a job to delete from data832 in the future

    :param file_path: path to file on spot832
    :param send_to_nersc: if True, send to NERSC and ingest into SciCat
    """

    logger.info("Starting New 832 File Flow")
    if not config:
        config = Config832()

    # paths come in from the app on spot832 as /global/raw/...
    # remove 'global' so that all paths start with 'raw', which is common
    # to all 3 systems.
    logger.info(f"Transferring {file_path} from spot to data")
    relative_path = file_path.split("/global")[1]

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    data832_transfer_success = transfer_controller.copy(
        file_path=relative_path,
        source=config.spot832,
        destination=config.data832,
    )

    if send_to_nersc and data832_transfer_success:
        nersc_transfer_success = transfer_controller.copy(
            file_path=relative_path,
            source=config.data832,
            destination=config.nersc832
        )

        if nersc_transfer_success:
            logger.info(
                f"File successfully transferred from data832 to NERSC {file_path}. Task {task}"
            )
            logger.info(f"Ingesting {file_path} with {TOMO_INGESTOR_MODULE}")
            try:
                ingest_dataset(file_path, TOMO_INGESTOR_MODULE)
            except Exception as e:
                logger.error(f"SciCat ingest failed with {e}")

    bl832_settings = JSON.load("bl832-settings").value

    schedule_spot832_delete_days = bl832_settings["delete_spot832_files_after_days"]
    schedule_data832_delete_days = bl832_settings["delete_data832_files_after_days"]

    prune_controller = get_prune_controller(
        prune_type=PruneMethod.GLOBUS,
        config=config
    )

    prune_controller.prune(
        file_path=relative_path,
        source_endpoint=config.spot832,
        check_endpoint=config.data832,
        days_from_now=datetime.timedelta(days=schedule_spot832_delete_days)
    )
    logger.info(
        f"Scheduled delete from spot832 at {datetime.timedelta(days=schedule_spot832_delete_days)}"
    )

    prune_controller.prune(
        file_path=relative_path,
        source_endpoint=config.data832,
        check_endpoint=config.nersc832,
        days_from_now=datetime.timedelta(days=schedule_data832_delete_days)
    )
    logger.info(
        f"Scheduled delete from data832 at {datetime.timedelta(days=schedule_data832_delete_days)}"
    )

    return


@flow(name="test_832_transfers")
def test_transfers_832(file_path: str = "/raw/transfer_tests/test.txt"):
    config = Config832()
    logger.info(f"{str(uuid.uuid4())}{file_path}")
    # copy file to a uniquely-named file in the same folder
    file = Path(file_path)
    new_file = str(file.with_name(f"test_{str(uuid.uuid4())}.txt"))
    logger.info(new_file)

    success = start_transfer(
        config.tc, config.spot832, file_path, config.spot832, new_file, logger=logger
    )

    logger.info(success)

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    dat832_success = transfer_controller.copy(
        file_path=new_file,
        source=config.spot832,
        destination=config.data832,
    )
    logger.info(f"Transferred {new_file} from spot to data. Success: {dat832_success}")

    nersc_success = transfer_controller.copy(
        file_path=new_file,
        source=config.data832,
        destination=config.nersc832,
    )
    logger.info(f"File successfully transferred from data832 to NERSC {new_file}. Success: {nersc_success}")
    pass
