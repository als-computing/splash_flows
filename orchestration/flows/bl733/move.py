import logging
from prefect import flow

from orchestration.flows.bl733.config import Config733
from orchestration.transfer_controller import CopyMethod, get_transfer_controller

logger = logging.getLogger(__name__)


@flow(name="new_733_file_flow")
def process_new_733_file(
    file_path: str,
    config: Config733
) -> None:
    """
    Flow to process a new file at BL 7.3.3
    1. Copy the file from the data733 to NERSC CFS. Ingest file path in SciCat.
    2. Schedule pruning from data733.
    3. Copy the file from NERSC CFS to NERSC HPSS. Ingest file path in SciCat.
    4. Schedule pruning from NERSC CFS.

    :param file_path: Path to the new file to be processed.
    :param config: Configuration settings for processing.
    """

    logger.info(f"Processing new 733 file: {file_path}")

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=Config733()
    )

    transfer_controller.copy(
        file_path=file_path,
        source=config.data733_raw,
        destination=config.nersc733_alsdev_raw
    )

    # TODO: Ingest file path in SciCat

    # TODO: Schedule pruning from QNAP
    # Waiting for PR #62 to be merged (prune_controller)

    # TODO: Copy the file from NERSC CFS to NERSC HPSS
    # Waiting for PR #62 to be merged (transfer_controller)

    # TODO: Ingest file path in SciCat
