import logging
from prefect import flow

from orchestration.flows.bl7011.config import Config7011
from orchestration.transfer_controller import CopyMethod, get_transfer_controller

logger = logging.getLogger(__name__)


@flow(name="new_7011_file_flow")
def process_new_7011_file(
    file_path: str,
    config: Config7011
) -> None:
    """
    Flow to process a new file at BL 7.0.1.1
    1. Copy the file from the QNAP to NERSC CFS. Ingest file path in SciCat.
    2. Schedule pruning from QNAP.
    3. Copy the file from NERSC CFS to NERSC HPSS. Ingest file path in SciCat.
    4. Schedule pruning from NERSC CFS.

    :param file_path: Path to the new file to be processed.
    :param config: Configuration settings for processing.
    """

    logger.info(f"Processing new BL 7.0.1.1 file: {file_path}")

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=Config7011()
    )

    transfer_controller.copy(
        file_path=file_path,
        source=config.qnap7011_raw,
        destination=config.nersc7011_alsdev_raw
    )

    # TODO: Ingest file path in SciCat

    # TODO: Schedule pruning from QNAP
    # Waiting for PR #62 to be merged (prune_controller)

    # TODO: Copy the file from NERSC CFS to NERSC HPSS
    # Waiting for PR #62 to be merged (transfer_controller)

    # TODO: Ingest file path in SciCat
