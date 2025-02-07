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
