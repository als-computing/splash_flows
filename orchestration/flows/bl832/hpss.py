"""
This module contains the HPSS flow for BL832.
"""
import logging
from typing import List, Optional

from prefect import flow

from orchestration.config import BeamlineConfig
from orchestration.transfer_endpoints import FileSystemEndpoint, HPSSEndpoint
from orchestration.flows.bl832.config import Config832
from orchestration.transfer_controller import get_transfer_controller, CopyMethod

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@flow(name="cfs_to_hpss_flow")
def cfs_to_hpss_flow(
    file_path: str = None,
    source_endpoint: FileSystemEndpoint = None,
    destination_endpoint: HPSSEndpoint = None,
    config: BeamlineConfig = Config832()
) -> bool:
    """
    The CFS to HPSS flow for BL832.

    Parameters
    ----------
    file_path : str
        The path of the file to transfer.
    source_endpoint : FileSystemEndpoint
        The source endpoint.
    destination_endpoint : HPSSEndpoint
        The destination endpoint.
    """

    logger.info("Running cfs_to_hpss_flow")
    logger.info(f"Transferring {file_path} from {source_endpoint.name} to {destination_endpoint.name}")

    logger.info("Configuring transfer controller for CFS_TO_HPSS.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.CFS_TO_HPSS,
        config=config
    )

    logger.info("CFSToHPSSTransferController selected. Initiating transfer.")
    result = transfer_controller.copy(
        file_path=file_path,
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint
    )

    return result


@flow(name="hpss_to_cfs_flow")
def hpss_to_cfs_flow(
    file_path: str = None,
    source_endpoint: HPSSEndpoint = None,
    destination_endpoint: FileSystemEndpoint = None,
    files_to_extract: Optional[List[str]] = None,
    config: BeamlineConfig = Config832()
) -> bool:
    """
    The HPSS to CFS flow for BL832.

    Parameters
    ----------
    file_path : str
        The path of the file to transfer.
    source_endpoint : HPSSEndpoint
        The source endpoint.
    destination_endpoint : FileSystemEndpoint
        The destination endpoint.
    """

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.HPSS_TO_CFS,
        config=config
    )

    result = transfer_controller.copy(
        file_path=file_path,
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint,
        files_to_extract=files_to_extract,
    )

    return result


if __name__ == "__main__":

    config = Config832()
    project_name = "ALS-11193_nbalsara"
    source_endpoint = FileSystemEndpoint(
        name="CFS",
        root_path="/global/cfs/cdirs/als/data_mover/8.3.2/raw/"
    )
    destination_endpoint = config.hpss_alsdev

    cfs_to_hpss_flow(
        file_path=f"{project_name}",
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint,
        config=config
    )
