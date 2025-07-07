import datetime
import logging
from typing import Optional

from prefect import flow
from prefect.blocks.system import JSON

from orchestration.flows.bl733.config import Config733
from orchestration.globus.transfer import GlobusEndpoint, prune_one_safe
from orchestration.prefect import schedule_prefect_flow
from orchestration.transfer_controller import CopyMethod, get_transfer_controller

logger = logging.getLogger(__name__)

# Prune code is from the prune_controller in this PR: https://github.com/als-computing/splash_flows_globus/pulls
# Note: once the PR is merged, we can import prune_controller directly instead of copying the code here.


def prune(
    file_path: str = None,
    source_endpoint: GlobusEndpoint = None,
    check_endpoint: Optional[GlobusEndpoint] = None,
    days_from_now: float = 0.0
) -> bool:
    """
    Prune (delete) data from a file system endpoint.
    If days_from_now is 0, executes pruning immediately.
    Otherwise, schedules pruning for future execution using Prefect.
    Args:
        file_path (str): The path to the file or directory to prune
        source_endpoint (FileSystemEndpoint): The file system endpoint containing the data
        check_endpoint (Optional[FileSystemEndpoint]): If provided, verify data exists here before pruning
        days_from_now (datetime.timedelta): Delay before pruning; if 0, prune immediately
    Returns:
        bool: True if pruning was successful or scheduled successfully, False otherwise
    """
    if not file_path:
        logger.error("No file_path provided for pruning operation")
        return False

    if not source_endpoint:
        logger.error("No source_endpoint provided for pruning operation")
        return False

    # globus_settings = JSON.load("globus-settings").value
    # max_wait_seconds = globus_settings["max_wait_seconds"]
    flow_name = f"prune_from_{source_endpoint.name}"
    logger.info(f"Setting up pruning of '{file_path}' from '{source_endpoint.name}'")

    # convert float days → timedelta
    days_from_now: datetime.timedelta = datetime.timedelta(days=days_from_now)

    # If days_from_now is 0, prune immediately
    if days_from_now.total_seconds() == 0:
        logger.info(f"Executing immediate pruning of '{file_path}' from '{source_endpoint.name}'")
        return _prune_globus_endpoint(
            relative_path=file_path,
            source_endpoint=source_endpoint,
            check_endpoint=check_endpoint,
            config=self.config
        )
    else:
        # Otherwise, schedule pruning for future execution
        logger.info(f"Scheduling pruning of '{file_path}' from '{source_endpoint.name}' "
                    f"in {days_from_now.total_seconds()/86400:.1f} days")

        try:
            schedule_prefect_flow(
                deployment_name="prune_globus_endpoint/prune_globus_endpoint",
                flow_run_name=flow_name,
                parameters={
                    "relative_path": file_path,
                    "source_endpoint": source_endpoint,
                    "check_endpoint": check_endpoint,
                    "config": self.config
                },
                duration_from_now=days_from_now,
            )
            logger.info(f"Successfully scheduled pruning task for {days_from_now.total_seconds()/86400:.1f} days from now")
            return True
        except Exception as e:
            logger.error(f"Failed to schedule pruning task: {str(e)}", exc_info=True)
            return False

# Prune code is from the prune_controller in this PR: https://github.com/als-computing/splash_flows_globus/pulls
# Note: once the PR is merged, we can import prune_controller directly instead of copying the code here.


# @staticmethod
@flow(name="prune_globus_endpoint")
def _prune_globus_endpoint(
    relative_path: str,
    source_endpoint: GlobusEndpoint,
    check_endpoint: Optional[GlobusEndpoint] = None,
    config: Config733 = None
) -> None:
    """
    Prefect flow that performs the actual Globus endpoint pruning operation.
    Args:
        relative_path (str): The path of the file or directory to prune
        source_endpoint (GlobusEndpoint): The Globus endpoint to prune from
        check_endpoint (Optional[GlobusEndpoint]): If provided, verify data exists here before pruning
        config (BeamlineConfig): Configuration object with transfer client
    """
    logger.info(f"Running Globus pruning flow for '{relative_path}' from '{source_endpoint.name}'")

    globus_settings = JSON.load("globus-settings").value
    max_wait_seconds = globus_settings["max_wait_seconds"]
    flow_name = f"prune_from_{source_endpoint.name}"
    logger.info(f"Running flow: {flow_name}")
    logger.info(f"Pruning {relative_path} from source endpoint: {source_endpoint.name}")
    prune_one_safe(
        file=relative_path,
        if_older_than_days=0,
        transfer_client=config.tc,
        source_endpoint=source_endpoint,
        check_endpoint=check_endpoint,
        logger=logger,
        max_wait_seconds=max_wait_seconds
    )


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

    if not config:
        config = Config733()

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    transfer_controller.copy(
        file_path=file_path,
        source=config.data733_raw,
        destination=config.nersc733_alsdev_raw
    )

    # TODO: Ingest file path in SciCat

    # TODO: Schedule pruning from QNAP
    # Waiting for PR #62 to be merged (prune_controller)
    # Determine scheduling days_from_now based on beamline needs
    prune(
        file_path=file_path,
        source_endpoint=config.data733_raw,
        check_endpoint=config.nersc733_alsdev_raw,
        days_from_now=1.0  # work with Chenhui/Eric to determine appropriate value
    )

    # TODO: Copy the file from NERSC CFS to NERSC HPSS
    # Waiting for PR #62 to be merged (transfer_controller)

    # TODO: Ingest file path in SciCat
