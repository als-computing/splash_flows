import datetime
import logging
from typing import Optional

from prefect import flow, get_run_logger, task
from prefect.variables import Variable

from orchestration.flows.bl7011.config import Config7011
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
    days_from_now: float = 0.0,
    config: Config7011 = None
) -> bool:
    """
    Prune (delete) data from a globus endpoint.
    If days_from_now is 0, executes pruning immediately.
    Otherwise, schedules pruning for future execution using Prefect.
    Args:
        file_path (str): The path to the file or directory to prune
        source_endpoint (GlobusEndpoint): The globus endpoint containing the data
        check_endpoint (Optional[GlobusEndpoint]): If provided, verify data exists here before pruning
        days_from_now (float): Delay before pruning; if 0, prune immediately
    Returns:
        bool: True if pruning was successful or scheduled successfully, False otherwise
    """
    logger = get_run_logger()
    if not file_path:
        logger.error("No file_path provided for pruning operation")
        return False

    if not source_endpoint:
        logger.error("No source_endpoint provided for pruning operation")
        return False

    if not config:
        config = Config7011()

    if days_from_now < 0:
        raise ValueError(f"Invalid days_from_now: {days_from_now}")

    # JSON blocks are deprecated, we should use what they recommend in the docs
    # globus_settings = JSON.load("globus-settings").value
    # max_wait_seconds = globus_settings["max_wait_seconds"]

    logger.info(f"Setting up pruning of '{file_path}' from '{source_endpoint.name}'")

    # convert float days → timedelta
    delay: datetime.timedelta = datetime.timedelta(days=days_from_now)

    # If days_from_now is 0, prune immediately
    if delay.total_seconds() == 0:
        logger.info(f"Executing immediate pruning of '{file_path}' from '{source_endpoint.name}'")
        return _prune_globus_endpoint(
            relative_path=file_path,
            source_endpoint=source_endpoint,
            check_endpoint=check_endpoint,
            config=config
        )
    else:
        # Otherwise, schedule pruning for future execution
        logger.info(f"Scheduling pruning of '{file_path}' from '{source_endpoint.name}' "
                    f"in {delay.total_seconds()/86400:.1f} days")

        try:
            schedule_prefect_flow(
                deployment_name="prune_globus_endpoint/prune_globus_endpoint",
                flow_run_name=f"prune_globus-{source_endpoint.name}-{file_path}",
                parameters={
                    "relative_path": file_path,
                    "source_endpoint": source_endpoint,
                    "check_endpoint": check_endpoint,
                    "config": config
                },
                duration_from_now=delay,
            )
            logger.info(f"Successfully scheduled pruning task for {delay.total_seconds()/86400:.1f} days from now")
            return True
        except Exception as e:
            logger.error(f"Failed to schedule pruning task: {str(e)}", exc_info=True)
            return False

# Prune code is from the prune_controller in this PR: https://github.com/als-computing/splash_flows_globus/pulls
# Note: once the PR is merged, we can import prune_controller directly instead of copying the code here.


# @staticmethod
@flow(name="prune_globus_endpoint", flow_run_name="prune_globus-{source_endpoint.name}-{relative_path}")
def _prune_globus_endpoint(
    relative_path: str,
    source_endpoint: GlobusEndpoint,
    check_endpoint: Optional[GlobusEndpoint] = None,
    config: Config7011 = None
) -> None:
    """
    Prefect flow that performs the actual Globus endpoint pruning operation.
    Args:
        relative_path (str): The path of the file or directory to prune
        source_endpoint (GlobusEndpoint): The Globus endpoint to prune from
        check_endpoint (Optional[GlobusEndpoint]): If provided, verify data exists here before pruning
        config (BeamlineConfig): Configuration object with transfer client
    """
    logger = get_run_logger()
    logger.info(f"Running Globus pruning flow for '{relative_path}' from '{source_endpoint.name}'")

    if not config:
        config = Config7011()

    globus_settings = Variable.get("globus-settings")
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


@flow(name="new_7011_file_flow", flow_run_name="process_new-{file_path}")
def process_new_7011_file_flow(
    file_path: str,
    metadata: Optional[dict] = None,
    config: Optional[Config7011] = None
) -> None:
    """
    Flow to process a new file at BL 7.0.1.1

    :param file_path: Path to the new file to be processed.
    :param metadata: Optional metadata associated with the file.
    :param config: Configuration settings for processing.
    :return: None
    """
    process_new_7011_file_task(file_path=file_path, metadata=metadata, config=config)


@task(name="new_7011_file_task")
def process_new_7011_file_task(
    file_path: str,
    metadata: Optional[dict] = None,
    config: Optional[Config7011] = None
) -> None:
    """
    Flow to process a new file at BL 7.0.1.1
    1. Copy the file from the data7011 to NERSC CFS. Ingest file path in SciCat.
    2. Schedule pruning from data7011. 6 months from now.
    3. Copy the file from NERSC CFS to NERSC HPSS. Ingest file path in SciCat.
    4. Schedule pruning from NERSC CFS.

    :param file_path: Path to the new file to be processed.
    :param metadata: Optional metadata associated with the file.
    :param config: Configuration settings for processing.
    """
    logger = get_run_logger()
    logger.info(f"Processing new 7011 file: {file_path}")

    if not config:
        logger.info("No config provided, using default Config7011")
        config = Config7011()

    logger.info("Initializing transfer controller for Globus transfers")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    logger.info(f"Transferring file {file_path} from data7011 to NERSC CFS")
    nersc_transfer_success = transfer_controller.copy(
        file_path=file_path,
        source=config.bl7011_compute_dtn,
        destination=config.bl7011_nersc_alsdev
    )

    if not nersc_transfer_success:
        logger.error(f"Failed to transfer file {file_path} to NERSC CFS")
        raise Warning(f"Failed to transfer file {file_path} to NERSC CFS")
    else:
        logger.info(f"Successfully transferred file {file_path} to NERSC CFS")

    # Schedule pruning from QNAP
    # Waiting for PR #62 to be merged (prune_controller)
    # TODO: Determine scheduling days_from_now based on beamline needs

    logger.info(f"Scheduling pruning of file {file_path} from data7011 after configured retention period")
    bl7011_settings = Variable.get("bl7011-settings")
    prune(
        file_path=file_path,
        source_endpoint=config.bl7011_compute_dtn,
        check_endpoint=config.bl7011_nersc_alsdev,
        days_from_now=bl7011_settings["delete_data7011_files_after_days"],  # set to 180 days
    )

    # TODO: Copy the file from NERSC CFS to NERSC HPSS.. after 2 years?
    # Waiting for PR #62 to be merged (transfer_controller)

    # TODO: Ingest file path in SciCat
    # Waiting for PR #62 to be merged (scicat_controller)
    # scicat will ingest the "metadata" parameter if provided, which will contain the UID for bluesky runs in databroker.
    # the scicat spec for xpcs will need to use databroker to get the actual metadata from the UID.


@flow(name="move_7011_flight_check", flow_run_name="move_7011_flight_check-{file_path}")
def move_7011_flight_check(
    file_path: str = "test/test_065.h5",
) -> None:
    """Please keep your arms and legs inside the vehicle at all times.

    :param file_path: Path to the test file to be transferred.
    :raises RuntimeError: If the transfer fails.
    :return: None
    """
    logger = get_run_logger()
    logger.info("7011 flight check: testing transfer from data7011 to NERSC CFS")

    config = Config7011()

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    try:
        logger.info(f"7011 flight check: transferring file {file_path}")
        success = transfer_controller.copy(
            file_path=file_path,
            source=config.bl7011_compute_dtn,
            destination=config.bl7011_nersc_alsdev
        )
    except Exception as e:
        logger.error(f"7011 flight check: transfer failed with exception {e}")
        raise RuntimeError(f"7011 flight check: transfer failed with exception {e}")
    if success is True:
        logger.info("7011 flight check: transfer successful")
    else:
        logger.error("7011 flight check: transfer failed")
        raise RuntimeError("7011 flight check: transfer failed")
