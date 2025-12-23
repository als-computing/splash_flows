import datetime
import logging
from typing import Optional

from prefect import flow, task
from prefect.variables import Variable

from orchestration.flows.dichroism.config import ConfigDichroism
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
    config: ConfigDichroism = None
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
    if not file_path:
        logger.error("No file_path provided for pruning operation")
        return False

    if not source_endpoint:
        logger.error("No source_endpoint provided for pruning operation")
        return False

    if not config:
        config = ConfigDichroism()

    if days_from_now < 0:
        raise ValueError(f"Invalid days_from_now: {days_from_now}")

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
@flow(name="prune_globus_endpoint", flow_run_name="prune_globus_endpoint-{relative_path}")
def _prune_globus_endpoint(
    relative_path: str,
    source_endpoint: GlobusEndpoint,
    check_endpoint: Optional[GlobusEndpoint] = None,
    config: ConfigDichroism = None
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

    if not config:
        config = ConfigDichroism()

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

# ----------------------------------------------
# Flow and task to process new files at BL 4.0.2
# ----------------------------------------------


@flow(name="new_402_file_flow", flow_run_name="process_new-{file_path}")
def process_new_402_file_flow(
    file_path: str,
    config: Optional[ConfigDichroism] = None
) -> None:
    process_new_402_file_task(
        file_path=file_path,
        config=config
    )


@task(name="new_402_file_task")
def process_new_402_file_task(
    file_path: str,
    config: Optional[ConfigDichroism] = None
) -> None:
    """
    Flow to process a new file at BL 4.0.2
    1. Copy the file from the data402 to NERSC CFS. Ingest file path in SciCat.
    2. Schedule pruning from data402. 6 months from now.
    3. Copy the file from NERSC CFS to NERSC HPSS. Ingest file path in SciCat.
    4. Schedule pruning from NERSC CFS.

    :param file_path: Path to the new file to be processed.
    :param config: Configuration settings for processing.
    """

    logger.info(f"Processing new 402 file: {file_path}")

    if not config:
        config = ConfigDichroism()

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    transfer_controller.copy(
        file_path=file_path,
        source=config.bl402_compute_dtn,
        destination=config.bl402_nersc_alsdev_raw
    )

    # TODO: Ingest file path in SciCat
    # Waiting for PR #62 to be merged (scicat_controller)

    # Schedule pruning from QNAP
    # Waiting for PR #62 to be merged (prune_controller)
    # TODO: Determine scheduling days_from_now based on beamline needs

    dichroism_settings = Variable.get("dichroism-settings")

    prune(
        file_path=file_path,
        source_endpoint=config.bl402_compute_dtn,
        check_endpoint=config.bl402_nersc_alsdev_raw,
        days_from_now=dichroism_settings["delete_data402_files_after_days"]  # determine appropriate value: currently 6 months
    )

    # TODO: Copy the file from NERSC CFS to NERSC HPSS.. after 2 years?
    # Waiting for PR #62 to be merged (transfer_controller)

    # TODO: Ingest file path in SciCat
    # Waiting for PR #62 to be merged (scicat_controller)


@flow(name="move_402_flight_check", flow_run_name="move_402_flight_check-{file_path}")
def move_402_flight_check(
    file_path: str = "test_directory/test.txt",
):
    """Please keep your arms and legs inside the vehicle at all times."""
    logger.info("402 flight check: testing transfer from data402 to NERSC CFS")

    config = ConfigDichroism()

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    success = transfer_controller.copy(
        file_path=file_path,
        source=config.bl402_compute_dtn,
        destination=config.bl402_nersc_alsdev_raw
    )
    if success is True:
        logger.info("402 flight check: transfer successful")
    else:
        logger.error("402 flight check: transfer failed")
        raise RuntimeError("402 flight check: transfer failed")

# ----------------------------------------------
# Flow and task to process new files at BL 6.3.1
# ----------------------------------------------


@flow(name="new_631_file_flow", flow_run_name="process_new-{file_path}")
def process_new_631_file_flow(
    file_path: str,
    config: Optional[ConfigDichroism] = None
) -> None:
    process_new_631_file_task(
        file_path=file_path,
        config=config
    )


@task(name="new_631_file_task")
def process_new_631_file_task(
    file_path: str,
    config: Optional[ConfigDichroism] = None
) -> None:
    """
    Flow to process a new file at BL 6.3.1
    1. Copy the file from the data631 to NERSC CFS. Ingest file path in SciCat.
    2. Schedule pruning from data631. 6 months from now.
    3. Copy the file from NERSC CFS to NERSC HPSS. Ingest file path in SciCat.
    4. Schedule pruning from NERSC CFS.

    :param file_path: Path to the new file to be processed.
    :param config: Configuration settings for processing.
    """

    logger.info(f"Processing new 631 file: {file_path}")

    if not config:
        config = ConfigDichroism()

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    transfer_controller.copy(
        file_path=file_path,
        source=config.bl631_compute_dtn,
        destination=config.bl631_nersc_alsdev_raw
    )

    # TODO: Ingest file path in SciCat
    # Waiting for PR #62 to be merged (scicat_controller)

    # Waiting for PR #62 to be merged (prune_controller)
    # TODO: Determine scheduling days_from_now based on beamline needs

    dichroism_settings = Variable.get("dichroism-settings")

    prune(
        file_path=file_path,
        source_endpoint=config.bl631_compute_dtn,
        check_endpoint=config.bl631_nersc_alsdev_raw,
        days_from_now=dichroism_settings["delete_data631_files_after_days"]  # determine appropriate value: currently 6 months
    )

    # TODO: Copy the file from NERSC CFS to NERSC HPSS.. after 2 years?
    # Waiting for PR #62 to be merged (transfer_controller)

    # TODO: Ingest file path in SciCat
    # Waiting for PR #62 to be merged (scicat_controller)


@flow(name="move_631_flight_check", flow_run_name="move_631_flight_check-{file_path}")
def move_631_flight_check(
    file_path: str = "test_directory/test.txt",
):
    """Please keep your arms and legs inside the vehicle at all times."""
    logger.info("631 flight check: testing transfer from data631 to NERSC CFS")

    config = ConfigDichroism()

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    success = transfer_controller.copy(
        file_path=file_path,
        source=config.bl631_compute_dtn,
        destination=config.bl631_nersc_alsdev_raw
    )
    if success is True:
        logger.info("631 flight check: transfer successful")
    else:
        logger.error("631 flight check: transfer failed")
        raise RuntimeError("631 flight check: transfer failed")
