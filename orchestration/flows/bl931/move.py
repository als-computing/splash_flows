import datetime
import logging
# from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task
from prefect.variables import Variable

from orchestration.flows.bl931.config import Config931
# from orchestration.flows.scicat.ingest import scicat_ingest_flow
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
    config: Config931 = None
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
        config = Config931()

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
                deployment_name="prune_globus_endpoint/prune_data931",
                flow_run_name=f"prune_globus-{source_endpoint.name}-{file_path}",
                parameters={
                    "relative_path": file_path,
                    "source_endpoint": source_endpoint,
                    "check_endpoint": check_endpoint,
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
    config: Config931 = None
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
        config = Config931()

    globus_settings = Variable.get("globus-settings", _sync=True)
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


@flow(name="new_931_file_flow", flow_run_name="process_new-{file_path}")
def process_new_931_file_flow(
    file_path: str,
    config: Optional[Config931] = None
) -> None:
    process_new_931_file_task(
        file_path=file_path,
        config=config
    )


@task(name="new_931_file_task")
def process_new_931_file_task(
    file_path: str,
    config: Optional[Config931] = None
) -> None:
    """
    Flow to process a new file at BL 9.3.1
    1. Copy the file from the data931 to NERSC CFS. Ingest file path in SciCat.
    2. Schedule pruning from data931. 6 months from now.
    3. Copy the file from NERSC CFS to NERSC HPSS. Ingest file path in SciCat.
    4. Schedule pruning from NERSC CFS.

    :param file_path: Path to the new file to be processed.
    :param config: Configuration settings for processing.
    """
    logger = get_run_logger()

    if not file_path:
        logger.error("No file_path provided")
        raise ValueError("No file_path provided")

    logger.info(f"Processing new 931 file: {file_path}")

    if not config:
        logger.info("No config provided, creating default Config931")
        config = Config931()

    logger.info("Initializing Globus transfer controller")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    # logger.info(f"Step 1: Copying {file_path} from data931 to beegfs ({config.bl931_beegfs.name})")

    # beegfs_transfer_success = transfer_controller.copy(
    #     file_path=file_path,
    #     source=config.bl931_compute_dtn,
    #     destination=config.bl931_beegfs
    # )
    # if not beegfs_transfer_success:
    #     logger.error("Step 1 failed: Beegfs transfer was not successful")
    #     raise Warning("Beegfs transfer failed")
    # else:
    #     logger.info("Step 1 complete: File copied to beegfs")

    logger.info(f"Step 2: Copying {file_path} from data931 ({config.bl931_compute_dtn.name}) "
                f"to NERSC CFS ({config.bl931_nersc_alsdev_raw.name})")

    try:
        nersc_transfer_success = transfer_controller.copy(
            file_path=file_path,
            source=config.bl931_compute_dtn,
            destination=config.bl931_nersc_alsdev_raw
        )
        if not nersc_transfer_success:
            logger.error("Step 2 failed: NERSC transfer was not successful")
            raise RuntimeError("Transfer failed")

        logger.info("Step 2 complete: File copied to NERSC CFS")
    except Exception as e:
        logger.error(f"Step 2 failed: Could not copy file to NERSC CFS: {e}", exc_info=True)
        raise

    # Waiting for PR #62 to be merged (prune_controller)
    # TODO: Determine scheduling days_from_now based on beamline needs

    logger.info("Step 3: Scheduling pruning from data931")
    try:
        bl931_settings = Variable.get("bl931-settings", _sync=True)
        days_from_now = bl931_settings.get("delete_data931_files_after_days", 180)
        logger.info(f"Pruning scheduled for {days_from_now} days from now")

        prune(
            file_path=file_path,
            source_endpoint=config.bl931_compute_dtn,
            check_endpoint=config.bl931_nersc_alsdev_raw,
            days_from_now=days_from_now
        )
        logger.info("Step 3 complete: Pruning scheduled")
    except Exception as e:
        logger.error(f"Step 3 failed: Could not schedule pruning: {e}", exc_info=True)
        raise

    logger.info(f"All steps complete for file: {file_path}")

    # TODO: Copy the file from NERSC CFS to NERSC HPSS.. after 2 years?
    # Waiting for PR #62 to be merged (transfer_controller)

    # TODO: Ingest file path in SciCat
    # Waiting for PR #62 to be merged (scicat_controller)

    # logger.info(f"Step 3: Ingesting {file_path} into SciCat")

    # # Build beegfs path for SciCat ingestion
    # # Get relative path from source root
    # try:
    #     rel_path = str(Path(file_path).relative_to(config.data733_raw.root_path))
    # except ValueError:
    #     # Already a relative path
    #     rel_path = file_path.lstrip("/")

    # # Build full beegfs path
    # beegfs_path = "/global/" + config.beegfs931.root_path.strip("/") + "/" + rel_path

    # logger.info(f"Beegfs path: {beegfs_path}")
    # try:
    #     scicat_ingest_flow(file_path=beegfs_path, ingester_spec="als931_ingester")
    #     logger.info("Step 3 complete: SciCat ingest successful")
    # except Exception as e:
    #     logger.error(f"SciCat ingest failed with {e}")


@flow(name="move_931_flight_check", flow_run_name="move_931_flight_check-{file_path}")
def move_931_flight_check(
    file_path: str = "globus_test/test.txt",
):
    """Please keep your arms and legs inside the vehicle at all times."""
    logger = get_run_logger()
    logger.info("931 flight check: testing transfer from data931 to NERSC CFS")

    config = Config931()

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    success = transfer_controller.copy(
        file_path=file_path,
        source=config.bl931_compute_dtn,
        destination=config.bl931_nersc_alsdev_raw
    )
    if success is True:
        logger.info("931 flight check: transfer successful")
    else:
        logger.error("931 flight check: transfer failed")
        raise RuntimeError("931 flight check: transfer failed")
