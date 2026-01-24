import logging
from pathlib import Path
import os
from typing import Optional

from prefect import flow, get_run_logger, task
from prefect.variables import Variable

from orchestration.flows.scicat.ingest import scicat_ingest_flow
from orchestration.flows.bl733.config import Config733
from orchestration.prune_controller import PruneMethod, get_prune_controller
from orchestration.transfer_controller import CopyMethod, get_transfer_controller

logger = logging.getLogger(__name__)

# Prune code is from the prune_controller in this PR: https://github.com/als-computing/splash_flows_globus/pulls
# Note: once the PR is merged, we can import prune_controller directly instead of copying the code here.


def get_common_parent_path(file_paths: list[str]) -> str:
    """
    Find the highest common parent directory for a list of file paths.

    :param file_paths: List of file paths
    :return: Common parent directory path
    """
    if not file_paths:
        raise ValueError("No file paths provided")

    if len(file_paths) == 1:
        # Single file - return its parent directory
        return str(Path(file_paths[0]).parent)

    # Use os.path.commonpath for multiple files
    return os.path.commonpath(file_paths)


@flow(name="new_733_file_flow", flow_run_name="process_new-{file_path[0]}")
def process_new_733_file_flow(
    file_path: list[str],
    config: Optional[Config733] = None
) -> None:
    """
    Flow to process a new file at BL 7.3.3
    1. Copy the file from the data733 to NERSC CFS. Ingest file path in SciCat.
    2. Schedule pruning from data733. 6 months from now.
    3. Copy the file from NERSC CFS to NERSC HPSS. Ingest file path in SciCat.
    4. Schedule pruning from NERSC CFS.

    :param file_paths: Paths to the new files to be processed.
    :param config: Configuration settings for processing.
    :return: None
    """
    process_new_733_file_task(
        file_path=file_path,
        config=config
    )


@task(name="new_733_file_task")
def process_new_733_file_task(
    file_path: list[str],
    config: Optional[Config733] = None
) -> None:
    """
    Task to process new data at BL 7.3.3
    1. Copy the data from data733 to beegfs (our common staging area).
    2. Copy the file from the data733 to NERSC CFS.
    3. Ingest the data from beegfs into SciCat.
    4. Schedule pruning from data733 for 6 months from now.
    5. Archive the file from NERSC CFS to NERSC HPSS at some point in the future.

    :param file_paths: Path(s) to the new file(s) to be processed.
    :param config: Configuration settings for processing.
    """
    logger = get_run_logger()

    if not file_path:
        logger.error("No file_paths provided")
        raise ValueError("No file_paths provided")

    file_paths = file_path
    if not isinstance(file_path, list):
        file_paths = [file_path]

    logger.info(f"Processing new 733 files: {file_paths}")

    if not config:
        logger.info("No config provided, creating default Config733")
        config = Config733()

    common_path = get_common_parent_path(file_paths)
    logger.info(f"Common parent path: {common_path}")

    logger.info("Initializing Globus transfer controller")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    logger.info(f"Step 1: Copying {common_path} from data733 to beegfs ({config.beegfs733.name})")

    transfer_controller.copy(
        file_path=common_path,
        source=config.data733_raw,
        destination=config.beegfs733
    )
    logger.info("Step 1 complete: File copied to beegfs")

    logger.info(f"Step 2: Copying {common_path} from data733 to NERSC CFS ({config.nersc733_alsdev_raw.name})")
    transfer_controller.copy(
        file_path=common_path,
        source=config.data733_raw,
        destination=config.nersc733_alsdev_raw
    )

    logger.info("Step 2 complete: File copied to NERSC CFS")

    logger.info(f"Step 3: Ingesting {len(file_paths)} into SciCat")

    # Build beegfs paths for SciCat ingestion
    beegfs_file_paths = []
    for fp in file_paths:
        # Get relative path from source root
        try:
            rel_path = str(Path(fp).relative_to(config.data733_raw.root_path))
        except ValueError:
            # Already a relative path
            rel_path = fp.lstrip("/")

        # Build full beegfs path
        beegfs_path = "/global/" + config.beegfs733.root_path.strip("/") + "/" + rel_path
        beegfs_file_paths.append(beegfs_path)

    logger.info(f"Beegfs paths: {beegfs_file_paths}")
    try:
        scicat_ingest_flow(file_paths=beegfs_file_paths, ingester_spec="als733_saxs")
        logger.info("Step 3 complete: SciCat ingest successful")
    except Exception as e:
        logger.error(f"SciCat ingest failed with {e}")

    logger.info("Step 4: Scheduling pruning from data733")

    # Waiting for PR #62 to be merged (prune_controller)
    prune_controller = get_prune_controller(
        prune_type=PruneMethod.GLOBUS,
        config=config
    )

    bl733_settings = Variable.get("bl733-settings", _sync=True)
    for file_path in file_paths:
        prune_controller.prune(
            file_path=file_path,
            source_endpoint=config.data733_raw,
            check_endpoint=config.nersc733_alsdev_raw,
            days_from_now=bl733_settings["delete_data733_files_after_days"]  # 180 days
        )

    logger.info("Step 4 complete: Pruning scheduled")
    logger.info(f"All steps complete for {len(file_paths)} file(s)")

    # TODO: Copy the file from NERSC CFS to NERSC HPSS.. after 2 years?
    # Waiting for PR #62 to be merged (transfer_controller)


@flow(name="move_733_flight_check", flow_run_name="move_733_flight_check-{file_path}")
def move_733_flight_check(
    file_path: str = "test_directory/test.txt",
):
    """Please keep your arms and legs inside the vehicle at all times.
    :param file_path: Path to the test file to be transferred.
    :raises RuntimeError: If the transfer fails.
    :return: None
    """
    logger = get_run_logger()
    logger.info("733 flight check: testing transfer from data733 to NERSC CFS")

    config = Config733()

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    success = transfer_controller.copy(
        file_path=file_path,
        source=config.data733_raw,
        destination=config.nersc733_alsdev_raw
    )
    if success is True:
        logger.info("733 flight check: transfer successful")
    else:
        logger.error("733 flight check: transfer failed")
        raise RuntimeError("733 flight check: transfer failed")
