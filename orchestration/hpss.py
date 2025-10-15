"""
HPSS Module - Handling transfers to and from NERSC's High Performance Storage System (HPSS).

This module provides functionality for transferring data between NERSC's Community File System (CFS)
and the High Performance Storage System (HPSS) tape archive. It includes:

1. Prefect flows for initiating transfers in both directions
2. Transfer controllers for CFS to HPSS and HPSS to CFS operations
3. HPSS-specific pruning controller for managing data lifecycle
4. Slurm job scripts for executing HPSS operations via SFAPI

The module follows tape-safe practices as recommended in NERSC documentation:
https://docs.nersc.gov/filesystems/HPSS-best-practices/
"""

import datetime
import logging
import os
from pathlib import Path
import re
import time
from typing import Dict, List, Optional, Union

from prefect import flow
from sfapi_client import Client
from sfapi_client.compute import Machine

from orchestration.config import BeamlineConfig
from orchestration.prefect import schedule_prefect_flow
from orchestration.prune_controller import get_prune_controller, PruneController, PruneMethod
from orchestration.transfer_controller import get_transfer_controller, CopyMethod, TransferController
from orchestration.transfer_endpoints import FileSystemEndpoint, HPSSEndpoint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


"""
HPSS SLURM Template Loader

Provides utilities for loading SLURM job script templates for HPSS operations.
"""

# Directory containing SLURM template files


def load_slurm_job(job_name: str, **variables) -> str:
    """
    Load and render a SLURM template with variable substitution.

    Args:
        job_name: Name of the job (without .slurm extension)
        **variables: Variables to substitute using .format()

    Returns:
        str: The rendered SLURM script
    """
    # Read slurm files from orchestration/slurm/
    TEMPLATES_DIR = Path(__file__).parent / "slurm"
    slurm_path = TEMPLATES_DIR / f"{job_name}.slurm"
    job = slurm_path.read_text()
    return job.format(**variables)


# ---------------------------------
# HPSS Prefect Flows
# ---------------------------------

@flow(name="cfs_to_hpss_flow")
def cfs_to_hpss_flow(
    file_path: Union[str, List[str]] = None,
    source: FileSystemEndpoint = None,
    destination: HPSSEndpoint = None,
    config: BeamlineConfig = None
) -> bool:
    """
    Prefect flow for transferring data from CFS to HPSS tape archive.

    This flow handles the transfer of files or directories from NERSC's Community
    File System (CFS) to the High Performance Storage System (HPSS) tape archive.
    For directories, files are bundled into tar archives based on time periods.

    Args:
        file_path (Union[str, List[str]]): A single file path or a list of file paths to transfer
        source (FileSystemEndpoint): The CFS source endpoint
        destination (HPSSEndpoint): The HPSS destination endpoint
        config (BeamlineConfig): The beamline configuration containing endpoints and credentials

    Returns:
        bool: True if all transfers succeeded, False otherwise
    """
    logger.info("Running cfs_to_hpss_flow")

    if not file_path:
        logger.error("No file path provided for CFS to HPSS transfer")
        return False

    if not source or not destination:
        logger.error("Source or destination endpoint not provided for CFS to HPSS transfer")
        return False

    if not config:
        logger.error("No configuration provided for CFS to HPSS transfer")
        return False

    # Log detailed information about the transfer
    if isinstance(file_path, list):
        logger.info(f"Transferring {len(file_path)} files/directories from {source.name} to {destination.name}")
        for path in file_path:
            logger.debug(f"  - {path}")
    else:
        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

    # Configure the transfer controller for CFS to HPSS
    logger.info("Configuring transfer controller for CFS_TO_HPSS.")
    try:
        transfer_controller = get_transfer_controller(
            transfer_type=CopyMethod.CFS_TO_HPSS,
            config=config
        )
    except Exception as e:
        logger.error(f"Failed to initialize CFS to HPSS transfer controller: {str(e)}", exc_info=True)
        return False

    logger.info("CFSToHPSSTransferController selected. Initiating transfer for all file paths.")

    try:
        result = transfer_controller.copy(
            file_path=file_path,
            source=source,
            destination=destination
        )
        if result:
            logger.info("CFS to HPSS transfer completed successfully")
        else:
            logger.error("CFS to HPSS transfer failed")
        return result
    except Exception as e:
        logger.error(f"Error during CFS to HPSS transfer: {str(e)}", exc_info=True)
        return False


@flow(name="hpss_to_cfs_flow")
def hpss_to_cfs_flow(
    file_path: str = None,
    source: HPSSEndpoint = None,
    destination: FileSystemEndpoint = None,
    files_to_extract: Optional[List[str]] = None,
    config: BeamlineConfig = None
) -> bool:
    """
    Prefect flow for retrieving data from HPSS tape archive to CFS.

    This flow handles the retrieval of files or tar archives from NERSC's High
    Performance Storage System (HPSS) to the Community File System (CFS).
    For tar archives, you can optionally specify specific files to extract.

    Args:
        file_path (str): The path of the file or tar archive on HPSS
        source (HPSSEndpoint): The HPSS source endpoint
        destination (FileSystemEndpoint): The CFS destination endpoint
        files_to_extract (Optional[List[str]]): Specific files to extract from the tar archive
        config (BeamlineConfig): The beamline configuration containing endpoints and credentials

    Returns:
        bool: True if the transfer succeeded, False otherwise
    """
    logger.info("Running hpss_to_cfs_flow")

    if not file_path:
        logger.error("No file path provided for HPSS to CFS transfer")
        return False

    if not source or not destination:
        logger.error("Source or destination endpoint not provided for HPSS to CFS transfer")
        return False

    if not config:
        logger.error("No configuration provided for HPSS to CFS transfer")
        return False

    logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

    # Log detailed information about the transfer
    if files_to_extract:
        logger.info(f"Extracting {len(files_to_extract)} specific files from tar archive:")
        for file in files_to_extract:
            logger.debug(f"  - {file}")

    # Configure transfer controller for HPSS_TO_CFS
    logger.info("Configuring transfer controller for HPSS_TO_CFS.")
    try:
        transfer_controller = get_transfer_controller(
            transfer_type=CopyMethod.HPSS_TO_CFS,
            config=config
        )
    except Exception as e:
        logger.error(f"Failed to initialize HPSS to CFS transfer controller: {str(e)}", exc_info=True)
        return False

    logger.info("HPSSToCFSTransferController selected. Initiating transfer for all file paths.")

    # Initiate transfer
    logger.info("HPSSToCFSTransferController selected. Initiating transfer.")
    try:
        result = transfer_controller.copy(
            file_path=file_path,
            source=source,
            destination=destination,
            files_to_extract=files_to_extract,
        )

        if result:
            logger.info("HPSS to CFS transfer completed successfully")
        else:
            logger.error("HPSS to CFS transfer failed")

        return result
    except Exception as e:
        logger.error(f"Error during HPSS to CFS transfer: {str(e)}", exc_info=True)
        return False


# ----------------------------------
# HPSS ls Function
# ----------------------------------

def list_hpss_slurm(
    client: Client,
    endpoint: HPSSEndpoint,
    remote_path: str,
    recursive: bool = True
) -> str:
    """
    Schedule and run a Slurm job on Perlmutter to list contents on HPSS,
    then read back the result from the Slurm output file.

    If `remote_path` ends with '.tar', uses `htar -tvf` to list tar members;
    otherwise uses `hsi ls [-R]` to list directory contents.

    Args:
        client (Client): SFAPI client with compute permissions.
        endpoint (HPSSEndpoint): HPSS endpoint (knows root_path & URI).
        remote_path (str): Path relative to endpoint.root_path on HPSS.
        recursive (bool): Recursively list directories (ignored for .tar).

    Returns:
        .

    Raises:
        RuntimeError: If job submission or output retrieval fails.
    """
    logger = logging.getLogger(__name__)
    # Build logs directory on CFS
    beamline_id = remote_path.split("/")[0]
    logs_dir = f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{beamline_id}/ls"

    # Sanitize remote_path for filenames
    safe_name = re.sub(r'[^A-Za-z0-9_]', '_', remote_path)
    job_name = f"ls_hpss_{safe_name}"
    out_pattern = f"{logs_dir}/{safe_name}_%j.out"
    err_pattern = f"{logs_dir}/{safe_name}_%j.err"

    full_hpss = endpoint.full_path(remote_path)

    # for tar: list contents & then show .idx; otherwise do an hsi ls
    if remote_path.lower().endswith(".tar"):
        cmd = (
            f'echo "[LOG] TAR contents:" && htar -tvf "{full_hpss}"'
        )
    else:
        ls_flag = "-R" if recursive else ""
        cmd = f'hsi ls {ls_flag} "{full_hpss}"'

    job_script = load_slurm_job(
        "ls_hpss",
        job_name=job_name,
        out_pattern=out_pattern,
        err_pattern=err_pattern,
        full_hpss=full_hpss,
        cmd=cmd
    )

    # submit & wait
    perlmutter = client.compute(Machine.perlmutter)
    job = perlmutter.submit_job(job_script)
    try:
        job.update()
    except Exception:
        logger.debug("Initial job.update() failed, proceeding to wait")
    job.complete()

    # print where you can find the actual log
    out_file = out_pattern.replace("%j", str(job.jobid))
    print(f"HPSS listing complete. See Slurm output at: {out_file}")

    return out_file

# ----------------------------------
# HPSS Prune Controller
# ----------------------------------


class HPSSPruneController(PruneController[HPSSEndpoint]):
    """
    Controller for pruning files from HPSS tape archive.

    This controller uses SFAPI, Slurm, and hsi to prune data from HPSS at NERSC.
    It requires the source to be an HPSSEndpoint and the optional destination to
    be a FileSystemEndpoint. It uses "hsi rm" to prune files from HPSS.

    Args:
        client (Client): The SFAPI client for submitting jobs to NERSC
        config (BeamlineConfig): Configuration object containing endpoints and credentials
    """
    def __init__(
        self,
        client: Client,
        config: BeamlineConfig,
    ) -> None:
        """
        Initialize the HPSS prune controller.

        Args:
            client (Client): The SFAPI client for submitting jobs to NERSC
            config (BeamlineConfig): Configuration object containing endpoints and credentials
        """
        super().__init__(config)
        self.client = client
        logger.debug(f"Initialized HPSSPruneController with client for beamline {config.beamline_id}")

    def prune(
        self,
        file_path: str = None,
        source_endpoint: HPSSEndpoint = None,
        check_endpoint: Optional[FileSystemEndpoint] = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        """
        Prune (delete) data from HPSS tape archive.

        If days_from_now is 0, executes pruning immediately.
        Otherwise, schedules pruning for future execution using Prefect.

        Args:
            file_path (str): The path to the file or directory to prune on HPSS
            source_endpoint (HPSSEndpoint): The HPSS endpoint containing the data
            check_endpoint (Optional[FileSystemEndpoint]): If provided, verify data exists here before pruning
            days_from_now (datetime.timedelta): Delay before pruning; if 0, prune immediately

        Returns:
            bool: True if pruning was successful or scheduled successfully, False otherwise
        """
        if not file_path:
            logger.error("No file_path provided for HPSS pruning operation")
            return False

        if not source_endpoint:
            logger.error("No source_endpoint provided for HPSS pruning operation")
            return False

        flow_name = f"prune_from_{source_endpoint.name}"
        logger.info(f"Setting up pruning of '{file_path}' from HPSS endpoint '{source_endpoint.name}'")

        # If days_from_now is 0, prune immediately
        if days_from_now.total_seconds() == 0:
            self._prune_hpss_endpoint(
                self,
                relative_path=file_path,
                source_endpoint=source_endpoint,
                check_endpoint=check_endpoint,
            )
        # Otherwise, schedule pruning for future execution
        else:
            logger.info(f"Scheduling pruning of '{file_path}' from '{source_endpoint.name}' "
                        f"in {days_from_now.total_seconds()/86400:.1f} days")

            try:
                schedule_prefect_flow(
                    deployment_name="prune_hpss_endpoint/prune_hpss_endpoint",
                    flow_run_name=flow_name,
                    parameters={
                        "relative_path": file_path,
                        "source_endpoint": source_endpoint,
                        "check_endpoint": check_endpoint,
                        "config": self.config
                    },
                    duration_from_now=days_from_now
                )
                logger.info(f"Successfully scheduled HPSS pruning task in {days_from_now.total_seconds()/86400:.1f} days")
                return True
            except Exception as e:
                logger.error(f"Failed to schedule HPSS pruning task: {str(e)}", exc_info=True)
                return False

    @flow(name="prune_hpss_endpoint")
    def _prune_hpss_endpoint(
        self,
        relative_path: str,
        source_endpoint: HPSSEndpoint,
        check_endpoint: Optional[Union[FileSystemEndpoint, None]] = None,
    ) -> None:
        """
        Prefect flow that performs the actual HPSS pruning operation.

        Args:
            relative_path (str): The HPSS path of the file or directory to prune
            source_endpoint (HPSSEndpoint): The HPSS endpoint to prune from
            check_endpoint (Optional[FileSystemEndpoint]): If provided, verify data exists here before pruning
        """
        logger.info("Pruning files from HPSS")
        logger.info(f"Pruning {relative_path} from source endpoint: {source_endpoint.name}")

        beamline_id = self.config.beamline_id
        logs_path = f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{beamline_id}"
        job_script = load_slurm_job(
            "prune_hpss",
            relative_path=relative_path,
            logs_path=logs_path,
            source_endpoint=source_endpoint
        )

        try:
            logger.info("Submitting HPSS transfer job to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()  # Wait until the job completes.
            logger.info("Transfer job completed successfully.")
            return True

        except Exception as e:
            logger.error(f"Error during job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))
            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.perlmutter.job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Transfer job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                return False


# ----------------------------------
# HPSS Transfer Controllers
# ----------------------------------

class CFSToHPSSTransferController(TransferController[HPSSEndpoint]):
    """
    Use SFAPI, Slurm, hsi, and htar to move data from CFS to HPSS at NERSC.

    This controller requires the source to be a FileSystemEndpoint on CFS and the
    destination to be an HPSSEndpoint. For a single file, the transfer is done using hsi (via hsi cput).
    For a directory, the transfer is performed with htar. In this updated version, if the source is a
    directory then the files are bundled into tar archives based on their modification dates as follows:
      - Files with modification dates between Jan 1 and Jul 15 (inclusive) are grouped together
        (Cycle 1 for that year).
      - Files with modification dates between Jul 16 and Dec 31 are grouped together (Cycle 2).

    Within each group, if the total size exceeds 2 TB the files are partitioned into multiple tar bundles.
    The resulting naming convention on HPSS is:

        /home/a/alsdev/data_mover/[beamline]/raw/[proposal_name]/
           [proposal_name]_[year]-[cycle].tar
           [proposal_name]_[year]-[cycle]_part0.tar
           [proposal_name]_[year]-[cycle]_part1.tar
           ...

    At the end of the SLURM script, the directory tree for both the source (CFS) and destination (HPSS)
    is echoed for logging purposes.
    """

    def __init__(
        self,
        client: Client,
        config: BeamlineConfig
    ) -> None:
        super().__init__(config)
        self.client = client

    def list_hpss(
        self,
        endpoint: HPSSEndpoint,
        remote_path: str,
        recursive: bool = True
    ) -> List[str]:
        """
        Schedule and run a Slurm job to list contents on HPSS.

        Args:
            endpoint (HPSSEndpoint): HPSS endpoint (knows root_path & URI).
            remote_path (str): Path under endpoint.root_path to list.
            recursive (bool): If True, pass -R to `hsi ls` (ignored for tar).

        Returns:
            List[str]: Lines of output from the listing command.
        """
        return list_hpss_slurm(
            client=self.client,
            endpoint=endpoint,
            remote_path=remote_path,
            recursive=recursive
        )

    def copy(
        self,
        file_path: str = None,
        source: FileSystemEndpoint = None,
        destination: HPSSEndpoint = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        """
        Copy a file or directory from a CFS source endpoint to an HPSS destination endpoint.

        Args:
            file_path (str): Path to the file or directory on CFS.
            source (FileSystemEndpoint): The CFS source endpoint.
            destination (HPSSEndpoint): The HPSS destination endpoint.

        Returns:
            bool: True if the transfer job completes successfully, False otherwise.
        """
        logger.info("Transferring data from CFS to HPSS")
        if not file_path or not source or not destination:
            logger.error("Missing required parameters for CFSToHPSSTransferController.")
            return False

        # Compute the full path on CFS for the file/directory.
        full_cfs_path = source.full_path(file_path)
        # Get the beamline_id from the configuration.
        beamline_id = self.config.beamline_id
        # Build the HPSS destination root path using the convention: [destination.root_path]/[beamline_id]/raw
        hpss_root_path = f"{destination.root_path.rstrip('/')}/{beamline_id}/raw"

        # Determine the proposal (project) folder name from the file_path.
        path = Path(file_path)
        proposal_name = path.parent.name
        if not proposal_name or proposal_name == ".":  # if file_path is in the root directory
            proposal_name = file_path

        logger.info(f"Proposal name derived from file path: {proposal_name}")

        logs_path = f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{beamline_id}"
        logger.info(f"Logs will be saved to: {logs_path}")
        # Build the SLURM job script with detailed inline comments for clarity.

        job_script = load_slurm_job(
            "cfs_to_hpss",
            full_cfs_path=full_cfs_path,
            hpss_root_path=hpss_root_path,
            proposal_name=proposal_name,
            logs_path=logs_path
        )

        try:
            logger.info("Submitting HPSS transfer job to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()  # Wait until the job completes.
            logger.info("Transfer job completed successfully.")
            return True

        except Exception as e:
            logger.error(f"Error during job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))
            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.perlmutter.job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Transfer job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                return False


class HPSSToCFSTransferController(TransferController[HPSSEndpoint]):
    """
    Use SFAPI, Slurm, hsi and htar to move data between HPSS and CFS at NERSC.

    This controller retrieves data from an HPSS source endpoint and places it on a CFS destination endpoint.
    It supports the following modes:
      - "single": Single file retrieval via hsi get.
      - "tar": Full tar archive extraction via htar -xvf.
      - "partial": Partial extraction from a tar archive: if a list of files is provided (via files_to_extract),
        only the specified files will be extracted.

    A single SLURM job script is generated that branches based on the mode.
    """

    def __init__(
        self,
        client: Client,
        config: BeamlineConfig
    ) -> None:
        super().__init__(config)
        self.client = client

    def list_hpss(
        self,
        endpoint: HPSSEndpoint,
        remote_path: str,
        recursive: bool = True
    ) -> List[str]:
        """
        Schedule and run a Slurm job to list contents on HPSS.

        Args:
            endpoint (HPSSEndpoint): HPSS endpoint (knows root_path & URI).
            remote_path (str): Path under endpoint.root_path to list.
            recursive (bool): If True, pass -R to `hsi ls` (ignored for tar).

        Returns:
            List[str]: Lines of output from the listing command.
        """
        return list_hpss_slurm(
            client=self.client,
            endpoint=endpoint,
            remote_path=remote_path,
            recursive=recursive
        )

    def copy(
        self,
        file_path: str = None,
        source: HPSSEndpoint = None,
        destination: FileSystemEndpoint = None,
        files_to_extract: Optional[List[str]] = None,
    ) -> bool:
        """
        Copy a file from an HPSS source endpoint to a CFS destination endpoint.

        Args:
            file_path (str): Path to the file or tar archive on HPSS.
            source (HPSSEndpoint): The HPSS source endpoint.
            destination (FileSystemEndpoint): The CFS destination endpoint.
            files_to_extract (List[str], optional): Specific files to extract from the tar archive.
                If provided (and file_path ends with '.tar'), only these files will be extracted.
                If not provided, the entire tar archive will be extracted.
                If file_path is a single file, this parameter is ignored.

        Returns:
            bool: True if the transfer job completes successfully, False otherwise.
        """
        logger.info("Starting HPSS to CFS transfer.")
        if not file_path or not source or not destination:
            logger.error("Missing required parameters: file_path, source, or destination.")
            return False

        # Sanitize the file_path for the log file names.
        # Build a small ord→char map
        translate_dict: Dict[int, str] = {
            ord("/"): "_",      # always replace forward slash
            ord(" "): "_",      # replace spaces
            ord(os.sep): "_",   # replace primary separator
            ord("\\"): "_"      # In case of Windows
        }

        # One-pass replacement
        sanitized_path = file_path.translate(translate_dict)

        # Compute the full HPSS path from the source endpoint.
        hpss_path = source.full_path(file_path)
        dest_root = destination.root_path

        # Get the beamline_id from the configuration.
        beamline_id = self.config.beamline_id

        logs_path = f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{beamline_id}"

        # If files_to_extract is provided, join them as a space‐separated string.
        files_to_extract_str = " ".join(files_to_extract) if files_to_extract else ""

        # The following SLURM script contains all logic to decide the transfer mode.
        # It determines:
        #   - if HPSS_PATH ends with .tar, then if FILES_TO_EXTRACT is nonempty, MODE becomes "partial",
        #     else MODE is "tar".
        #   - Otherwise, MODE is "single" and hsi get is used.

        job_script = load_slurm_job(
            job_name="hpss_to_cfs",
            logs_path=logs_path,
            sanitized_path=sanitized_path,
            hpss_path=hpss_path,
            dest_root=dest_root,
            files_to_extract_str=files_to_extract_str
        )

        logger.info("Submitting HPSS to CFS transfer job to Perlmutter.")
        try:
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()  # Wait until the job completes.
            logger.info("HPSS to CFS transfer job completed successfully.")
            return True

        except Exception as e:
            logger.error(f"Error during job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))
            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.perlmutter.job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("HPSS to CFS transfer job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                return False


if __name__ == "__main__":
    TEST_HPSS_PRUNE = False
    TEST_CFS_TO_HPSS = False
    TEST_HPSS_TO_CFS = False
    TEST_HPSS_LS = True

    # ------------------------------------------------------
    # Test pruning from HPSS
    # ------------------------------------------------------
    if TEST_HPSS_PRUNE:
        from orchestration.flows.bl832.config import Config832
        config = Config832()
        file_name = "8.3.2/raw/ALS-11193_nbalsara/ALS-11193_nbalsara_2022-2.tar"
        source = HPSSEndpoint(
            name="HPSS",
            root_path=config.hpss_alsdev["root_path"],
            uri=config.hpss_alsdev["uri"]
        )

        days_from_now = datetime.timedelta(days=0)  # Prune immediately

        prune_controller = get_prune_controller(
            prune_type=PruneMethod.HPSS,
            config=config
        )
        prune_controller.prune(
            file_path=f"{file_name}",
            source_endpoint=source,
            check_endpoint=None,
            days_from_now=days_from_now
        )
    # ------------------------------------------------------
    # Test transfer from CFS to HPSS
    # ------------------------------------------------------
    if TEST_CFS_TO_HPSS:
        from orchestration.flows.bl832.config import Config832
        config = Config832()
        project_name = "ALS-11193_nbalsara"
        source = FileSystemEndpoint(
            name="CFS",
            root_path="/global/cfs/cdirs/als/data_mover/8.3.2/raw/",
            uri="nersc.gov"
        )
        destination = HPSSEndpoint(
            name="HPSS",
            root_path=config.hpss_alsdev["root_path"],
            uri=config.hpss_alsdev["uri"]
        )
        cfs_to_hpss_flow(
            file_path=f"{project_name}",
            source=source,
            destination=destination,
            config=config
        )

    # ------------------------------------------------------
    # Test transfer from HPSS to CFS
    # ------------------------------------------------------
    if TEST_HPSS_TO_CFS:
        from orchestration.flows.bl832.config import Config832
        config = Config832()
        relative_file_path = f"{config.beamline_id}/raw/ALS-11193_nbalsara/ALS-11193_nbalsara_2022-2.tar"
        source = HPSSEndpoint(
            name="HPSS",
            root_path=config.hpss_alsdev["root_path"],  # root_path: /home/a/alsdev/data_mover
            uri=config.hpss_alsdev["uri"]
        )
        destination = FileSystemEndpoint(
            name="CFS",
            root_path="/global/cfs/cdirs/als/data_mover/8.3.2/retrieved_from_tape",
            uri="nersc.gov"
        )

        files_to_extract = [
            "20221109_012020_MSB_Book1_Proj33_Cell5_2pFEC_LiR2_6C_Rest3.h5",
            "20221012_172023_DTH_100722_LiT_r01_cell3_10x_0_19_CP2.h5",
        ]

        hpss_to_cfs_flow(
            file_path=f"{relative_file_path}",
            source=source,
            destination=destination,
            files_to_extract=files_to_extract,
            config=config
        )

    # ------------------------------------------------------
    # Test listing HPSS files
    # ------------------------------------------------------
    if TEST_HPSS_LS:
        from orchestration.flows.bl832.config import Config832

        # Build client, config, endpoint
        config = Config832()
        endpoint = HPSSEndpoint(
            name="HPSS",
            root_path=config.hpss_alsdev["root_path"],
            uri=config.hpss_alsdev["uri"]
        )

        # Instantiate controller
        transfer_controller = get_transfer_controller(
            transfer_type=CopyMethod.CFS_TO_HPSS,
            config=config
        )

        # Directory listing
        project_path = f"{config.beamline_id}/raw/BLS-00564_dyparkinson"
        logger.info("Controller-based directory listing on HPSS:")
        output_file = transfer_controller.list_hpss(
            endpoint=endpoint,
            remote_path=project_path,
            recursive=True
        )

        # TAR archive listing
        archive_name = project_path.split("/")[-1]
        tar_path = f"{project_path}/{archive_name}_2023-1.tar"
        logger.info("Controller-based tar archive listing on HPSS:")
        output_file = transfer_controller.list_hpss(
            endpoint=endpoint,
            remote_path=tar_path,
            recursive=False
        )
