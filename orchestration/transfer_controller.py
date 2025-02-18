from abc import ABC, abstractmethod
from dotenv import load_dotenv
from enum import Enum
import datetime
import logging
import os
from pathlib import Path
import re
import time
from typing import Generic, List, Optional, TypeVar

import globus_sdk
from sfapi_client import Client
from sfapi_client.compute import Machine

# Import the generic Beamline configuration class.
from orchestration.config import BeamlineConfig
from orchestration.globus.transfer import GlobusEndpoint, start_transfer
from orchestration.prometheus_utils import PrometheusMetrics
from orchestration.transfer_endpoints import FileSystemEndpoint, HPSSEndpoint, TransferEndpoint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


Endpoint = TypeVar("Endpoint", bound=TransferEndpoint)


class TransferController(Generic[Endpoint], ABC):
    """
    Abstract class for transferring data.

    Args:
        ABC: Abstract Base Class
    """
    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        self.config = config

    @abstractmethod
    def copy(
        self,
        file_path: str = None,
        source: Endpoint = None,
        destination: Endpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint.

        Args:
            file_path (str): The path of the file to copy.
            source (Endpoint): The source endpoint.
            destination (Endpoint): The destination endpoint.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        pass


class GlobusTransferController(TransferController[GlobusEndpoint]):
    def __init__(
        self,
        config: BeamlineConfig,
        prometheus_metrics: Optional[PrometheusMetrics] = None
    ) -> None:
        super().__init__(config)
        self.prometheus_metrics = prometheus_metrics

    """
    Use Globus Transfer to move data between endpoints.

    Args:
        TransferController: Abstract class for transferring data.
    """

    def get_transfer_file_info(
        self,
        task_id: str,
        transfer_client: Optional[globus_sdk.TransferClient] = None
    ) -> Optional[dict]:
        """
        Get information about a completed transfer from the Globus API.

        Args:
            task_id (str): The Globus transfer task ID
            transfer_client (TransferClient, optional): TransferClient instance

        Returns:
            Optional[dict]: Task information including bytes_transferred, or None if unavailable
        """
        if transfer_client is None:
            transfer_client = self.config.tc

        try:
            task_info = transfer_client.get_task(task_id)
            task_dict = task_info.data

            if task_dict.get('status') == 'SUCCEEDED':
                bytes_transferred = task_dict.get('bytes_transferred', 0)
                bytes_checksummed = task_dict.get('bytes_checksummed', 0)
                files_transferred = task_dict.get('files_transferred', 0)
                effective_bytes_per_second = task_dict.get('effective_bytes_per_second', 0)
                return {
                    'bytes_transferred': bytes_transferred,
                    'bytes_checksummed': bytes_checksummed,
                    'files_transferred': files_transferred,
                    'effective_bytes_per_second': effective_bytes_per_second
                }

            return None

        except Exception as e:
            logger.error(f"Error getting transfer task info: {e}")
            return None

    def collect_and_push_metrics(
        self,
        start_time: float,
        end_time: float,
        file_path: str,
        source: GlobusEndpoint,
        destination: GlobusEndpoint,
        file_size: int,
        transfer_speed: float,
        success: bool
    ) -> None:
        """
        Collect transfer metrics and push them to Prometheus.

        Args:
            start_time (float): Transfer start time as UNIX timestamp.
            end_time (float): Transfer end time as UNIX timestamp.
            file_path (str): The path of the transferred file.
            source (GlobusEndpoint): The source endpoint.
            destination (GlobusEndpoint): The destination endpoint.
            file_size (int): Size of the transferred file in bytes.
            transfer_speed (float): Transfer speed (bytes/second) provided by Globus
            success (bool): Whether the transfer was successful.
        """
        try:
            # Get machine_name
            machine_name = destination.name

            # Convert UNIX timestamps to ISO format strings
            start_datetime = datetime.datetime.fromtimestamp(start_time, tz=datetime.timezone.utc)
            end_datetime = datetime.datetime.fromtimestamp(end_time, tz=datetime.timezone.utc)
            start_timestamp = start_datetime.isoformat()
            end_timestamp = end_datetime.isoformat()

            # Calculate duration in seconds
            duration_seconds = end_time - start_time

            # Calculate transfer speed (bytes per second)
            # transfer_speed = file_size / duration_seconds if duration_seconds > 0 and file_size > 0 else 0

            # Prepare metrics dictionary
            metrics = {
                "timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "local_path": os.path.join(source.root_path, file_path),
                "remote_path": os.path.join(destination.root_path, file_path),
                "bytes_transferred": file_size,
                "duration_seconds": duration_seconds,
                "transfer_speed": transfer_speed,
                "status": "success" if success else "failed",
                "machine": machine_name
            }

            # Push metrics to Prometheus
            self.prometheus_metrics.push_metrics_to_prometheus(metrics, logger)

        except Exception as e:
            logger.error(f"Error collecting or pushing metrics: {e}")

    def copy(
        self,
        file_path: str = None,
        source: GlobusEndpoint = None,
        destination: GlobusEndpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint.

        Args:
            file_path (str): The path of the file to copy.
            source (GlobusEndpoint): The source endpoint.
            destination (GlobusEndpoint): The destination endpoint.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """

        if not file_path:
            logger.error("No file_path provided")
            return False

        if not source or not destination:
            logger.error("Source or destination endpoint not provided")
            return False

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        # Remove leading slash if present
        if file_path[0] == "/":
            file_path = file_path[1:]

        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")

        # Start the timer
        transfer_start_time = time.time()
        success = False
        task_id = None  # Initialize task_id here to prevent UnboundLocalError
        file_size = 0   # Initialize file_size here as well

        try:
            success, task_id = start_transfer(
                transfer_client=self.config.tc,
                source_endpoint=source,
                source_path=source_path,
                dest_endpoint=destination,
                dest_path=dest_path,
                max_wait_seconds=600,
                logger=logger,
            )

            if success:
                logger.info("Transfer completed successfully.")
            else:
                logger.error("Transfer failed.")

        except globus_sdk.services.transfer.errors.TransferAPIError as e:
            logger.error(f"Failed to submit transfer: {e}")

        finally:
            # Stop the timer and calculate the duration
            transfer_end_time = time.time()

            # Try to get transfer info from the completed task
            if task_id:
                transfer_info = self.get_transfer_file_info(task_id)
                if transfer_info:
                    file_size = transfer_info.get('bytes_transferred', 0)
                    transfer_speed = transfer_info.get('effective_bytes_per_second', 0)
                    logger.info(f"Globus Task Info: Transferred {file_size} bytes ")
                    logger.info(f"Globus Task Info: Effective speed: {transfer_speed} bytes/second")

            # Collect and push metrics if enabled
            if self.prometheus_metrics and file_size > 0:
                self.collect_and_push_metrics(
                    start_time=transfer_start_time,
                    end_time=transfer_end_time,
                    file_path=file_path,
                    source=source,
                    destination=destination,
                    file_size=file_size,
                    transfer_speed=transfer_speed,
                    success=success,
                )

            return success


class SimpleTransferController(TransferController[FileSystemEndpoint]):
    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        super().__init__(config)
    """
    Use a simple 'cp' command to move data within the same system.

    Args:
        TransferController: Abstract class for transferring data.
    """

    def copy(
        self,
        file_path: str = None,
        source: FileSystemEndpoint = None,
        destination: FileSystemEndpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint using the 'cp' command.

        Args:
            file_path (str): The path of the file to copy.
            source (FileSystemEndpoint): The source endpoint.
            destination (FileSystemEndpoint): The destination endpoint.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        if not file_path:
            logger.error("No file_path provided.")
            return False
        if not source or not destination:
            logger.error("Source or destination endpoint not provided.")
            return False

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        if file_path.startswith("/"):
            file_path = file_path[1:]

        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")

        # Start the timer
        start_time = time.time()

        try:
            result = os.system(f"cp -r '{source_path}' '{dest_path}'")
            if result == 0:
                logger.info("Transfer completed successfully.")
                return True
            else:
                logger.error(f"Transfer failed with exit code {result}.")
                return False
        except Exception as e:
            logger.error(f"Transfer failed: {e}")
            return False
        finally:
            # Stop the timer and calculate the duration
            elapsed_time = time.time() - start_time
            logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")


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

    def copy(
        self,
        file_path: str = None,
        source: FileSystemEndpoint = None,
        destination: HPSSEndpoint = None
    ) -> bool:
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

        logs_path = f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{beamline_id}"

        # Build the SLURM job script with detailed inline comments for clarity.
        job_script = rf"""#!/bin/bash
# ------------------------------------------------------------------
# SLURM Job Script for Transferring Data from CFS to HPSS
# This script will:
#   1. Define the source (CFS) and destination (HPSS) paths.
#   2. Create the destination directory on HPSS if it doesn't exist.
#   3. Determine if the source is a file or a directory.
#      - If a file, transfer it using 'hsi cput'.
#      - If a directory, group files by beam cycle and archive them.
#         * Cycle 1: Jan 1 - Jul 15
#         * Cycle 2: Jul 16 - Dec 31
#         * If a group exceeds 2 TB, it is partitioned into multiple tar archives.
#         * Archive names:
#              [proposal_name]_[year]-[cycle].tar
#              [proposal_name]_[year]-[cycle]_part0.tar, _part1.tar, etc.
#   4. Echo directory trees for both source and destination for logging.
# ------------------------------------------------------------------

#SBATCH -q xfer                           # Specify the SLURM queue to use.
#SBATCH -A als                            # Specify the account.
#SBATCH -C cron                           # Use the 'cron' constraint.
#SBATCH --time=12:00:00                   # Maximum runtime of 12 hours.
#SBATCH --job-name=transfer_to_HPSS_{file_path}  # Set a descriptive job name.
#SBATCH --output={logs_path}/{file_path}_%j.out       # Standard output log file.
#SBATCH --error={logs_path}/{file_path}_%j.err        # Standard error log file.
#SBATCH --licenses=SCRATCH                # Request the SCRATCH license.
#SBATCH --mem=20GB                        # Request #GB of memory. Default 2GB.

set -euo pipefail                        # Enable strict error checking.
echo "[LOG] Job started at: $(date)"

# ------------------------------------------------------------------
# Define source and destination variables.
# ------------------------------------------------------------------
echo "[LOG] Defining source and destination paths."

# SOURCE_PATH: Full path of the file or directory on CFS.
SOURCE_PATH="{full_cfs_path}"
echo "[LOG] SOURCE_PATH set to: $SOURCE_PATH"

# DEST_ROOT: Root destination on HPSS built from configuration.
DEST_ROOT="{hpss_root_path}"
echo "[LOG] DEST_ROOT set to: $DEST_ROOT"

# FOLDER_NAME: Proposal name (project folder) derived from the file path.
FOLDER_NAME="{proposal_name}"
echo "[LOG] FOLDER_NAME set to: $FOLDER_NAME"

# DEST_PATH: Final HPSS destination directory.
DEST_PATH="${{DEST_ROOT}}/${{FOLDER_NAME}}"
echo "[LOG] DEST_PATH set to: $DEST_PATH"

# ------------------------------------------------------------------
# Create destination directory on HPSS if it doesn't exist.
# ------------------------------------------------------------------
echo "[LOG] Checking if HPSS destination directory exists at $DEST_PATH."

if hsi -q "ls $DEST_PATH" >/dev/null 2>&1; then
    echo "[LOG] Destination directory $DEST_PATH already exists."
else
    echo "[LOG] Destination directory $DEST_PATH does not exist. Attempting to create it."
    if hsi -q "mkdir $DEST_PATH" >/dev/null 2>&1; then
        echo "[LOG] Created directory $DEST_PATH."
    else
        echo "[ERROR] Failed to create directory $DEST_PATH."
        exit 1
    fi
fi

# # ------------------------------------------------------------------
# # Transfer Logic: Check if SOURCE_PATH is a file or directory.
# # ------------------------------------------------------------------
# echo "[LOG] Determining type of SOURCE_PATH: $SOURCE_PATH"
# if [ -f "$SOURCE_PATH" ]; then
#     # Case: Single file detected.
#     echo "[LOG] Single file detected. Transferring via hsi cput."
#     FILE_NAME=$(basename "$SOURCE_PATH")
#     echo "[LOG] File name: $FILE_NAME"
#     hsi cput "$SOURCE_PATH" "$DEST_PATH/$FILE_NAME"
#     echo "[LOG] (Simulated) File transfer completed for $FILE_NAME."

# elif [ -d "$SOURCE_PATH" ]; then
#     # Case: Directory detected.
#     echo "[LOG] Directory detected. Initiating bundling process."
#     THRESHOLD=2199023255552  # 2 TB in bytes.
#     echo "[LOG] Threshold set to 2 TB (in bytes: $THRESHOLD)"

#     # ------------------------------------------------------------------
#     # Group files based on modification date.
#     # ------------------------------------------------------------------
#     echo "[LOG] Grouping files by modification date."
#     FILE_LIST=$(mktemp)
#     find "$SOURCE_PATH" -type f > "$FILE_LIST"
#     echo "[LOG] List of files stored in temporary file: $FILE_LIST"

#     # Declare associative arrays to hold grouped file paths and sizes.
#     declare -A group_files
#     declare -A group_sizes

#     while IFS= read -r file; do
#         mtime=$(stat -c %Y "$file")
#         year=$(date -d @"$mtime" +%Y)
#         month=$(date -d @"$mtime" +%m | sed 's/^0*//')
#         day=$(date -d @"$mtime" +%d | sed 's/^0*//')
#         # Determine cycle: Cycle 1 if month < 7 or (month == 7 and day <= 15), else Cycle 2.
#         if [ "$month" -lt 7 ] || {{ [ "$month" -eq 7 ] && [ "$day" -le 15 ]; }}; then
#             cycle=1
#         else
#             cycle=2
#         fi
#         key="${{year}}-${{cycle}}"
#         group_files["$key"]="${{group_files["$key"]:-}} $file"
#         fsize=$(stat -c %s "$file")
#         group_sizes["$key"]=$(( ${{group_sizes["$key"]:-0}} + fsize ))
#     done < "$FILE_LIST"
#     rm "$FILE_LIST"
#     echo "[LOG] Completed grouping files."

#     # Print the files in each group at the end
#     for key in "${{!group_files[@]}}"; do
#         echo "[LOG] Group $key contains files:"
#         for f in ${{group_files["$key"]}}; do
#             echo "    $f"
#         done
#     done

#     # ------------------------------------------------------------------
#     # Bundle files into tar archives.
#     # ------------------------------------------------------------------
#     for key in "${{!group_files[@]}}"; do
#         files=(${{group_files["$key"]}})
#         total_group_size=${{group_sizes["$key"]}}
#         echo "[LOG] Processing group $key with ${{#files[@]}} files; total size: $total_group_size bytes."

#         part=0
#         current_size=0
#         current_files=()
#         for f in "${{files[@]}}"; do
#             fsize=$(stat -c %s "$f")
#             # If adding this file exceeds the threshold, process the current bundle.
#             if (( current_size + fsize > THRESHOLD && ${{#current_files[@]}} > 0 )); then
#                 if [ $part -eq 0 ]; then
#                     tar_name="${{FOLDER_NAME}}_${{key}}.tar"
#                 else
#                     tar_name="${{FOLDER_NAME}}_${{key}}_part${{part}}.tar"
#                 fi
#                 echo "[LOG] Bundle reached threshold."
#                 echo "[LOG] Files in current bundle:"
#                 for file in "${{current_files[@]}}"; do
#                     echo "$file"
#                 done
#                 echo "[LOG] Creating archive $tar_name with ${{#current_files[@]}} files; bundle size: $current_size bytes."
#                 htar -cvf "${{DEST_PATH}}/${{tar_name}}" $(printf "%s " "${{current_files[@]}}")
#                 part=$((part+1))
#                 echo "[DEBUG] Resetting bundle variables."
#                 current_files=()
#                 current_size=0
#             fi
#             current_files+=("$f")
#             current_size=$(( current_size + fsize ))
#         done
#         if [ ${{#current_files[@]}} -gt 0 ]; then
#             if [ $part -eq 0 ]; then
#                 tar_name="${{FOLDER_NAME}}_${{key}}.tar"
#             else
#                 tar_name="${{FOLDER_NAME}}_${{key}}_part${{part}}.tar"
#             fi
#             echo "[LOG] Final bundle for group $key:"
#             echo "[LOG] Files in final bundle:"
#             for file in "${{current_files[@]}}"; do
#                 echo "$file"
#             done
#             echo "[LOG] Creating final archive $tar_name with ${{#current_files[@]}} files."
#             echo "[LOG] Bundle size: $current_size bytes."
#             htar -cvf "${{DEST_PATH}}/${{tar_name}}" $(printf "%s " "${{current_files[@]}}")
#         fi
#         echo "[LOG] Completed processing group $key."
#     done
# else
#     echo "[ERROR] $SOURCE_PATH is neither a file nor a directory. Exiting."
#     exit 1
# fi

# # ------------------------------------------------------------------
# # Logging: Display directory trees for both source and destination.
# # ------------------------------------------------------------------
# echo "[LOG] Listing Source (CFS) Tree:"
# if [ -d "$SOURCE_PATH" ]; then
#     find "$SOURCE_PATH" -print
# else
#     echo "[LOG] $SOURCE_PATH is a file."
# fi

# echo "[LOG] Listing Destination (HPSS) Tree:"
# hsi ls -R "$DEST_PATH" || echo "[ERROR] Failed to list HPSS tree at $DEST_PATH"

echo "[LOG] Job completed at: $(date)"
"""
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
    Use SFAPI to move data between HPSS and CFS at NERSC.

    This controller retrieves data from an HPSS source endpoint and places it on a CFS destination endpoint.
    It supports:
      - Single file retrieval via hsi get.
      - Full tar archive extraction via htar -xvf.
      - Partial extraction from a tar archive: if a list of files is provided (via files_to_extract),
        only the specified files will be extracted.

    A single SLURM job script is generated that branches based on the mode.
    Args:
        file_path (str): Path to the file or tar archive on HPSS.
        source (HPSSEndpoint): The HPSS source endpoint.
        destination (FileSystemEndpoint): The CFS destination endpoint.
        files_to_extract (List[str], optional): Specific files to extract from the tar archive.
            If provided (and file_path ends with '.tar'), only these files will be extracted.

    Returns:
        bool: True if the transfer job completes successfully, False otherwise.
    """

    def __init__(
        self,
        client: Client,
        config: BeamlineConfig
    ) -> None:
        super().__init__(config)
        self.client = client

    def copy(
        self,
        file_path: str = None,
        source: HPSSEndpoint = None,
        destination: FileSystemEndpoint = None,
        files_to_extract: Optional[List[str]] = None,
    ) -> bool:
        logger.info("Starting HPSS to CFS transfer.")
        if not file_path or not source or not destination:
            logger.error("Missing required parameters: file_path, source, or destination.")
            return False

        # Set the job name suffix based on the file name (or archive stem)
        job_name_suffix = Path(file_path).stem

        # Compute the full HPSS path from the source endpoint.
        hpss_path = source.full_path(file_path)
        dest_root = destination.root_path
        logs_path = "/global/cfs/cdirs/als/data_mover/hpss_transfer_logs"

        # If files_to_extract is provided, join them as a space‐separated string.
        files_to_extract_str = " ".join(files_to_extract) if files_to_extract else ""

        # The following SLURM script contains all logic to decide the transfer mode.
        # It determines:
        #   - if HPSS_PATH ends with .tar, then if FILES_TO_EXTRACT is nonempty, MODE becomes "partial",
        #     else MODE is "tar".
        #   - Otherwise, MODE is "single" and hsi get is used.
        job_script = fr"""#!/bin/bash
#SBATCH -q xfer
#SBATCH -A als
#SBATCH -C cron
#SBATCH --time=12:00:00
#SBATCH --job-name=transfer_from_HPSS_{job_name_suffix}
#SBATCH --output={logs_path}/%j.out
#SBATCH --error={logs_path}/%j.err
#SBATCH --licenses=SCRATCH
#SBATCH --mem=100GB

set -euo pipefail
date

# Environment variables provided by Python.
HPSS_PATH="{hpss_path}"
DEST_ROOT="{dest_root}"
FILES_TO_EXTRACT="{files_to_extract_str}"

# Determine the transfer mode in bash.
if [[ "$HPSS_PATH" =~ \.tar$ ]]; then
    if [ -n "${{FILES_TO_EXTRACT}}" ]; then
         MODE="partial"
    else
         MODE="tar"
    fi
else
    MODE="single"
fi

echo "Transfer mode: $MODE"
if [ "$MODE" = "single" ]; then
    echo "Single file detected. Using hsi get."
    mkdir -p "$DEST_ROOT"
    hsi get "$HPSS_PATH" "$DEST_ROOT/"
elif [ "$MODE" = "tar" ]; then
    echo "Tar archive detected. Extracting entire archive using htar."
    ARCHIVE_BASENAME=$(basename "$HPSS_PATH")
    ARCHIVE_NAME="${{ARCHIVE_BASENAME%.tar}}"
    DEST_PATH="${{DEST_ROOT}}/${{ARCHIVE_NAME}}"
    mkdir -p "$DEST_PATH"
    htar -xvf "$HPSS_PATH" -C "$DEST_PATH"
elif [ "$MODE" = "partial" ]; then
    echo "Partial extraction detected. Extracting selected files using htar."
    ARCHIVE_BASENAME=$(basename "$HPSS_PATH")
    ARCHIVE_NAME="${{ARCHIVE_BASENAME%.tar}}"
    DEST_PATH="${{DEST_ROOT}}/${{ARCHIVE_NAME}}"
    mkdir -p "$DEST_PATH"
    echo "Files to extract: $FILES_TO_EXTRACT"
    htar -xvf "$HPSS_PATH" -C "$DEST_PATH" $FILES_TO_EXTRACT
else
    echo "Error: Unknown mode: $MODE"
    exit 1
fi

date
"""
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


class CopyMethod(Enum):
    """
    Enum representing different transfer methods.
    Use enum names as strings to identify transfer methods, ensuring a standard set of values.
    """
    GLOBUS = "globus"
    SIMPLE = "simple"
    CFS_TO_HPSS = "cfs_to_hpss"
    HPSS_TO_CFS = "hpss_to_cfs"


def get_transfer_controller(
    transfer_type: CopyMethod,
    config: BeamlineConfig,
    prometheus_metrics: Optional[PrometheusMetrics] = None
) -> TransferController:
    """
    Get the appropriate transfer controller based on the transfer type.

    Args:
        transfer_type (str): The type of transfer to perform.
        config (Config832): The configuration object.

    Returns:
        TransferController: The transfer controller object.
    """
    if transfer_type == CopyMethod.GLOBUS:
        return GlobusTransferController(config, prometheus_metrics)
    elif transfer_type == CopyMethod.SIMPLE:
        return SimpleTransferController(config)
    elif transfer_type == CopyMethod.CFS_TO_HPSS:
        from orchestration.sfapi import create_sfapi_client
        return CFSToHPSSTransferController(
            client=create_sfapi_client(),
            config=config
        )
    elif transfer_type == CopyMethod.HPSS_TO_CFS:
        from orchestration.sfapi import create_sfapi_client
        return HPSSToCFSTransferController(
            client=create_sfapi_client(),
            config=config
        )
    else:
        raise ValueError(f"Invalid transfer type: {transfer_type}")


if __name__ == "__main__":
    from orchestration.flows.bl832.config import Config832
    config = Config832()
    transfer_type = CopyMethod.GLOBUS
    globus_transfer_controller = get_transfer_controller(transfer_type, config)
    globus_transfer_controller.copy(
        file_path="dabramov/test.txt",
        source=config.alcf832_raw,
        destination=config.alcf832_scratch
    )

    simple_transfer_controller = get_transfer_controller(CopyMethod.SIMPLE, config)
    success = simple_transfer_controller.copy(
        file_path="test.rtf",
        source=FileSystemEndpoint("source", "/Users/david/Documents/copy_test/test_source/"),
        destination=FileSystemEndpoint("destination", "/Users/david/Documents/copy_test/test_destination/")
    )

    if success:
        logger.info("Simple transfer succeeded.")
    else:
        logger.error("Simple transfer failed.")
