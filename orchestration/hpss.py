"""
This module contains HPSS related functions and classes.
"""
import datetime
import logging
from pathlib import Path
import re
import time
from typing import List, Optional, Union

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
    The CFS to HPSS flow.

    Parameters
    ----------
    file_path : Union[str, List[str]]
        A single file path or a list of file paths to transfer.
    source : FileSystemEndpoint
        The source endpoint.
    destination : HPSSEndpoints
        The destination endpoint.
    config : BeamlineConfig
        The beamline configuration.

    Returns
    -------
    bool
        True if all transfers succeeded, False otherwise.
    """

    logger.info("Running cfs_to_hpss_flow")
    logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

    logger.info("Configuring transfer controller for CFS_TO_HPSS.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.CFS_TO_HPSS,
        config=config
    )

    logger.info("CFSToHPSSTransferController selected. Initiating transfer for all file paths.")

    result = transfer_controller.copy(
        file_path=file_path,
        source=source,
        destination=destination
    )

    return result


@flow(name="hpss_to_cfs_flow")
def hpss_to_cfs_flow(
    file_path: str = None,
    source: HPSSEndpoint = None,
    destination: FileSystemEndpoint = None,
    files_to_extract: Optional[List[str]] = None,
    config: BeamlineConfig = None
) -> bool:
    """
    The HPSS to CFS flow.

    Parameters
    ----------
    file_path : str
        The path of the file to transfer.
    source_endpoint : HPSSEndpoint
        The source endpoint.
    destination_endpoint : FileSystemEndpoint
        The destination endpoint.
    """

    logger.info("Running hpss_to_cfs_flow")
    logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

    logger.info("Configuring transfer controller for HPSS_TO_CFS.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.HPSS_TO_CFS,
        config=config
    )

    logger.info("HPSSToCFSTransferController selected. Initiating transfer for all file paths.")

    result = transfer_controller.copy(
        file_path=file_path,
        source=source,
        destination=destination,
        files_to_extract=files_to_extract,
    )

    return result


# ----------------------------------
# HPSS Prune Controller
# ----------------------------------

class HPSSPruneController(PruneController[HPSSEndpoint]):
    """
    Use SFAPI, Slurm, and hsi to prune data from HPSS at NERSC.
    This controller requires the source to be an HPSSEndpoint and the
    optional destination to be a FileSystemEndpoint. It uses "hsi rm" to prune
    files from HPSS.
    """
    def __init__(
        self,
        client: Client,
        config: BeamlineConfig,
    ) -> None:
        super().__init__(config)
        self.client = client

    def prune(
        self,
        file_path: str = None,
        source_endpoint: HPSSEndpoint = None,
        check_endpoint: Optional[FileSystemEndpoint] = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        flow_name = f"prune_from_{source_endpoint.name}"
        logger.info(f"Running flow: {flow_name}")
        logger.info(f"Pruning {file_path} from source endpoint: {source_endpoint.name}")

        self._prune_hpss_endpoint(
            self,
            relative_path=file_path,
            source_endpoint=source_endpoint,
            check_endpoint=check_endpoint,
        )
        # Uncomment the following lines to schedule the flow with Prefect.
        # schedule_prefect_flow(
        #     deployment_name="prune_hpss_endpoint/prune_hpss_endpoint",
        #     flow_run_name=flow_name,
        #     parameters={
        #         "relative_path": file_path,
        #         "source_endpoint": source_endpoint,
        #         "check_endpoint": check_endpoint,
        #         "config": self.config
        #     },
        #     duration_from_now=days_from_now
        # )
        return True

    @flow(name="prune_hpss_endpoint")
    def _prune_hpss_endpoint(
        self,
        relative_path: str,
        source_endpoint: HPSSEndpoint,
        check_endpoint: Optional[Union[FileSystemEndpoint, None]] = None,
    ) -> None:
        """
        Prune files from HPSS.

        Args:
            relative_path (str): The HPSS path of the file or directory to prune.
            source_endpoint (HPSSEndpoint): The Globus source endpoint to prune from.
            check_endpoint (FileSystemEndpoint, optional): The Globus target endpoint to check. Defaults to None.
        """
        logger.info("Pruning files from HPSS")
        logger.info(f"Pruning {relative_path} from source endpoint: {source_endpoint.name}")

        beamline_id = self.config.beamline_id
        logs_path = f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{beamline_id}"

        job_script = rf"""#!/bin/bash
# ------------------------------------------------------------------
# SLURM Job Script for Pruning Data from HPSS
# ------------------------------------------------------------------

#SBATCH -q xfer                           # Specify the SLURM queue to use.
#SBATCH -A als                            # Specify the account.
#SBATCH -C cron                           # Use the 'cron' constraint.
#SBATCH --time=12:00:00                   # Maximum runtime of 12 hours.
#SBATCH --job-name=transfer_to_HPSS_{relative_path}  # Set a descriptive job name.
#SBATCH --output={logs_path}/{relative_path}_prune_from_hpss_%j.out       # Standard output log file.
#SBATCH --error={logs_path}/{relative_path}_prune_from_hpss_%j.err        # Standard error log file.
#SBATCH --licenses=SCRATCH                # Request the SCRATCH license.
#SBATCH --mem=20GB                        # Request #GB of memory. Default 2GB.

set -euo pipefail                        # Enable strict error checking.
echo "[LOG] Job started at: $(date)"

# Check if the file exists on HPSS
if hsi "ls {source_endpoint.full_path(relative_path)}" &> /dev/null; then
    echo "[LOG] File {relative_path} exists on HPSS. Proceeding to prune."
    # Prune the file from HPSS
    hsi "rm {source_endpoint.full_path(relative_path)}"
    echo "[LOG] File {relative_path} has been pruned from HPSS."
    hsi ls -R {source_endpoint.full_path(relative_path)}
else
    echo "[LOG] Could not find File {relative_path} does not on HPSS. Check your file path again."
    exit 0
fi
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
#SBATCH --job-name=transfer_to_HPSS_{proposal_name}  # Set a descriptive job name.
#SBATCH --output={logs_path}/{proposal_name}_to_hpss_%j.out       # Standard output log file.
#SBATCH --error={logs_path}/{proposal_name}_to_hpss_%j.err        # Standard error log file.
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
# Create destination directory on HPSS recursively using hsi mkdir.
# This section ensures that the entire directory tree specified in DEST_PATH
# exists on HPSS. Since HPSS hsi does not support a recursive mkdir option,
# we split the path into its components and create each directory one by one.
# ------------------------------------------------------------------

echo "[LOG] Checking if HPSS destination directory exists at $DEST_PATH."

# Use 'hsi ls' to verify if the destination directory exists.
# The '-q' flag is used for quiet mode, and any output or errors are discarded.
if hsi -q "ls $DEST_PATH" >/dev/null 2>&1; then
    echo "[LOG] Destination directory $DEST_PATH already exists."
else
    # If the directory does not exist, begin the process to create it.
    echo "[LOG] Destination directory $DEST_PATH does not exist. Attempting to create it recursively."

    # Initialize an empty variable 'current' that will store the path built so far.
    current=""

    # Split the DEST_PATH using '/' as the delimiter.
    # This creates an array 'parts' where each element is a directory level in the path.
    IFS='/' read -ra parts <<< "$DEST_PATH"

    # Iterate over each directory component in the 'parts' array.
    for part in "${{parts[@]}}"; do
        # Skip any empty parts. An empty string may occur if the path starts with a '/'.
        if [ -z "$part" ]; then
            continue
        fi

        # Append the current part to the 'current' path variable.
        # This step incrementally reconstructs the full path one directory at a time.
        current="$current/$part"

        # Check if the current directory exists on HPSS using 'hsi ls'.
        if ! hsi -q "ls $current" >/dev/null 2>&1; then
            # If the directory does not exist, attempt to create it using 'hsi mkdir'.
            if hsi "mkdir $current" >/dev/null 2>&1; then
                echo "[LOG] Created directory $current."
            else
                echo "[ERROR] Failed to create directory $current."
                exit 1
            fi
        else
            echo "[LOG] Directory $current already exists."
        fi
    done
fi

# List the final HPSS directory tree for logging purposes.
# For some reason this gets logged in the project.err file, not the .out file.
hsi ls $DEST_PATH

# ------------------------------------------------------------------
# Transfer Logic: Check if SOURCE_PATH is a file or directory.
# ------------------------------------------------------------------
echo "[LOG] Determining type of SOURCE_PATH: $SOURCE_PATH"
if [ -f "$SOURCE_PATH" ]; then
    # Case: Single file detected.
    echo "[LOG] Single file detected. Transferring via hsi cput."
    FILE_NAME=$(basename "$SOURCE_PATH")
    echo "[LOG] File name: $FILE_NAME"
    hsi cput "$SOURCE_PATH" "$DEST_PATH/$FILE_NAME"
    echo "[LOG] (Simulated) File transfer completed for $FILE_NAME."
elif [ -d "$SOURCE_PATH" ]; then
    # Case: Directory detected.
    echo "[LOG] Directory detected. Initiating bundling process."
    THRESHOLD=2199023255552  # 2 TB in bytes.
    echo "[LOG] Threshold set to 2 TB (in bytes: $THRESHOLD)"

    # ------------------------------------------------------------------
    # Generate a list of relative file paths in the project directory.
    # This list will be used to group files by their modification date.
    # ------------------------------------------------------------------
    # Create a temporary file to store the list of relative file paths.
    # Explanation:
    # 1. FILE_LIST=$(mktemp)
    #    - mktemp creates a unique temporary file and its path is stored in FILE_LIST.
    #
    # 2. (cd "$SOURCE_PATH" && find . -type f | sed 's|^\./||')
    #    - The parentheses run the commands in a subshell, so the directory change does not affect the current shell.
    #    - cd "$SOURCE_PATH": Changes the working directory to the source directory.
    #    - find . -type f: Recursively finds all files starting from the current directory (which is now SOURCE_PATH),
    #      outputting paths prefixed with "./".
    #    - sed 's|^\./||': Removes the leading "./" from each file path, resulting in relative paths without the prefix.
    #
    # 3. The output is then redirected into the temporary file specified by FILE_LIST.
    # ------------------------------------------------------------------
    echo "[LOG] Grouping files by modification date."

    FILE_LIST=$(mktemp)
    (cd "$SOURCE_PATH" && find . -type f | sed 's|^\./||') > "$FILE_LIST"

    echo "[LOG] List of files stored in temporary file: $FILE_LIST"

    # Declare associative arrays to hold grouped file paths and sizes.
    declare -A group_files
    declare -A group_sizes

    # ------------------------------------------------------------------
    # Group files by modification date.
    # ------------------------------------------------------------------

    cd "$SOURCE_PATH" && \
    while IFS= read -r file; do
        mtime=$(stat -c %Y "$file")
        year=$(date -d @"$mtime" +%Y)
        month=$(date -d @"$mtime" +%m | sed 's/^0*//')
        day=$(date -d @"$mtime" +%d | sed 's/^0*//')
        # Determine cycle: Cycle 1 if month < 7 or (month == 7 and day <= 15), else Cycle 2.
        if [ "$month" -lt 7 ] || {{ [ "$month" -eq 7 ] && [ "$day" -le 15 ]; }}; then
            cycle=1
        else
            cycle=2
        fi
        key="${{year}}-${{cycle}}"
        group_files["$key"]="${{group_files["$key"]:-}} $file"
        fsize=$(stat -c %s "$file")
        group_sizes["$key"]=$(( ${{group_sizes["$key"]:-0}} + fsize ))
    done < "$FILE_LIST"
    rm "$FILE_LIST"
    echo "[LOG] Completed grouping files."

    # Print the files in each group at the end
    for key in "${{!group_files[@]}}"; do
        echo "[LOG] Group $key contains files:"
        for f in ${{group_files["$key"]}}; do
            echo "    $f"
        done
    done

    # ------------------------------------------------------------------
    # Bundle files into tar archives.
    # ------------------------------------------------------------------
    for key in "${{!group_files[@]}}"; do
        files=(${{group_files["$key"]}})
        total_group_size=${{group_sizes["$key"]}}
        echo "[LOG] Processing group $key with ${{#files[@]}} files; total size: $total_group_size bytes."

        part=0
        current_size=0
        current_files=()
        for f in "${{files[@]}}"; do
            fsize=$(stat -c %s "$f")
            # If adding this file exceeds the threshold, process the current bundle.
            if (( current_size + fsize > THRESHOLD && ${{#current_files[@]}} > 0 )); then
                if [ $part -eq 0 ]; then
                    tar_name="${{FOLDER_NAME}}_${{key}}.tar"
                else
                    tar_name="${{FOLDER_NAME}}_${{key}}_part${{part}}.tar"
                fi
                echo "[LOG] Bundle reached threshold."
                echo "[LOG] Files in current bundle:"
                for file in "${{current_files[@]}}"; do
                    echo "$file"
                done
                echo "[LOG] Creating archive $tar_name with ${{#current_files[@]}} files; bundle size: $current_size bytes."
                (cd "$SOURCE_PATH" && htar -cvf "${{DEST_PATH}}/${{tar_name}}" $(printf "%s " "${{current_files[@]}}"))
                part=$((part+1))
                echo "[DEBUG] Resetting bundle variables."
                current_files=()
                current_size=0
            fi
            current_files+=("$f")
            current_size=$(( current_size + fsize ))
        done
        if [ ${{#current_files[@]}} -gt 0 ]; then
            if [ $part -eq 0 ]; then
                tar_name="${{FOLDER_NAME}}_${{key}}.tar"
            else
                tar_name="${{FOLDER_NAME}}_${{key}}_part${{part}}.tar"
            fi
            echo "[LOG] Final bundle for group $key:"
            echo "[LOG] Files in final bundle:"
            for file in "${{current_files[@]}}"; do
                echo "$file"
            done
            echo "[LOG] Creating final archive $tar_name with ${{#current_files[@]}} files."
            echo "[LOG] Bundle size: $current_size bytes."
            (cd "$SOURCE_PATH" && htar -cvf "${{DEST_PATH}}/${{tar_name}}" $(printf "%s " "${{current_files[@]}}"))
        fi
        echo "[LOG] Completed processing group $key."
    done
else
    echo "[ERROR] $SOURCE_PATH is neither a file nor a directory. Exiting."
    exit 1
fi

# ------------------------------------------------------------------
# Logging: Display directory trees for both source and destination.
# ------------------------------------------------------------------
echo "[LOG] Listing Source (CFS) Tree:"
if [ -d "$SOURCE_PATH" ]; then
    find "$SOURCE_PATH" -print
else
    echo "[LOG] $SOURCE_PATH is a file."
fi

echo "[LOG] Listing Destination (HPSS) Tree:"
hsi ls -R "$DEST_PATH" || echo "[ERROR] Failed to list HPSS tree at $DEST_PATH"

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
        job_script = fr"""#!/bin/bash
#SBATCH -q xfer                                             # Specify the SLURM queue to u
#SBATCH -A als                                              # Specify the account.
#SBATCH -C cron                                             # Use the 'cron' constraint.
#SBATCH --time=12:00:00                                     # Maximum runtime of 12 hours.
#SBATCH --job-name=transfer_from_HPSS_{file_path}           # Set a descriptive job name.
#SBATCH --output={logs_path}/{file_path}_from_hpss_%j.out   # Standard output log file.
#SBATCH --error={logs_path}/{file_path}_from_hpss_%j.err    # Standard error log file.
#SBATCH --licenses=SCRATCH                                  # Request the SCRATCH license.
#SBATCH --mem=20GB                                          # Request #GB of memory. Default 2GB.
set -euo pipefail                                           # Enable strict error checking.
echo "[LOG] Job started at: $(date)"

# -------------------------------------------------------------------
# Define source and destination variables.
# -------------------------------------------------------------------

echo "[LOG] Defining source and destination paths."

# SOURCE_PATH: Full path of the file or directory on HPSS.
SOURCE_PATH="{hpss_path}"
echo "[LOG] SOURCE_PATH set to: $SOURCE_PATH"

# DEST_ROOT: Root destination on CFS built from configuration.
DEST_ROOT="{dest_root}"
echo "[LOG] DEST_ROOT set to: $DEST_ROOT"

# FILES_TO_EXTRACT: Specific files to extract from the tar archive, if any.
# If not provided, this will be empty.
FILES_TO_EXTRACT="{files_to_extract_str}"
echo "[LOG] FILES_TO_EXTRACT set to: $FILES_TO_EXTRACT"

# -------------------------------------------------------------------
# Verify that SOURCE_PATH exists on HPSS using hsi ls.
# -------------------------------------------------------------------

echo "[LOG] Verifying file existence with hsi ls."
if ! hsi ls "$SOURCE_PATH" >/dev/null 2>&1; then
    echo "[ERROR] File not found on HPSS: $SOURCE_PATH"
    exit 1
fi

# -------------------------------------------------------------------
# Determine the transfer mode based on the type (file vs tar).
# -------------------------------------------------------------------

echo "[LOG] Determining transfer mode based on the type (file vs tar)."

# Check if SOURCE_PATH ends with .tar
if [[ "$SOURCE_PATH" =~ \.tar$ ]]; then
    # If FILES_TO_EXTRACT is nonempty, MODE becomes "partial", else MODE is "tar".
    if [ -n "${{FILES_TO_EXTRACT}}" ]; then
         MODE="partial"
    else
         MODE="tar"
    fi
else
    MODE="single"
fi

echo "Transfer mode: $MODE"

# -------------------------------------------------------------------
# Transfer Logic: Based on the mode, perform the appropriate transfer.
# -------------------------------------------------------------------

if [ "$MODE" = "single" ]; then
    echo "[LOG] Single file detected. Using hsi get."
    # mkdir -p "$DEST_ROOT"
    # hsi get "$SOURCE_PATH" "$DEST_ROOT/"
elif [ "$MODE" = "tar" ]; then
    echo "[LOG] Tar archive detected. Extracting entire archive using htar."
    ARCHIVE_BASENAME=$(basename "$SOURCE_PATH")
    ARCHIVE_NAME="${{ARCHIVE_BASENAME%.tar}}"
    DEST_PATH="${{DEST_ROOT}}/${{ARCHIVE_NAME}}"
    echo "[LOG] Extracting to: $DEST_PATH"
    # mkdir -p "$DEST_PATH"
    # htar -xvf "$SOURCE_PATH" -C "$DEST_PATH"
elif [ "$MODE" = "partial" ]; then
    echo "[LOG] Partial extraction detected. Extracting selected files using htar."
    ARCHIVE_BASENAME=$(basename "$SOURCE_PATH")
    ARCHIVE_NAME="${{ARCHIVE_BASENAME%.tar}}"
    DEST_PATH="${{DEST_ROOT}}/${{ARCHIVE_NAME}}"

    # Verify that each requested file exists in the tar archive.
    echo "[LOG] Verifying requested files are in the tar archive."
    ARCHIVE_CONTENTS=$(htar -tvf "$SOURCE_PATH")
    echo "[LOG] List: $ARCHIVE_CONTENTS"
    for file in $FILES_TO_EXTRACT; do
        echo "[LOG] Checking for file: $file"
        if ! echo "$ARCHIVE_CONTENTS" | grep -q "$file"; then
            echo "[ERROR] Requested file '$file' not found in archive $SOURCE_PATH"
            exit 1
        else
            echo "[LOG] File '$file' found in archive."
        fi
    done

    echo "[LOG] All requested files verified. Proceeding with extraction."
    mkdir -p "$DEST_PATH"
    (cd "$DEST_PATH" && htar -xvf "$SOURCE_PATH" -Hnostage $FILES_TO_EXTRACT)

    echo "[LOG] Extraction complete. Listing contents of $DEST_PATH:"
    ls -l "$DEST_PATH"

else
    echo "[ERROR]: Unknown mode: $MODE"
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


if __name__ == "__main__":
    TEST_HPSS_PRUNE = False
    TEST_CFS_TO_HPSS = False
    TEST_HPSS_TO_CFS = False

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
