import datetime
from dotenv import load_dotenv
import json
import logging
import os
from pathlib import Path
import re
import time

from authlib.jose import JsonWebKey
from prefect import flow, get_run_logger
from prefect.variables import Variable
from sfapi_client import Client
from sfapi_client.compute import Machine
from typing import Optional

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.job_controller import get_controller, HPC, TomographyHPCController
from orchestration.transfer_controller import get_transfer_controller, CopyMethod
from orchestration.flows.bl832.streaming_mixin import (
    NerscStreamingMixin, SlurmJobBlock, cancellation_hook, monitor_streaming_job, save_block
)
from orchestration.prefect import schedule_prefect_flow

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


class NERSCTomographyHPCController(TomographyHPCController, NerscStreamingMixin):
    """
    Implementation for a NERSC-based tomography HPC controller.

    Submits reconstruction and multi-resolution jobs to NERSC via SFAPI.
    """

    def __init__(
        self,
        client: Client,
        config: Config832
    ) -> None:
        TomographyHPCController.__init__(self, config)
        self.client = client

    @staticmethod
    def create_sfapi_client() -> Client:
        """Create and return an NERSC client instance"""

        # When generating the SFAPI Key in Iris, make sure to select "asldev" as the user!
        # Otherwise, the key will not have the necessary permissions to access the data.
        client_id_path = os.getenv("PATH_NERSC_CLIENT_ID")
        client_secret_path = os.getenv("PATH_NERSC_PRI_KEY")

        if not client_id_path or not client_secret_path:
            logger.error("NERSC credentials paths are missing.")
            raise ValueError("Missing NERSC credentials paths.")
        if not os.path.isfile(client_id_path) or not os.path.isfile(client_secret_path):
            logger.error("NERSC credential files are missing.")
            raise FileNotFoundError("NERSC credential files are missing.")

        client_id = None
        client_secret = None
        with open(client_id_path, "r") as f:
            client_id = f.read()

        with open(client_secret_path, "r") as f:
            client_secret = JsonWebKey.import_key(json.loads(f.read()))

        try:
            client = Client(client_id, client_secret)
            logger.info("NERSC client created successfully.")
            return client
        except Exception as e:
            logger.error(f"Failed to create NERSC client: {e}")
            raise e

    def reconstruct(
        self,
        file_path: str = "",
    ) -> bool:
        """
        Use NERSC for tomography reconstruction
        """
        logger.info("Starting NERSC reconstruction process.")

        user = self.client.user()

        raw_path = self.config.nersc832_alsdev_raw.root_path
        logger.info(f"{raw_path=}")

        recon_image = self.config.ghcr_images832["recon_image"]
        logger.info(f"{recon_image=}")

        recon_scripts_dir = self.config.nersc832_alsdev_recon_scripts.root_path
        logger.info(f"{recon_scripts_dir=}")

        scratch_path = self.config.nersc832_alsdev_scratch.root_path
        logger.info(f"{scratch_path=}")

        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
        logger.info(f"{pscratch_path=}")

        path = Path(file_path)
        folder_name = path.parent.name
        if not folder_name:
            folder_name = ""

        file_name = f"{path.stem}.h5"

        logger.info(f"File name: {file_name}")
        logger.info(f"Folder name: {folder_name}")

        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        # Note: If q=debug, there is no minimum time limit
        # However, if q=preempt, there is a minimum time limit of 2 hours. Otherwise the job won't run.
        # The realtime queue  can only be used for select accounts (e.g. ALS)
        job_script = f"""#!/bin/bash
#SBATCH -q realtime
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomo_recon_{folder_name}_{file_name}
#SBATCH --output={pscratch_path}/tomo_recon_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 128
#SBATCH --time=0:15:00
#SBATCH --exclusive

date
echo "Creating directory {pscratch_path}/8.3.2/raw/{folder_name}"
mkdir -p {pscratch_path}/8.3.2/raw/{folder_name}
mkdir -p {pscratch_path}/8.3.2/scratch/{folder_name}

echo "Copying file {raw_path}/{folder_name}/{file_name} to {pscratch_path}/8.3.2/raw/{folder_name}/"
cp {raw_path}/{folder_name}/{file_name} {pscratch_path}/8.3.2/raw/{folder_name}
if [ $? -ne 0 ]; then
    echo "Failed to copy data to pscratch."
    exit 1
fi

# chmod -R 2775 {pscratch_path}/8.3.2
chmod 2775 {pscratch_path}/8.3.2/raw/{folder_name}
chmod 2775 {pscratch_path}/8.3.2/scratch/{folder_name}
chmod 664 {pscratch_path}/8.3.2/raw/{folder_name}/{file_name}


echo "Verifying copied files..."
ls -l {pscratch_path}/8.3.2/raw/{folder_name}/

echo "Running reconstruction container..."
srun podman-hpc run \
--env NUMEXPR_MAX_THREADS=128 \\
--env NUMEXPR_NUM_THREADS=128 \\
--env OMP_NUM_THREADS=128 \\
--env MKL_NUM_THREADS=128 \\
--volume {recon_scripts_dir}/sfapi_reconstruction.py:/alsuser/sfapi_reconstruction.py \
--volume {pscratch_path}/8.3.2:/alsdata \
--volume {pscratch_path}/8.3.2:/alsuser/ \
{recon_image} \
bash -c "python sfapi_reconstruction.py {file_name} {folder_name}"
date
"""

        try:
            logger.info("Submitting reconstruction job script to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()  # Wait until the job completes
            logger.info("Reconstruction job completed successfully.")
            return True

        except Exception as e:
            logger.info(f"Error during job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))

            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.perlmutter.job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Reconstruction job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                # Unknown error: cannot recover
                return False

    def reconstruct_multinode(
        self,
        file_path: str = "",
        num_nodes: int = 2,
    ) -> bool:

        """
        Use NERSC for tomography reconstruction

        :param file_path: Path to the file to reconstruct
        :param num_nodes: Number of nodes to use for parallel reconstruction
        """
        logger.info("Starting NERSC reconstruction process.")

        user = self.client.user()

        raw_path = self.config.nersc832_alsdev_raw.root_path
        logger.info(f"{raw_path=}")

        recon_image = self.config.ghcr_images832["recon_image"]
        logger.info(f"{recon_image=}")

        recon_scripts_dir = self.config.nersc832_alsdev_recon_scripts.root_path
        logger.info(f"{recon_scripts_dir=}")

        scratch_path = self.config.nersc832_alsdev_scratch.root_path
        logger.info(f"{scratch_path=}")

        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
        logger.info(f"{pscratch_path=}")

        path = Path(file_path)
        folder_name = path.parent.name
        if not folder_name:
            folder_name = ""

        file_name = f"{path.stem}.h5"

        logger.info(f"File name: {file_name}")
        logger.info(f"Folder name: {folder_name}")
        logger.info(f"Number of nodes: {num_nodes}")

        if num_nodes == 8:
            qos = "debug"
        if num_nodes < 8:
            qos = "realtime"
        if num_nodes > 8:
            qos = "preempt"

        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        job_script = f"""#!/bin/bash
#SBATCH -q {qos}
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomo_recon_{folder_name}_{file_name}
#SBATCH --output={pscratch_path}/tomo_recon_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/%x_%j.err
#SBATCH -N {num_nodes}
#SBATCH --ntasks={num_nodes}
#SBATCH --cpus-per-task=128
#SBATCH --time=0:30:00
#SBATCH --exclusive
#SBATCH --image={recon_image}

# Timing file for this job
TIMING_FILE="{pscratch_path}/tomo_recon_logs/timing_$SLURM_JOB_ID.txt"

echo "JOB_START=$(date +%s)" > $TIMING_FILE
echo "Running reconstruction with {num_nodes} nodes"

# No container pull needed with Shifter - image is pre-staged via --image
echo "PREPULL_START=$(date +%s)" >> $TIMING_FILE
echo "PREPULL_END=$(date +%s)" >> $TIMING_FILE

mkdir -p {pscratch_path}/8.3.2/raw/{folder_name}
mkdir -p {pscratch_path}/8.3.2/scratch/{folder_name}

echo "COPY_START=$(date +%s)" >> $TIMING_FILE
if [ ! -f "{pscratch_path}/8.3.2/raw/{folder_name}/{file_name}" ]; then
    cp {raw_path}/{folder_name}/{file_name} {pscratch_path}/8.3.2/raw/{folder_name}
    if [ $? -ne 0 ]; then
        echo "Failed to copy data to pscratch."
        exit 1
    fi
    echo "COPY_SKIPPED=false" >> $TIMING_FILE
else
    echo "COPY_SKIPPED=true" >> $TIMING_FILE
fi
echo "COPY_END=$(date +%s)" >> $TIMING_FILE

chmod 2775 {pscratch_path}/8.3.2/raw/{folder_name}
chmod 2775 {pscratch_path}/8.3.2/scratch/{folder_name}
chmod 664 {pscratch_path}/8.3.2/raw/{folder_name}/{file_name}

NNODES={num_nodes}

echo "METADATA_START=$(date +%s)" >> $TIMING_FILE
NUM_SLICES=$(shifter \
    --volume={pscratch_path}/8.3.2:/alsdata \
    python -c "
import h5py
with h5py.File('/alsdata/raw/{folder_name}/{file_name}', 'r') as f:
    if '/exchange/data' in f:
        print(f['/exchange/data'].shape[1])
    else:
        for key in f.keys():
            grp = f[key]
            if 'nslices' in grp.attrs:
                print(int(grp.attrs['nslices']))
                break
" 2>&1 | grep -E '^[0-9]+$' | head -1)
echo "METADATA_END=$(date +%s)" >> $TIMING_FILE

echo "NUM_SLICES=$NUM_SLICES" >> $TIMING_FILE

if [ -z "$NUM_SLICES" ]; then
    echo "Failed to read number of slices from HDF5 file"
    exit 1
fi

if ! [[ "$NUM_SLICES" =~ ^[0-9]+$ ]]; then
    echo "Failed to read number of slices. Got: $NUM_SLICES"
    exit 1
fi

SLICES_PER_NODE=$((NUM_SLICES / NNODES))

echo "RECON_START=$(date +%s)" >> $TIMING_FILE

# Create symlink so folder_name resolves correctly (like podman mount did)
ln -sfn {pscratch_path}/8.3.2/raw/{folder_name} {pscratch_path}/8.3.2/{folder_name}

for i in $(seq 0 $((NNODES - 1))); do
    SINO_START=$((i * SLICES_PER_NODE))

    if [ $i -eq $((NNODES - 1)) ]; then
        SINO_END=$NUM_SLICES
    else
        SINO_END=$(((i + 1) * SLICES_PER_NODE))
    fi

    srun --nodes=1 --ntasks=1 --exclusive shifter \
        --env=NUMEXPR_MAX_THREADS=128 \
        --env=NUMEXPR_NUM_THREADS=128 \
        --env=OMP_NUM_THREADS=128 \
        --env=MKL_NUM_THREADS=128 \
        --volume={pscratch_path}/8.3.2:/alsuser \
        --volume={pscratch_path}/8.3.2/scratch:/scratch \
        --volume={recon_scripts_dir}:/opt/scripts \
        /bin/bash -c "cd /alsuser && python /opt/scripts/sfapi_reconstruction_multinode.py {file_name} {folder_name} $SINO_START $SINO_END" &
done

wait
WAIT_STATUS=$?
echo "RECON_END=$(date +%s)" >> $TIMING_FILE

if [ $WAIT_STATUS -ne 0 ]; then
    echo "One or more reconstruction tasks failed"
    echo "JOB_STATUS=FAILED" >> $TIMING_FILE
    exit 1
fi

echo "JOB_STATUS=SUCCESS" >> $TIMING_FILE
echo "JOB_END=$(date +%s)" >> $TIMING_FILE
"""
        try:
            logger.info("Submitting reconstruction job script to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()  # Wait until the job completes
            logger.info("Reconstruction job completed successfully.")
            # Fetch timing data
            timing = self._fetch_timing_data(perlmutter, pscratch_path, job.jobid)

            return {
                "success": True,
                "job_id": job.jobid,
                "timing": timing
            }

        except Exception as e:
            logger.info(f"Error during job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))

            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.perlmutter.job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Reconstruction job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                return False

    def _fetch_timing_data(self, perlmutter, pscratch_path: str, job_id: str) -> dict:
        """Fetch and parse timing data from the SLURM job."""
        timing_file = f"{pscratch_path}/tomo_recon_logs/timing_{job_id}.txt"

        try:
            # Use SFAPI to read the timing file
            result = perlmutter.run(f"cat {timing_file}")

            # result might be a string directly, or an object with .output
            if isinstance(result, str):
                output = result
            elif hasattr(result, 'output'):
                output = result.output
            elif hasattr(result, 'stdout'):
                output = result.stdout
            else:
                output = str(result)

            logger.info(f"Timing file contents:\n{output}")

            # Parse timing data
            timing = {}
            for line in output.strip().split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    timing[key] = value.strip()

            # Calculate durations
            breakdown = {}

            if 'JOB_START' in timing and 'JOB_END' in timing:
                breakdown['total'] = int(timing['JOB_END']) - int(timing['JOB_START'])

            if 'PREPULL_START' in timing and 'PREPULL_END' in timing:
                breakdown['container_pull'] = int(timing['PREPULL_END']) - int(timing['PREPULL_START'])

            if 'COPY_START' in timing and 'COPY_END' in timing:
                breakdown['file_copy'] = int(timing['COPY_END']) - int(timing['COPY_START'])
                breakdown['copy_skipped'] = timing.get('COPY_SKIPPED', 'false') == 'true'

            if 'METADATA_START' in timing and 'METADATA_END' in timing:
                breakdown['metadata'] = int(timing['METADATA_END']) - int(timing['METADATA_START'])

            if 'RECON_START' in timing and 'RECON_END' in timing:
                breakdown['reconstruction'] = int(timing['RECON_END']) - int(timing['RECON_START'])

            if 'NUM_SLICES' in timing:
                breakdown['num_slices'] = int(timing['NUM_SLICES'])

            breakdown['job_status'] = timing.get('JOB_STATUS', 'UNKNOWN')

            return breakdown

        except Exception as e:
            logger.warning(f"Error fetching timing data: {e}")
            import traceback
            logger.warning(traceback.format_exc())
            return None

    def pull_shifter_image(
        self,
        image: str = None,
        wait: bool = True,
    ) -> bool:
        """
        Pull a container image into NERSC's Shifter cache.

        This should be run once when the image is updated, not before every reconstruction.
        After the image is cached, jobs using --image= will start much faster.

        :param image: Container image to pull (defaults to recon_image from config)
        :param wait: Whether to wait for the pull to complete
        :return: True if successful, False otherwise
        """
        logger.info("Starting Shifter image pull.")

        user = self.client.user()
        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"

        if image is None:
            image = self.config.ghcr_images832["recon_image"]

        logger.info(f"Pulling image: {image}")

        job_script = f"""#!/bin/bash
#SBATCH -q debug
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=shifter_pull
#SBATCH --output={pscratch_path}/tomo_recon_logs/shifter_pull_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/shifter_pull_%j.err
#SBATCH -N 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=0:15:00

echo "Starting Shifter image pull at $(date)"
echo "Image: {image}"

# Check if image already exists
echo "Checking existing images..."
shifterimg images | grep -E "$(echo {image} | sed 's/:/.*/')" || true

# Pull the image
echo "Pulling image..."
shifterimg -v pull {image}
PULL_STATUS=$?

if [ $PULL_STATUS -eq 0 ]; then
    echo "Image pull successful"
else
    echo "Image pull failed with status $PULL_STATUS"
    exit 1
fi

# Verify the image is now available
echo "Verifying image..."
shifterimg images | grep -E "$(echo {image} | sed 's/:/.*/')"

echo "Completed at $(date)"
"""

        try:
            logger.info("Submitting Shifter image pull job to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            if wait:
                try:
                    job.update()
                except Exception as update_err:
                    logger.warning(f"Initial job update failed, continuing: {update_err}")

                time.sleep(30)
                logger.info(f"Job {job.jobid} current state: {job.state}")

                job.complete()
                logger.info("Shifter image pull completed successfully.")
                return True
            else:
                logger.info(f"Job submitted. Check status with job ID: {job.jobid}")
                return True

        except Exception as e:
            logger.error(f"Error during Shifter image pull: {e}")
            return False

    def check_shifter_image(
        self,
        image: str = None,
    ) -> bool:
        """
        Check if a container image is already in NERSC's Shifter cache.

        :param image: Container image to check (defaults to recon_image from config)
        :return: True if image exists in cache, False otherwise
        """
        logger.info("Checking Shifter image cache.")

        if image is None:
            image = self.config.ghcr_images832["recon_image"]

        try:
            perlmutter = self.client.compute(Machine.perlmutter)

            # Run shifterimg images command
            result = perlmutter.run(f"shifterimg images | grep -E \"$(echo {image} | sed 's/:/.*/g')\"")

            if isinstance(result, str):
                output = result
            elif hasattr(result, 'output'):
                output = result.output
            else:
                output = str(result)

            if output.strip():
                logger.info(f"Image found in Shifter cache: {output.strip()}")
                return True
            else:
                logger.info(f"Image not found in Shifter cache: {image}")
                return False

        except Exception as e:
            logger.warning(f"Error checking Shifter cache: {e}")
            return False

    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        """Use NERSC to make multiresolution version of tomography results."""

        logger.info("Starting NERSC multiresolution process.")

        user = self.client.user()

        multires_image = self.config.ghcr_images832["multires_image"]
        logger.info(f"{multires_image=}")

        recon_scripts_dir = self.config.nersc832_alsdev_recon_scripts.root_path
        logger.info(f"{recon_scripts_dir=}")

        scratch_path = self.config.nersc832_alsdev_scratch.root_path
        logger.info(f"{scratch_path=}")

        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
        logger.info(f"{pscratch_path=}")

        path = Path(file_path)
        folder_name = path.parent.name
        file_name = path.stem

        recon_path = f"scratch/{folder_name}/rec{file_name}/"
        logger.info(f"{recon_path=}")

        raw_path = f"raw/{folder_name}/{file_name}.h5"
        logger.info(f"{raw_path=}")

        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        job_script = f"""#!/bin/bash
#SBATCH -q realtime
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomo_multires_{folder_name}_{file_name}
#SBATCH --output={pscratch_path}/tomo_recon_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 64
#SBATCH --time=0:15:00
#SBATCH --exclusive

date

echo "Running multires container..."
srun podman-hpc run \
--volume {recon_scripts_dir}/tiff_to_zarr.py:/alsuser/tiff_to_zarr.py \
--volume {pscratch_path}/8.3.2:/alsdata \
--volume {pscratch_path}/8.3.2:/alsuser/ \
{multires_image} \
bash -c "python tiff_to_zarr.py {recon_path} --raw_file {raw_path}"

date
"""
        try:
            logger.info("Submitting Tiff to Zarr job script to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()  # Wait until the job completes
            logger.info("Reconstruction job completed successfully.")

            return True

        except Exception as e:
            logger.warning(f"Error during job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))

            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.perlmutter.job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Reconstruction job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                return False

    def start_streaming_service(
        self,
        walltime: datetime.timedelta = datetime.timedelta(minutes=30),
    ) -> str:
        return NerscStreamingMixin.start_streaming_service(
            self,
            client=self.client,
            walltime=walltime
        )


def schedule_pruning(
    config: Config832,
    raw_file_path: str,
    tiff_file_path: str,
    zarr_file_path: str
) -> bool:
    # data832/scratch : 14 days
    # nersc/pscratch : 1 day
    # nersc832/scratch : never?

    pruning_config = Variable.get("pruning-config", _sync=True)
    data832_delay = datetime.timedelta(days=pruning_config["delete_data832_files_after_days"])
    nersc832_delay = datetime.timedelta(days=pruning_config["delete_nersc832_files_after_days"])

    # data832_delay, nersc832_delay = datetime.timedelta(minutes=1), datetime.timedelta(minutes=1)

    # Delete tiffs from data832_scratch
    logger.info(f"Deleting tiffs from data832_scratch: {tiff_file_path=}")
    try:
        source_endpoint = config.data832_scratch
        check_endpoint = config.nersc832_alsdev_scratch
        location = "data832_scratch"

        flow_name = f"delete {location}: {Path(tiff_file_path).name}"
        schedule_prefect_flow(
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": tiff_file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
            duration_from_now=data832_delay
        )
    except Exception as e:
        logger.error(f"Failed to schedule prune task: {e}")

    # Delete zarr from data832_scratch
    logger.info(f"Deleting zarr from data832_scratch: {zarr_file_path=}")
    try:
        source_endpoint = config.data832_scratch
        check_endpoint = config.nersc832_alsdev_scratch
        location = "data832_scratch"

        flow_name = f"delete {location}: {Path(zarr_file_path).name}"
        schedule_prefect_flow(
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": zarr_file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
            duration_from_now=data832_delay
        )
    except Exception as e:
        logger.error(f"Failed to schedule prune task: {e}")

    # Delete from nersc832_pscratch/raw
    logger.info(f"Deleting raw from nersc832_alsdev_pscratch_raw: {raw_file_path=}")
    try:
        source_endpoint = config.nersc832_alsdev_pscratch_raw
        check_endpoint = None
        location = "nersc832_alsdev_pscratch_raw"

        path = Path(raw_file_path)
        folder_name = path.parent.name
        file_name = path.name  # includes .h5 extension
        pscratch_relative_path = f"{folder_name}/{file_name}"

        flow_name = f"delete {location}: {file_name}"
        schedule_prefect_flow(
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": pscratch_relative_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
            duration_from_now=nersc832_delay
        )
    except Exception as e:
        logger.error(f"Failed to schedule prune task: {e}")

    # Delete tiffs from from nersc832_pscratch/scratch
    logger.info(f"Deleting tiffs from nersc832_alsdev_pscratch_scratch: {tiff_file_path=}")
    try:
        source_endpoint = config.nersc832_alsdev_pscratch_scratch
        check_endpoint = None
        location = "nersc832_alsdev_pscratch_scratch"

        flow_name = f"delete {location}: {Path(tiff_file_path).name}"
        schedule_prefect_flow(
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": tiff_file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
            duration_from_now=nersc832_delay
        )
    except Exception as e:
        logger.error(f"Failed to schedule prune task: {e}")

    # Delete zarr from from nersc832_pscratch/scratch
    logger.info(f"Deleting zarr from nersc832_alsdev_pscratch_scratch: {zarr_file_path=}")
    try:
        source_endpoint = config.nersc832_alsdev_pscratch_scratch
        check_endpoint = None
        location = "nersc832_alsdev_pscratch_scratch"

        flow_name = f"delete {location}: {Path(zarr_file_path).name}"
        schedule_prefect_flow(
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": zarr_file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
            duration_from_now=nersc832_delay
        )
    except Exception as e:
        logger.error(f"Failed to schedule prune task: {e}")


@flow(name="nersc_recon_flow", flow_run_name="nersc_recon-{file_path}")
def nersc_recon_flow(
    file_path: str,
    num_nodes: int = 1,
    config: Optional[Config832] = None,
) -> bool:
    """
    Perform tomography reconstruction on NERSC.

    :param file_path: Path to the file to reconstruct.
    """
    logger = get_run_logger()

    if config is None:
        logger.info("Initializing Config")
        config = Config832()

    logger.info(f"Starting NERSC reconstruction flow for {file_path=}")
    controller = get_controller(
        hpc_type=HPC.NERSC,
        config=config
    )
    logger.info("NERSC reconstruction controller initialized")

    nersc_reconstruction_success = controller.reconstruct_multinode(
        file_path=file_path,
        num_nodes=num_nodes
    )

    if isinstance(nersc_reconstruction_success, dict):
        success = nersc_reconstruction_success.get('success', False)
        timing = nersc_reconstruction_success.get('timing')

        if timing:
            logger.info("=" * 50)
            logger.info("TIMING BREAKDOWN")
            logger.info("=" * 50)
            logger.info(f"  Total job time:      {timing.get('total', 'N/A')}s")
            logger.info(f"  Container pull:      {timing.get('container_pull', 'N/A')}s")
            logger.info(f"  File copy:           {timing.get('file_copy', 'N/A')}s (skipped: {timing.get('copy_skipped', 'N/A')})")
            logger.info(f"  Metadata detection:  {timing.get('metadata', 'N/A')}s")
            logger.info(f"  RECONSTRUCTION:      {timing.get('reconstruction', 'N/A')}s  <-- actual recon time")
            logger.info(f"  Num slices:          {timing.get('num_slices', 'N/A')}")
            logger.info("=" * 50)

            # Calculate overhead
            if all(k in timing for k in ['total', 'reconstruction']):
                overhead = timing['total'] - timing['reconstruction']
                logger.info(f"  Overhead:            {overhead}s")
                logger.info(f"  Reconstruction %:    {100 * timing['reconstruction'] / timing['total']:.1f}%")
            logger.info("=" * 50)
    else:
        success = nersc_reconstruction_success

    logger.info(f"NERSC reconstruction success: {success}")

    # Commented out for testing purposes -- should be re-enabled for production

    # nersc_multi_res_success = controller.build_multi_resolution(
    #     file_path=file_path,
    # )
    # logger.info(f"NERSC multi-resolution success: {nersc_multi_res_success}")

    # path = Path(file_path)
    # folder_name = path.parent.name
    # file_name = path.stem

    # tiff_file_path = f"{folder_name}/rec{file_name}"
    # zarr_file_path = f"{folder_name}/rec{file_name}.zarr"

    # logger.info(f"{tiff_file_path=}")
    # logger.info(f"{zarr_file_path=}")

    # Transfer reconstructed data
    # logger.info("Preparing transfer.")
    # transfer_controller = get_transfer_controller(
    #     transfer_type=CopyMethod.GLOBUS,
    #     config=config
    # )

    # logger.info("Copy from /pscratch/sd/a/alsdev/8.3.2 to /global/cfs/cdirs/als/data_mover/8.3.2/scratch.")
    # transfer_controller.copy(
    #     file_path=tiff_file_path,
    #     source=config.nersc832_alsdev_pscratch_scratch,
    #     destination=config.nersc832_alsdev_scratch
    # )

    # transfer_controller.copy(
    #     file_path=zarr_file_path,
    #     source=config.nersc832_alsdev_pscratch_scratch,
    #     destination=config.nersc832_alsdev_scratch
    # )

    # logger.info("Copy from NERSC /global/cfs/cdirs/als/data_mover/8.3.2/scratch to data832")
    # transfer_controller.copy(
    #     file_path=tiff_file_path,
    #     source=config.nersc832_alsdev_pscratch_scratch,
    #     destination=config.data832_scratch
    # )

    # transfer_controller.copy(
    #     file_path=zarr_file_path,
    #     source=config.nersc832_alsdev_pscratch_scratch,
    #     destination=config.data832_scratch
    # )

    # logger.info("Scheduling pruning tasks.")
    # schedule_pruning(
    #     config=config,
    #     raw_file_path=file_path,
    #     tiff_file_path=tiff_file_path,
    #     zarr_file_path=zarr_file_path
    # )

    # TODO: Ingest into SciCat
    if nersc_reconstruction_success:
        return True
    else:
        return False


@flow(name="nersc_streaming_flow", on_cancellation=[cancellation_hook])
def nersc_streaming_flow(
    walltime: datetime.timedelta = datetime.timedelta(minutes=5),
    monitor_interval: int = 10,
) -> bool:
    logger = get_run_logger()
    config = Config832()
    logger.info(f"Starting NERSC streaming flow with {walltime} walltime")

    controller: NERSCTomographyHPCController = get_controller(
        hpc_type=HPC.NERSC,
        config=config
    )  # type: ignore

    job_id = controller.start_streaming_service(walltime=walltime)
    save_block(SlurmJobBlock(job_id=job_id))

    success = monitor_streaming_job(
        client=controller.client,
        job_id=job_id,
        update_interval=monitor_interval
    )

    return success


@flow(name="pull_shifter_image_flow", flow_run_name="pull_shifter_image")
def pull_shifter_image_flow(
    image: Optional[str] = None,
    config: Optional[Config832] = None,
) -> bool:
    """
    Pull a container image into NERSC's Shifter cache.

    Run this once when the container image is updated.
    """
    logger = get_run_logger()

    if config is None:
        config = Config832()

    if image is None:
        image = config.ghcr_images832["recon_image"]

    logger.info(f"Pulling Shifter image: {image}")

    controller = get_controller(
        hpc_type=HPC.NERSC,
        config=config
    )

    # Check if already cached
    if controller.check_shifter_image(image):
        logger.info("Image already in cache, pulling anyway to update...")

    success = controller.pull_shifter_image(image)
    logger.info(f"Shifter image pull success: {success}")

    return success


if __name__ == "__main__":

    config = Config832()

    # pull_shifter_image_flow(config=config)

    # # Fibers ------------------------------------------

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20230215_135338_PET_Al_PP_Al2O3_fibers_in_glass_pipette.h5",
        num_nodes=8,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 8 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 8 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20230215_135338_PET_Al_PP_Al2O3_fibers_in_glass_pipette.h5",
        num_nodes=4,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 4 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 4 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20230215_135338_PET_Al_PP_Al2O3_fibers_in_glass_pipette.h5",
        num_nodes=2,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 2 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 2 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20230215_135338_PET_Al_PP_Al2O3_fibers_in_glass_pipette.h5",
        num_nodes=1,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 1 node: {end - start} seconds")
    print(f"Total reconstruction time with 1 node: {end - start} seconds")

    # # # Fungi ------------------------------------------

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5",
        num_nodes=8,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 8 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 8 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5",
        num_nodes=4,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 4 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 4 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5",
        num_nodes=2,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 2 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 2 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5",
        num_nodes=1,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 1 node: {end - start} seconds")
    print(f"Total reconstruction time with 1 node: {end - start} seconds")

    # # # Silk ------------------------------------------

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20251218_111600_silkraw.h5",
        num_nodes=8,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 8 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 8 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20251218_111600_silkraw.h5",
        num_nodes=4,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 4 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 4 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20251218_111600_silkraw.h5",
        num_nodes=2,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 2 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 2 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20251218_111600_silkraw.h5",
        num_nodes=1,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 1 node: {end - start} seconds")
    print(f"Total reconstruction time with 1 node: {end - start} seconds")
