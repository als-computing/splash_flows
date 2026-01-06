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

        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        job_script = f"""#!/bin/bash
#SBATCH -q realtime
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomo_recon_{folder_name}_{file_name}
#SBATCH --output={pscratch_path}/tomo_recon_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/%x_%j.err
#SBATCH -N {num_nodes}
#SBATCH --ntasks={num_nodes}
#SBATCH --cpus-per-task=128
#SBATCH --time=0:15:00
#SBATCH --exclusive

date
echo "Running reconstruction with {num_nodes} nodes"

echo "Pre-pulling container image..."
podman-hpc pull {recon_image}

echo "Creating directory {pscratch_path}/8.3.2/raw/{folder_name}"
mkdir -p {pscratch_path}/8.3.2/raw/{folder_name}
mkdir -p {pscratch_path}/8.3.2/scratch/{folder_name}

echo "Copying file {raw_path}/{folder_name}/{file_name} to {pscratch_path}/8.3.2/raw/{folder_name}/"
cp {raw_path}/{folder_name}/{file_name} {pscratch_path}/8.3.2/raw/{folder_name}
if [ $? -ne 0 ]; then
    echo "Failed to copy data to pscratch."
    exit 1
fi

chmod 2775 {pscratch_path}/8.3.2/raw/{folder_name}
chmod 2775 {pscratch_path}/8.3.2/scratch/{folder_name}
chmod 664 {pscratch_path}/8.3.2/raw/{folder_name}/{file_name}

echo "Verifying copied files..."
ls -l {pscratch_path}/8.3.2/raw/{folder_name}/

NNODES={num_nodes}
RAW_FILE="{pscratch_path}/8.3.2/raw/{folder_name}/{file_name}"

# Get the number of slices from the HDF5 file using the container
echo "Reading slice count from HDF5 file..."

NUM_SLICES=$(podman-hpc run --rm \\
    --volume {pscratch_path}/8.3.2:/alsdata \\
    {recon_image} \\
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

echo "Detected NUM_SLICES: $NUM_SLICES"

if [ -z "$NUM_SLICES" ]; then
    echo "Failed to read number of slices from HDF5 file"
    exit 1
fi

if ! [[ "$NUM_SLICES" =~ ^[0-9]+$ ]]; then
    echo "Failed to read number of slices. Got: $NUM_SLICES"
    exit 1
fi

echo "Total slices: $NUM_SLICES"
echo "Distributing across $NNODES nodes"

SLICES_PER_NODE=$((NUM_SLICES / NNODES))
echo "Slices per node: ~$SLICES_PER_NODE"

# Launch reconstruction on each node
for i in $(seq 0 $((NNODES - 1))); do
    SINO_START=$((i * SLICES_PER_NODE))

    # Last node takes any remainder slices
    if [ $i -eq $((NNODES - 1)) ]; then
        SINO_END=$NUM_SLICES
    else
        SINO_END=$(((i + 1) * SLICES_PER_NODE))
    fi

    echo "Launching node $i: slices $SINO_START to $SINO_END"

    srun --nodes=1 --ntasks=1 --exclusive podman-hpc run \
        --env NUMEXPR_MAX_THREADS=128 \
        --env NUMEXPR_NUM_THREADS=128 \
        --env OMP_NUM_THREADS=128 \
        --env MKL_NUM_THREADS=128 \
        --volume {recon_scripts_dir}/sfapi_reconstruction_multinode.py:/alsuser/sfapi_reconstruction_multinode.py \
        --volume {pscratch_path}/8.3.2/raw/{folder_name}:/alsuser/{folder_name} \
        --volume {pscratch_path}/8.3.2/scratch:/scratch \
        {recon_image} \
        bash -c "cd /alsuser && python sfapi_reconstruction_multinode.py {file_name} {folder_name} $SINO_START $SINO_END" &
done

echo "Waiting for all $NNODES nodes to complete..."
wait
WAIT_STATUS=$?

if [ $WAIT_STATUS -ne 0 ]; then
    echo "One or more reconstruction tasks failed"
    exit 1
fi

echo "All nodes completed successfully"
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

    if num_nodes > 1:
        nersc_reconstruction_success = controller.reconstruct_multinode(
            file_path=file_path,
            num_nodes=num_nodes
        )
    elif num_nodes == 1:
        nersc_reconstruction_success = controller.reconstruct(
            file_path=file_path,
        )
    else:
        raise ValueError("num_nodes must be at least 1")

    logger.info(f"NERSC reconstruction success: {nersc_reconstruction_success}")

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


if __name__ == "__main__":

    config = Config832()

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
        file_path="dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5",
        num_nodes=8,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 8 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 8 nodes: {end - start} seconds")

    start = time.time()
    nersc_recon_flow(
        file_path="dabramov/20251218_111600_silkraw.h5",
        num_nodes=8,
        config=config
    )
    end = time.time()
    logger.info(f"Total reconstruction time with 8 nodes: {end - start} seconds")
    print(f"Total reconstruction time with 8 nodes: {end - start} seconds")

    # start = time.time()
    # nersc_recon_flow(
    #     file_path="dabramov/20230215_135338_PET_Al_PP_Al2O3_fibers_in_glass_pipette.h5",
    #     num_nodes=4,
    #     config=config
    # )
    # end = time.time()
    # logger.info(f"Total reconstruction time with 4 nodes: {end - start} seconds")
    # print(f"Total reconstruction time with 4 nodes: {end - start} seconds")

    # start = time.time()
    # nersc_recon_flow(
    #     file_path="dabramov/20230215_135338_PET_Al_PP_Al2O3_fibers_in_glass_pipette.h5",
    #     num_nodes=2,
    #     config=config
    # )
    # end = time.time()
    # logger.info(f"Total reconstruction time with 2 nodes: {end - start} seconds")
    # print(f"Total reconstruction time with 2 nodes: {end - start} seconds")

    # start = time.time()
    # nersc_recon_flow(
    #     file_path="dabramov/20230215_135338_PET_Al_PP_Al2O3_fibers_in_glass_pipette.h5",
    #     num_nodes=1,
    #     config=config
    # )
    # end = time.time()
    # logger.info(f"Total reconstruction time with 1 node: {end - start} seconds")
    # print(f"Total reconstruction time with 1 node: {end - start} seconds")
    # nersc_streaming_flow(
    #     config=config,
    #     walltime=datetime.timedelta(minutes=5)
    # )'
