from dataclasses import dataclass, field
import datetime
from dotenv import load_dotenv
import json
import logging
import os
from pathlib import Path
import re
import time

from authlib.jose import JsonWebKey
from prefect import flow, get_run_logger, task
from prefect.variables import Variable
from sfapi_client import Client
from sfapi_client.compute import Machine
from typing import Any, Optional

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.job_controller import get_controller, HPC, TomographyHPCController
from orchestration.mlflow import get_checkpoint_info
from orchestration.prune_controller import get_prune_controller, PruneMethod
from orchestration.transfer_controller import globus_transfer_task
from orchestration.flows.bl832.streaming_mixin import (
    NerscStreamingMixin, SlurmJobBlock, cancellation_hook, monitor_streaming_job, save_block
)
from orchestration.prefect import schedule_prefect_flow

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


@dataclass
class SegmentationModelSpec:
    """All config-resolution inputs for a single model+project combination.

    Consumed by ``_load_job_options`` and the job-script builders.
    Adding a new model or project means adding one entry to the registry —
    nothing else changes.

    :param variable_name: Prefect Variable name for runtime overrides.
    :param settings: Config settings dict (from Config832) for base defaults.
    :param mlflow_model_name: Registered MLflow model name.
    :param mlflow_checkpoint_key: Config key populated from the MLflow
        model's ``nersc_path`` tag.
    :param output_subdir: Subdirectory written under ``seg_folder/``,
        e.g. ``'dino'``, ``'sam3'``, ``'dino_moon'``.
    :param extra_cli_flags: Additional flags injected into the inference
        command, e.g. ``{'--project': 'moon'}``. Omit flags not needed.
    """
    variable_name: str
    settings: dict[str, Any]
    mlflow_model_name: str
    mlflow_checkpoint_key: str
    extra_cli_flags: dict[str, str] = field(default_factory=dict)


def _load_job_options(
    variable_name: str,
    config_settings: dict[str, Any],
    config: Config832 | None = None,
    mlflow_model_name: str | None = None,
    mlflow_checkpoint_key: str | None = None,
) -> dict[str, Any]:
    """Load job options with three-layer resolution: config → MLflow → Prefect Variable.

    Resolution order (later layers win):

    1. ``config_settings`` — authoritative defaults from the config YAML.
    2. MLflow Model Registry — if ``mlflow_model_name`` is provided, all
       ``inference_params`` tags are overlaid onto opts by their config key name.
       ``nersc_path`` is additionally mapped to ``mlflow_checkpoint_key`` if given.
    3. Prefect Variable (``variable_name``) — skipped if absent or ``defaults: true``.
       If ``defaults: false``, provided keys override all lower layers.

    Args:
        variable_name: Name of the Prefect Variable to load.
        config_settings: Settings dict from Config832 used as base defaults.
        config: Config832 instance needed for MLflow lookup. If ``None``, the
            MLflow layer is skipped.
        mlflow_model_name: Registered MLflow model name, e.g. ``'sam3-petiole'``.
            If ``None``, the MLflow layer is skipped.
        mlflow_checkpoint_key: Config key to populate from the MLflow model's
            ``nersc_path`` tag, e.g. ``'finetuned_checkpoint_path'``.

    Returns:
        Resolved options dict ready for use by the caller.
    """
    # ── Layer 1: config defaults ──────────────────────────────────────────────
    opts = dict(config_settings)

    # ── Layer 2: MLflow registry ──────────────────────────────────────────────
    if config is not None and mlflow_model_name:
        try:
            checkpoint_info = get_checkpoint_info(mlflow_model_name, config)
            if checkpoint_info:
                # Map nersc_path to the caller-specified checkpoint key
                if mlflow_checkpoint_key:
                    opts[mlflow_checkpoint_key] = checkpoint_info.nersc_path
                    logger.info(
                        f"MLflow '{mlflow_model_name}': "
                        f"{mlflow_checkpoint_key}={checkpoint_info.nersc_path}"
                    )
                # Overlay all inference params that match existing config keys
                overlaid = []
                for k, v in checkpoint_info.inference_params.items():
                    if k in opts:
                        opts[k] = v
                        overlaid.append(k)
                    else:
                        # Also inject new keys (e.g. alcf_path for future use)
                        opts[k] = v
                logger.info(
                    f"MLflow '{mlflow_model_name}': overlaid params: {overlaid}"
                )
            else:
                logger.info(
                    f"MLflow: no production checkpoint for '{mlflow_model_name}', "
                    "using config defaults."
                )
        except Exception as e:
            logger.warning(
                f"MLflow lookup failed for '{mlflow_model_name}': {e}. "
                "Using config defaults."
            )

    # ── Layer 3: Prefect Variable overrides ───────────────────────────────────
    try:
        options = Variable.get(variable_name, default={"defaults": True}, _sync=True)
        if isinstance(options, str):
            options = json.loads(options)
    except Exception as e:
        logger.warning(f"Could not load '{variable_name}': {e}. Skipping variable overrides.")
        return opts

    if options.get("defaults", True):
        logger.info(f"Prefect Variable '{variable_name}': no overrides.")
        return opts

    overrides = {k: v for k, v in options.items() if k != "defaults"}
    logger.info(f"Prefect Variable '{variable_name}': applying overrides: {list(overrides)}")
    return {**opts, **overrides}


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

    def _get_segmentation_spec(self, model: str, project: str) -> SegmentationModelSpec:
        """Return the SegmentationModelSpec for a model+project combination.

        :param model: Model family, e.g. ``'dinov3'`` or ``'sam3'``.
        :param project: Experiment project, e.g. ``'petiole'`` or ``'moon'``.
        :return: The corresponding SegmentationModelSpec.
        :raises ValueError: If the combination is not registered.
        """
        registry: dict[tuple[str, str], SegmentationModelSpec] = {
            ("dinov3", "petiole"): SegmentationModelSpec(
                variable_name="nersc-dinov3-seg-options",
                settings=self.config.nersc_segment_dinov3_settings,
                mlflow_model_name="dinov3-petiole",
                mlflow_checkpoint_key="dino_checkpoint_path",
            ),
            ("dinov3", "moon"): SegmentationModelSpec(
                variable_name="nersc-dinov3-moon-seg-options",
                settings=self.config.nersc_segment_dinov3_moon_settings,
                mlflow_model_name="dinov3-moon",
                mlflow_checkpoint_key="dino_checkpoint_path",
                extra_cli_flags={"--project": "moon"},
            ),
            ("dinov3", "leaf"): SegmentationModelSpec(
                variable_name="nersc-dinov3-leaf-seg-options",
                settings=self.config.nersc_segment_dinov3_leaf_settings,
                mlflow_model_name="dinov3-leaf",
                mlflow_checkpoint_key="dino_checkpoint_path",
                extra_cli_flags={"--project": "leaf"},
            ),
            ("sam3", "petiole"): SegmentationModelSpec(
                variable_name="nersc-segmentation-options",
                settings=self.config.nersc_segment_sam3_settings,
                mlflow_model_name="sam3-petiole",
                mlflow_checkpoint_key="finetuned_checkpoint_path",
            ),
        }
        key = (model, project)
        if key not in registry:
            raise ValueError(
                f"No segmentation spec registered for model={model!r}, project={project!r}. "
                f"Registered combinations: {list(registry)}"
            )
        return registry[key]

    def reconstruct(
        self,
        file_path: str = "",
        num_nodes: int = 2,
    ) -> bool:
        """
        Use NERSC for tomography reconstruction

        :param file_path: Path to the file to reconstruct
        :param num_nodes: Number of nodes to use for parallel reconstruction
        :return: True if successful, False otherwise
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

        opts = _load_job_options("nersc-reconstruction-options", self.config.nersc_recon_settings)

        num_nodes = opts.get("num_nodes", num_nodes)
        cpus_per_task = opts["cpus-per-task"]
        qos = opts["qos"]
        account = opts["account"]
        reservation = opts.get("reservation", "")
        walltime = opts.get("walltime", "0:30:00")

        reservation_line = f"#SBATCH --reservation={reservation}" if reservation else ""


# If using with a reservation:
# SBATCH -q regular
# SBATCH --reservation=_CAP_MarchModCon_CPU
# SBATCH -A amsc006
        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        job_script = f"""#!/bin/bash
#SBATCH -q {qos}
#SBATCH -A {account}
{reservation_line}
#SBATCH -C cpu
#SBATCH --job-name=tomo_recon_{folder_name}_{file_name}
#SBATCH --output={pscratch_path}/tomo_recon_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/%x_%j.err
#SBATCH -N {num_nodes}
#SBATCH --ntasks={num_nodes}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --time={walltime}
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
        /bin/bash -c "cd /alsuser && python /opt/scripts/sfapi_reconstruction_multinode.py \
{file_name} {folder_name} $SINO_START $SINO_END" &

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
        """
        Fetch and parse timing data from the SLURM job.

        :param perlmutter: SFAPI compute object for Perlmutter
        :param pscratch_path: Path to the user's pscratch directory
        :param job_id: SLURM job ID
        :return: Dictionary with timing breakdown
        """
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

    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        """
        Use NERSC to make multiresolution version of tomography results.

        :param file_path: Path to the file to process
        :return: True if successful, False otherwise
        """

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

        # account = self.config.nersc_account

        opts = _load_job_options(
            "nersc-multiresolution-options", self.config.nersc_multiresolution_settings
        )

        qos = opts["qos"]
        account = opts["account"]
        cpus_per_task = opts["cpus-per-task"]
        reservation = opts.get("reservation", "")
        walltime = opts.get("walltime", "0:15:00")

        reservation_line = f"#SBATCH --reservation={reservation}" if reservation else ""

        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        job_script = f"""#!/bin/bash
#SBATCH -q {qos}
#SBATCH -A {account}
{reservation_line}
#SBATCH -C cpu
#SBATCH --job-name=tomo_multires_{folder_name}_{file_name}
#SBATCH --output={pscratch_path}/tomo_recon_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task {cpus_per_task}
#SBATCH --time={walltime}
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

    def segmentation_sam3(
        self,
        recon_folder_path: str = "",
        num_nodes: int = 4,
    ) -> dict:
        """
        Run SAM3 segmentation at NERSC Perlmutter (v6 with overlap + max confidence stitching).
        """
        logger.info("Starting NERSC segmentation process (inference_v6).")

        user = self.client.user()
        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"

        opts = _load_job_options(
            variable_name="nersc-segmentation-options",
            config_settings=self.config.nersc_segment_sam3_settings,
            config=self.config,
            mlflow_model_name="sam3-petiole",
            mlflow_checkpoint_key="finetuned_checkpoint_path",
        )

        cfs_path = opts["cfs_path"]
        conda_env_path = opts["conda_env_path"]
        seg_scripts_dir = opts["seg_scripts_dir"]
        checkpoints_dir = opts["checkpoints_dir"]
        bpe_path = opts["bpe_path"]
        original_checkpoint = opts["original_checkpoint_path"]
        ntasks_per_node = opts["ntasks-per-node"]
        gpus_per_node = opts["gpus-per-node"]
        cpus_per_task = opts["cpus-per-task"]
        num_nodes = opts.get("num_nodes", num_nodes)
        batch_size = opts["batch_size"]
        patch_size = opts["patch_size"]
        confidence = opts["confidence"]
        overlap = opts["overlap"]
        qos = opts["qos"]
        account = opts["account"]
        constraint = opts["constraint"]
        walltime = opts.get("walltime", "00:59:00")
        reservation = opts.get("reservation", "")
        script_name = opts.get("script_name", "src/inference_v6.py")

        prompts = opts["prompts"]
        if not isinstance(prompts, list) or not prompts:
            raise ValueError("nersc_segmentation_sam3.prompts must be a non-empty list")
        prompts_str = " ".join(f"'{p}'" for p in prompts)

        # "checkpoint" in the Prefect Variable is a bare filename that overrides
        # the config's finetuned_checkpoint_path. Config supplies the full path
        # as the default, so path construction is only needed when the variable
        # explicitly provides a different checkpoint filename.
        if "checkpoint" in opts and opts["checkpoint"] != Path(opts["finetuned_checkpoint_path"]).name:
            finetuned_checkpoint = f"{checkpoints_dir}/{opts['checkpoint']}"
        else:
            finetuned_checkpoint = opts["finetuned_checkpoint_path"]

        input_dir = f"{pscratch_path}/8.3.2/scratch/{recon_folder_path}"
        output_folder = recon_folder_path.replace('/rec', '/seg')
        output_dir = f"{pscratch_path}/8.3.2/scratch/{output_folder}/sam3"

        logger.info(f"Input directory: {input_dir}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Conda environment: {conda_env_path}")

        confidence_str = (
            " ".join(str(c) for c in confidence)
            if isinstance(confidence, list) else str(confidence)
        )
        reservation_line = f"#SBATCH --reservation={reservation}" if reservation else ""
        job_name = f"seg_{Path(recon_folder_path).name}"

        job_script = f"""#!/bin/bash
#SBATCH -q {qos}
#SBATCH -A {account}
{reservation_line}
#SBATCH -N {num_nodes}
#SBATCH -C {constraint} # gpu
#SBATCH --job-name={job_name}
#SBATCH --time={walltime}
#SBATCH --ntasks-per-node={ntasks_per_node}
#SBATCH --gpus-per-node={gpus_per_node}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --output={pscratch_path}/tomo_seg_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_seg_logs/%x_%j.err

# Get master node
export MASTER_ADDR=$(scontrol show hostnames $SLURM_JOB_NODELIST | head -n 1)
export MASTER_PORT=29500

# Load conda module and activate environment
module load conda
conda activate {conda_env_path}

HF_HOME_ROOT="{cfs_path}/.cache/huggingface"
mkdir -p "${{HF_HOME_ROOT}}/hub" "${{HF_HOME_ROOT}}/datasets"

export HF_HOME="${{HF_HOME_ROOT}}"
export HF_HUB_CACHE="${{HF_HOME_ROOT}}/hub"
export TRANSFORMERS_CACHE="${{HF_HUB_CACHE}}"
export HF_DATASETS_CACHE="${{HF_HOME_ROOT}}/datasets"

# prove what each rank sees
echo "[RANK=$SLURM_PROCID] HF_HOME=$HF_HOME"
echo "[RANK=$SLURM_PROCID] HF_HUB_CACHE=$HF_HUB_CACHE"

# Best-effort perms (ignore if not allowed)
chmod -R 2775 "{cfs_path}/tomography_segmentation_scripts/.cache" 2>/dev/null || true
chmod -R 2775 "${{HF_HOME_ROOT}}" 2>/dev/null || true

# Set parameters
export INPUT_DIR="{input_dir}"
export OUTPUT_DIR="{output_dir}"
export BATCH_SIZE={batch_size}

# Create output and log directories
mkdir -p ${{OUTPUT_DIR}}
mkdir -p {pscratch_path}/tomo_seg_logs

echo "============================================================"
echo "JOB STARTED: $(date)"
echo "============================================================"
echo "Master: $MASTER_ADDR:$MASTER_PORT"
echo "Nodes: $SLURM_JOB_NODELIST"
echo "Job ID: $SLURM_JOB_ID"
echo "GPUs: $((SLURM_NNODES * 4))"
echo "Input: ${{INPUT_DIR}}"
echo "Output: ${{OUTPUT_DIR}}"

# Count actual images
NUM_IMAGES=$(ls ${{INPUT_DIR}}/*.tif* 2>/dev/null | wc -l)
echo "Images to process: ${{NUM_IMAGES}}"
echo "============================================================"

# Record start time
START_TIME=$(date +%s)

# Change to script directory
cd {seg_scripts_dir}

# Run inference
export TORCH_DISTRIBUTED_DEBUG=DETAIL
export NCCL_DEBUG=INFO
export TORCH_NCCL_ASYNC_ERROR_HANDLING=1
export BPE_PATH="{bpe_path}"
export ORIG_CKPT="{original_checkpoint}"
export FT_CKPT="{finetuned_checkpoint}"

srun --ntasks-per-node=1 --gpus-per-task=4 \
  torchrun \
    --nnodes={num_nodes} \
    --nproc_per_node=4 \
    --rdzv_id=$SLURM_JOB_ID \
    --rdzv_backend=c10d \
    --rdzv_endpoint=$MASTER_ADDR:$MASTER_PORT \
    {script_name} \
    --input-dir "${{INPUT_DIR}}" \
    --output-dir "${{OUTPUT_DIR}}" \
    --patch-size {patch_size} \
    --batch-size "${{BATCH_SIZE}}" \
    --confidence {confidence_str} \
    --overlap-ratio {overlap} \
    --prompts {prompts_str} \
    --bpe-path "${{BPE_PATH}}" \
    --original-checkpoint "${{ORIG_CKPT}}" \
    --finetuned-checkpoint "${{FT_CKPT}}"

SEG_STATUS=$?

# Record end time and calculate duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

if [ $NUM_IMAGES -gt 0 ]; then
    TIME_PER_IMAGE=$(echo "scale=3; $DURATION / $NUM_IMAGES" | bc)
    THROUGHPUT=$(echo "scale=2; $NUM_IMAGES / $DURATION * 60" | bc)
else
    TIME_PER_IMAGE="N/A"
    THROUGHPUT="N/A"
fi

echo ""
echo "============================================================"
echo "JOB COMPLETED: $(date)"
echo "============================================================"
echo "Total time: ${{MINUTES}}m ${{SECONDS}}s (${{DURATION}}s)"
echo "Images processed: $NUM_IMAGES"
echo "Time per image: ${{TIME_PER_IMAGE}}s"
echo "Throughput: ${{THROUGHPUT}} images/minute"
echo "Results saved to: ${{OUTPUT_DIR}}"
echo "Exit status: $SEG_STATUS"
echo "============================================================"

# Set permissions
chmod -R 2775 ${{OUTPUT_DIR}} 2>/dev/null || true

exit $SEG_STATUS
"""

        try:
            logger.info("Submitting segmentation job to Perlmutter (v6).")
            perlmutter = self.client.compute(Machine.perlmutter)

            # Ensure directories exist
            logger.info("Creating necessary directories...")
            perlmutter.run(f"mkdir -p {pscratch_path}/tomo_seg_logs")
            perlmutter.run(f"mkdir -p {output_dir}")

            # Submit job
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            # Initial update
            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            # Wait briefly before polling
            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            # Wait for completion
            job.complete()
            logger.info("Segmentation job completed successfully.")

            # Fetch timing data from output file
            timing = self._fetch_seg_timing_from_output(perlmutter, pscratch_path, job.jobid, job_name)

            if timing:
                logger.info("=" * 60)
                logger.info("SEGMENTATION TIMING BREAKDOWN")
                logger.info("=" * 60)
                logger.info(f"  Total time:          {timing.get('total_time', 'N/A')}")
                logger.info(f"  Images processed:    {timing.get('num_images', 'N/A')}")
                logger.info(f"  Time per image:      {timing.get('time_per_image', 'N/A')}")
                logger.info(f"  Throughput:          {timing.get('throughput', 'N/A')} images/min")
                logger.info(f"  Exit status:         {timing.get('exit_status', 'N/A')}")
                logger.info("=" * 60)

            return {
                "success": True,
                "job_id": job.jobid,
                "timing": timing,
                "output_dir": output_dir
            }

        except Exception as e:
            logger.error(f"Error during segmentation job: {e}")
            import traceback
            logger.error(traceback.format_exc())

            # Attempt recovery
            match = re.search(r"Job not found:\s*(\d+)", str(e))
            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.compute(Machine.perlmutter).job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Segmentation job completed after recovery.")

                    timing = self._fetch_seg_timing_from_output(perlmutter, pscratch_path, jobid, job_name)
                    return {
                        "success": True,
                        "job_id": jobid,
                        "timing": timing,
                        "output_dir": output_dir
                    }
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")

            return {
                "success": False,
                "job_id": None,
                "timing": None,
                "output_dir": None
            }

    def segmentation_dinov3(
        self,
        recon_folder_path: str = "",
        project: str = "petiole",
    ) -> bool:
        """
        Run DINOv3 segmentation at NERSC Perlmutter via SFAPI Slurm job.

        :param recon_folder_path: Relative path to the reconstructed data folder,
               e.g. 'folder_name/recYYYYMMDD_hhmmss_scanname/'
        :param project: Project name for segmentation settings.
        :return: True if the job completed successfully, False otherwise.
        """
        logger.info("Starting NERSC DINOv3 segmentation process.")

        user = self.client.user()
        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"

        # Load from config
        spec = self._get_segmentation_spec("dinov3", project)
        opts = _load_job_options(
            variable_name=spec.variable_name,
            config_settings=spec.settings,
            config=self.config,
            mlflow_model_name=spec.mlflow_model_name,
            mlflow_checkpoint_key=spec.mlflow_checkpoint_key,
        )

        extra_flags = "\n".join(
            f"    {flag} {value} \\" for flag, value in spec.extra_cli_flags.items()
        )

        cfs_path = opts["cfs_path"]
        conda_env_path = opts["conda_env_path"]
        seg_scripts_dir = opts["seg_scripts_dir"]
        dino_checkpoint = opts["dino_checkpoint_path"]
        ntasks_per_node = opts["ntasks-per-node"]
        gpus_per_node = opts["gpus-per-node"]
        cpus_per_task = opts["cpus-per-task"]
        batch_size = opts["batch_size"]
        num_nodes = opts["num_nodes"]
        nproc_per_node = opts["nproc_per_node"]
        qos = opts["qos"]
        account = opts["account"]
        constraint = opts["constraint"]
        walltime = opts.get("walltime", "00:59:00")
        reservation = opts.get("reservation", "")
        script_name = opts.get("script_name", "src.inference_dino_v1")

        input_dir = f"{pscratch_path}/8.3.2/scratch/{recon_folder_path}"
        seg_folder = recon_folder_path.replace("/rec", "/seg")
        output_dir = f"{pscratch_path}/8.3.2/scratch/{seg_folder}/dino"

        logger.info(f"DINOv3 input dir:  {input_dir}")
        logger.info(f"DINOv3 output dir: {output_dir}")

        reservation_line = f"#SBATCH --reservation={reservation}" if reservation else ""
        job_name = f"dino_{Path(recon_folder_path).name}"

        job_script = f"""#!/bin/bash
#SBATCH -q {qos}
#SBATCH -A {account}
#SBATCH -N {num_nodes}
#SBATCH -C {constraint}
{reservation_line}
#SBATCH --job-name={job_name}
#SBATCH --time={walltime}
#SBATCH --ntasks-per-node={ntasks_per_node}
#SBATCH --gpus-per-node={gpus_per_node}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --output={pscratch_path}/tomo_seg_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_seg_logs/%x_%j.err

export MASTER_ADDR=$(scontrol show hostnames $SLURM_JOB_NODELIST | head -n 1)
export MASTER_PORT=29500

module load conda
conda activate {conda_env_path}

HF_HOME_ROOT="{cfs_path}/.cache/huggingface"
mkdir -p "${{HF_HOME_ROOT}}/hub" "${{HF_HOME_ROOT}}/datasets"
export HF_HOME="${{HF_HOME_ROOT}}"
export HF_HUB_CACHE="${{HF_HOME_ROOT}}/hub"
export TRANSFORMERS_CACHE="${{HF_HUB_CACHE}}"
export HF_DATASETS_CACHE="${{HF_HOME_ROOT}}/datasets"

chmod -R 2775 "{cfs_path}/tomography_segmentation_scripts/.cache" 2>/dev/null || true
chmod -R 2775 "${{HF_HOME_ROOT}}" 2>/dev/null || true

mkdir -p {output_dir}
mkdir -p {pscratch_path}/tomo_seg_logs

echo "============================================================"
echo "DINOv3 SEGMENTATION STARTED: $(date)"
echo "============================================================"
echo "Master: $MASTER_ADDR:$MASTER_PORT"
echo "Nodes: $SLURM_JOB_NODELIST"
echo "Job ID: $SLURM_JOB_ID"
echo "Input:  {input_dir}"
echo "Output: {output_dir}"
echo "Parameters: batch_size={batch_size}"
echo "============================================================"

NUM_IMAGES=$(ls {input_dir}/*.tif* 2>/dev/null | wc -l)
echo "Images to process: ${{NUM_IMAGES}}"

START_TIME=$(date +%s)

cd {seg_scripts_dir}

export TORCH_DISTRIBUTED_DEBUG=DETAIL
export NCCL_DEBUG=INFO
export TORCH_NCCL_ASYNC_ERROR_HANDLING=1

srun --ntasks-per-node=1 --gpus-per-task=4 \\
  torchrun \\
    --nnodes={num_nodes} \\
    --nproc_per_node={nproc_per_node} \\
    --rdzv_id=$SLURM_JOB_ID \\
    --rdzv_backend=c10d \\
    --rdzv_endpoint=$MASTER_ADDR:$MASTER_PORT \\
    -m {script_name} \\
    --input-dir "{input_dir}" \\
    --output-dir "{output_dir}" \\
    --batch-size {batch_size} \\
    --finetuned-checkpoint "{dino_checkpoint}" \\
    {extra_flags}
    --save-overlay

SEG_STATUS=$?

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo "============================================================"
echo "DINOv3 SEGMENTATION COMPLETED: $(date)"
echo "============================================================"
echo "Total time: ${{MINUTES}}m ${{SECONDS}}s (${{DURATION}}s)"
echo "Images processed: ${{NUM_IMAGES}}"
echo "Exit status: $SEG_STATUS"
echo "============================================================"

chmod -R 2775 {output_dir} 2>/dev/null || true

exit $SEG_STATUS
"""
        try:
            logger.info("Submitting DINOv3 segmentation job to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()
            logger.info("DINOv3 segmentation job completed successfully.")
            return True

        except Exception as e:
            logger.error(f"Error during DINOv3 segmentation job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))
            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.compute(Machine.perlmutter).job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("DINOv3 segmentation job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                return False

    def combine_segmentations(
        self,
        recon_folder_path: str = "",
    ) -> bool:
        """
        Run CPU-based combination of SAM3+DINOv3 segmentation results
        at NERSC Perlmutter via SFAPI Slurm job.

        :param recon_folder_path: Relative path to the reconstructed data folder,
               e.g. 'folder_name/recYYYYMMDD_hhmmss_scanname/'
        :return: True if the job completed successfully, False otherwise.
        """
        logger.info("Starting NERSC segmentation combination process.")

        user = self.client.user()
        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"

        opts = _load_job_options(
            "nersc-combine-seg-options", self.config.nersc_combine_segmentation_settings
        )

        conda_env_path = opts["conda_env_path"]
        seg_scripts_dir = opts["seg_scripts_dir"]
        num_nodes = opts["num_nodes"]
        qos = opts["qos"]
        account = opts["account"]
        constraint = opts["constraint"]
        walltime = opts.get("walltime", "01:00:00")
        dilate_px = opts["dilate_px"]
        reservation = opts.get("reservation", "")
        script_name = opts.get("script_name", "src.combine_sam_dino_v3")

        seg_folder = recon_folder_path.replace("/rec", "/seg")
        input_dir = f"{pscratch_path}/8.3.2/scratch/{recon_folder_path}"
        seg_base = f"{pscratch_path}/8.3.2/scratch/{seg_folder}"

        sam3_results = f"{seg_base}/sam3"
        dinov3_results = f"{seg_base}/dino"
        combined_output = f"{seg_base}/combined"

        logger.info(f"Combine input dir:  {input_dir}")
        logger.info(f"Combine output dir: {combined_output}")

        reservation_line = f"#SBATCH --reservation={reservation}" if reservation else ""
        job_name = f"combine_{Path(recon_folder_path).name}"

        job_script = f"""#!/bin/bash
#SBATCH -q {qos}
#SBATCH -A {account}
{reservation_line}
#SBATCH -N {num_nodes}
#SBATCH -C {constraint}
#SBATCH --job-name={job_name}
#SBATCH --time={walltime}
#SBATCH --ntasks={opts["ntasks"]}
#SBATCH --cpus-per-task={opts["cpus-per-task"]}
#SBATCH --output={pscratch_path}/tomo_seg_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_seg_logs/%x_%j.err

module load conda
conda activate {conda_env_path}

mkdir -p {combined_output}/sam_dino
mkdir -p {pscratch_path}/tomo_seg_logs

echo "============================================================"
echo "SEGMENTATION COMBINATION STARTED: $(date)"
echo "============================================================"
echo "Input:    {input_dir}"
echo "SAM3:     {sam3_results}"
echo "DINOv3:     {dinov3_results}"
echo "Output:   {combined_output}"
echo "Dilate:   {dilate_px}px"
echo "============================================================"

START_TIME=$(date +%s)

cd {seg_scripts_dir}

echo "--- Running SAM3 + DINOv3 combination ---"
python -m {script_name} \\
    --input-dir "{input_dir}" \\
    --instance-masks-dir "{sam3_results}" \\
    --semantic-masks-dir "{dinov3_results}/semantic_masks" \\
    --output-dir "{combined_output}/sam_dino" \\
    --dilate-px {dilate_px} \\
    --save-extracted \\
    --dino-trust Cortex Phloem_Fibers Phloem Air-based_Pith_cells Water-based_Pith_cells

SAM_DINO_STATUS=$?
echo "SAM3+DINOv3 exit status: $SAM_DINO_STATUS"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo "============================================================"
echo "SEGMENTATION COMBINATION COMPLETED: $(date)"
echo "============================================================"
echo "Total time: ${{MINUTES}}m ${{SECONDS}}s (${{DURATION}}s)"
echo "SAM3+DINOv3 status:     $SAM_DINO_STATUS"
echo "============================================================"

chmod -R 2775 {combined_output} 2>/dev/null || true

if [ $SAM_DINO_STATUS -ne 0 ]; then
    exit 1
fi
exit 0
"""
        try:
            logger.info("Submitting segmentation combination job to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()
            logger.info("Segmentation combination job completed successfully.")
            return True

        except Exception as e:
            logger.error(f"Error during segmentation combination job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))
            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.compute(Machine.perlmutter).job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Segmentation combination job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                return False

    def _fetch_seg_timing_from_output(self, perlmutter, pscratch_path: str, job_id: str, job_name: str) -> dict:
        """
        Fetch and parse timing data from the SLURM output file.

        :param perlmutter: SFAPI compute object for Perlmutter
        :param pscratch_path: Path to the user's pscratch directory
        :param job_id: SLURM job ID
        :param job_name: Job name for finding output file
        :return: Dictionary with timing breakdown
        """
        output_file = f"{pscratch_path}/tomo_seg_logs/{job_name}_{job_id}.out"

        try:
            # Use SFAPI to read the output file
            result = perlmutter.run(f"cat {output_file}")

            # Handle different result types
            if isinstance(result, str):
                output = result
            elif hasattr(result, 'output'):
                output = result.output
            elif hasattr(result, 'stdout'):
                output = result.stdout
            else:
                output = str(result)

            logger.info("Job output file contents (last 50 lines):")
            lines = output.strip().split('\n')
            for line in lines[-50:]:
                logger.info(f"  {line}")

            # Parse timing data from the output
            timing = {}

            for line in lines:
                if "Total time:" in line:
                    # Extract: "Total time: 5m 23s (323s)"
                    match = re.search(r'(\d+)m\s+(\d+)s\s+\((\d+)s\)', line)
                    if match:
                        timing['total_time'] = f"{match.group(1)}m {match.group(2)}s"
                        timing['total_seconds'] = int(match.group(3))

                elif "Images processed:" in line:
                    # Extract: "Images processed: 100"
                    match = re.search(r'Images processed:\s+(\d+)', line)
                    if match:
                        timing['num_images'] = int(match.group(1))

                elif "Time per image:" in line:
                    # Extract: "Time per image: 3.230s"
                    match = re.search(r'Time per image:\s+([\d.]+)s', line)
                    if match:
                        timing['time_per_image'] = f"{match.group(1)}s"

                elif "Throughput:" in line:
                    # Extract: "Throughput: 18.58 images/minute"
                    match = re.search(r'Throughput:\s+([\d.]+)\s+images/minute', line)
                    if match:
                        timing['throughput'] = float(match.group(1))

                elif "Exit status:" in line:
                    # Extract: "Exit status: 0"
                    match = re.search(r'Exit status:\s+(\d+)', line)
                    if match:
                        timing['exit_status'] = int(match.group(1))

            return timing if timing else None

        except Exception as e:
            logger.warning(f"Error fetching timing data from output: {e}")
            import traceback
            logger.warning(traceback.format_exc())
            return None

    def start_streaming_service(
        self,
        walltime: datetime.timedelta = datetime.timedelta(minutes=30),
    ) -> str:
        return NerscStreamingMixin.start_streaming_service(
            self,
            client=self.client,
            walltime=walltime
        )

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
    num_nodes: Optional[int] = 4,
    config: Optional[Config832] = None,
) -> bool:
    """
    Perform multi-node tomography reconstruction on NERSC.

    :param file_path: Path to the file to reconstruct.
    :param num_nodes: Number of nodes to use for reconstruction.
    :param config: Configuration object (if None, a default Config832 will be created).
    :return: True if successful, False otherwise.
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

    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem

    tiff_file_path = f"{folder_name}/rec{file_name}"
    zarr_file_path = f"{folder_name}/rec{file_name}.zarr"

    logger.info(f"{tiff_file_path=}")
    logger.info(f"{zarr_file_path=}")

    if num_nodes is None:
        num_nodes = config.nersc_recon_settings.get("num_nodes", 4)
    logger.info(f"Configured to use {num_nodes} nodes for reconstruction")

    logger.info(f"Using multi-node reconstruction with {num_nodes} nodes")
    nersc_reconstruction_success = controller.reconstruct(
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
            logger.info(
                f"  File copy:           {timing.get('file_copy', 'N/A')}s "
                f"(skipped: {timing.get('copy_skipped', 'N/A')})"
            )
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

    logger.info("Scheduling reconstruction transfers from pscratch to CFS and data832.")
    pscratch_to_cfs_tiff_future = globus_transfer_task.submit(
        file_path=tiff_file_path,
        source=config.nersc832_alsdev_pscratch_scratch,
        destination=config.nersc832_alsdev_scratch,
        config=config,
    )
    pscratch_to_data832_tiff_future = globus_transfer_task.submit(
        file_path=tiff_file_path,
        source=config.nersc832_alsdev_pscratch_scratch,
        destination=config.data832_scratch,
        config=config,
    )

    logger.info("Building multi-resolution Zarrs.")
    nersc_multi_res_success = controller.build_multi_resolution(
        file_path=file_path,
    )
    logger.info(f"NERSC multi-resolution success: {nersc_multi_res_success}")

    logger.info("Scheduling Zarr transfers from pscratch to CFS and data832.")
    pscratch_to_cfs_zarr_future = globus_transfer_task.submit(
        file_path=zarr_file_path,
        source=config.nersc832_alsdev_pscratch_scratch,
        destination=config.nersc832_alsdev_scratch,
        config=config,
    )
    pscratch_to_data832_zarr_future = globus_transfer_task.submit(
        file_path=zarr_file_path,
        source=config.nersc832_alsdev_pscratch_scratch,
        destination=config.data832_scratch,
        config=config,
    )

    # Resolve before pruning (which needs to know what landed where)
    pscratch_to_cfs_tiff_future.result()
    pscratch_to_cfs_zarr_future.result()
    pscratch_to_data832_tiff_future.result()
    pscratch_to_data832_zarr_future.result()
    logger.info("All transfers complete.")

    logger.info("Scheduling pruning tasks.")
    schedule_pruning(
        config=config,
        raw_file_path=file_path,
        tiff_file_path=tiff_file_path,
        zarr_file_path=zarr_file_path
    )

    # TODO: Ingest into SciCat
    if nersc_reconstruction_success:
        return True
    else:
        return False


@flow(name="nersc_petiole_segment_flow",
      flow_run_name="nersc_petiole_seg-{file_path}")
def nersc_petiole_segment_flow(
    file_path: str,
    config: Optional[Config832] = None,
    num_nodes: Optional[int] = None,
) -> bool:
    """
    Transfer raw data to NERSC, run reconstruction, then run SAM3 and DINOv3
    segmentation concurrently, followed by a combination step.

    :param file_path: The path to the file to be processed.
    :param config: Configuration object for the flow.
    :param num_nodes: Number of nodes for reconstruction.
    :return: True if reconstruction and at least one segmentation task succeeded.
    """
    logger = get_run_logger()

    if config is None:
        logger.info("Initializing Config")
        config = Config832()

    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem
    scratch_path_tiff = f"{folder_name}/rec{file_name}"
    scratch_path_segment = f"{folder_name}/seg{file_name}"

    logger.info(f"Starting NERSC reconstruction + multi-segmentation flow for {file_path=}")
    logger.info(f"Reconstructed TIFFs will be at: {scratch_path_tiff}")
    logger.info(f"Segmented output will be at: {scratch_path_segment}")

    controller = get_controller(hpc_type=HPC.NERSC, config=config)
    logger.info("NERSC controller initialized")

    if num_nodes is None:
        num_nodes = config.nersc_recon_settings.get("num_nodes", 4)
    logger.info(f"Configured to use {num_nodes} nodes for reconstruction")

    nersc_reconstruction_success = False
    sam3_success = False
    dinov3_success = False
    data832_tiff_future = None
    data832_sam3_future = None
    data832_dinov3_future = None
    data832_combined_future = None
    data832_tiff_transfer_success = False
    data832_sam3_transfer_success = False
    data832_dinov3_transfer_success = False
    data832_combined_transfer_success = False

    # ── STEP 1: Multinode Reconstruction ─────────────────────────────────────
    logger.info(f"Using multi-node reconstruction with {num_nodes} nodes")
    recon_result = controller.reconstruct(
        file_path=file_path,
        num_nodes=num_nodes
    )

    if isinstance(recon_result, dict):
        nersc_reconstruction_success = recon_result.get('success', False)
        timing = recon_result.get('timing')
        if timing:
            logger.info("=" * 50)
            logger.info("TIMING BREAKDOWN")
            logger.info("=" * 50)
            logger.info(f"  Total job time:      {timing.get('total', 'N/A')}s")
            logger.info(f"  Container pull:      {timing.get('container_pull', 'N/A')}s")
            logger.info(
                f"  File copy:           {timing.get('file_copy', 'N/A')}s "
                f"(skipped: {timing.get('copy_skipped', 'N/A')})"
            )
            logger.info(f"  Metadata detection:  {timing.get('metadata', 'N/A')}s")
            logger.info(f"  RECONSTRUCTION:      {timing.get('reconstruction', 'N/A')}s  <-- actual recon time")
            logger.info(f"  Num slices:          {timing.get('num_slices', 'N/A')}")
            logger.info("=" * 50)
            if all(k in timing for k in ['total', 'reconstruction']):
                overhead = timing['total'] - timing['reconstruction']
                logger.info(f"  Overhead:            {overhead}s")
                logger.info(f"  Reconstruction %:    {100 * timing['reconstruction'] / timing['total']:.1f}%")
            logger.info("=" * 50)
    else:
        nersc_reconstruction_success = recon_result

    logger.info(f"NERSC reconstruction success: {nersc_reconstruction_success}")

    if not nersc_reconstruction_success:
        logger.error("Reconstruction Failed.")
        raise ValueError("Reconstruction at NERSC Failed")

    logger.info("Reconstruction Successful.")

    # ── STEP 2: Transfer TIFFs to data832 ────────────────────────────────────
    logger.info("Transferring reconstructed TIFFs from NERSC pscratch to data832")
    try:
        data832_tiff_future = globus_transfer_task.submit(
            file_path=scratch_path_tiff,
            source=config.nersc832_alsdev_pscratch_scratch,
            destination=config.data832_scratch,
            config=config,
        )
        logger.info("TIFF transfer to data832 submitted.")
    except Exception as e:
        logger.error(f"Failed to transfer TIFFs to data832: {e}")
        data832_tiff_transfer_success = False

    # ── STEP 3: SAM3 / DINOv3 ──────────────────────────
    logger.info("Submitting SAM3 and DINOv3 segmentation tasks concurrently.")

    sam3_future = nersc_segmentation_sam3_task.submit(
        recon_folder_path=scratch_path_tiff, config=config
    )
    dinov3_future = nersc_segmentation_dinov3_task.submit(
        recon_folder_path=scratch_path_tiff, config=config, project="petiole"
    )

    # ── STEP 4: Transfer each model's output as it completes ─────────────────
    sam3_result = sam3_future.result()
    sam3_success = sam3_result.get('success', False) if isinstance(sam3_result, dict) else bool(sam3_result)
    logger.info(f"SAM3 segmentation result: {sam3_success}")
    if sam3_success:
        logger.info("Transferring SAM3 segmentation outputs to data832")
        sam3_segment_path = f"{folder_name}/seg{file_name}/sam3"
        try:
            data832_sam3_future = globus_transfer_task.submit(
                file_path=sam3_segment_path,
                source=config.nersc832_alsdev_pscratch_scratch,
                destination=config.data832_scratch,
                config=config,
            )
            logger.info("SAM3 transfer to data832 submitted")
        except Exception as e:
            logger.error(f"Failed to transfer SAM3 outputs to data832: {e}")

    dinov3_success = dinov3_future.result()
    logger.info(f"DINOv3 segmentation result: {dinov3_success}")
    if dinov3_success:
        logger.info("Transferring DINOv3 segmentation outputs to data832")
        dinov3_segment_path = f"{folder_name}/seg{file_name}/dino"
        try:
            data832_dinov3_future = globus_transfer_task.submit(
                file_path=dinov3_segment_path,
                source=config.nersc832_alsdev_pscratch_scratch,
                destination=config.data832_scratch,
                config=config,
            )
            logger.info("DINOv3 transfer to data832 submitted")
        except Exception as e:
            logger.error(f"Failed to transfer DINOv3 outputs to data832: {e}")

    any_seg_success = any([sam3_success, dinov3_success])

    logger.info(f"Segmentation results — SAM3: {sam3_success}, DINOv3: {dinov3_success}")

    # ── STEP 5: Combine Segmentations (after SAM3+DINOv3 complete) ──
    if dinov3_success and sam3_success:
        logger.info("Running segmentation combination.")

        combine_future = nersc_combine_segmentations_task.submit(
            recon_folder_path=scratch_path_tiff, config=config
        )

        combine_success = combine_future.result()
        logger.info(f"Combination result: {combine_success}")
        if combine_success:
            logger.info("Transferring combined segmentation outputs to data832")
            combined_segment_path = f"{folder_name}/seg{file_name}/combined/sam_dino"
            try:
                data832_combined_future = globus_transfer_task.submit(
                    file_path=combined_segment_path,
                    source=config.nersc832_alsdev_pscratch_scratch,
                    destination=config.data832_scratch,
                    config=config,
                )
                logger.info("Combined transfer to data832 submitted")
            except Exception as e:
                logger.error(f"Failed to transfer combined outputs to data832: {e}")

    else:
        logger.warning("Skipping combination and extraction: requires DINO plus SAM3.")

    logger.info("Copying rec and seg folders from pscratch to NERSC CFS.")
    for cfs_path in [scratch_path_tiff, scratch_path_segment]:
        try:
            globus_transfer_task.submit(
                file_path=cfs_path,
                source=config.nersc832_alsdev_pscratch_scratch,
                destination=config.nersc832_alsdev_scratch,
                config=config,
            )
            logger.info(f"CFS transfer submitted: {cfs_path}")
        except Exception as e:
            logger.error(f"Failed to copy {cfs_path} to NERSC CFS: {e}")

    # ── Resolve all data832 futures before pruning ────────────────────────────
    data832_tiff_transfer_success = data832_tiff_future.result() if data832_tiff_future else False
    data832_sam3_transfer_success = data832_sam3_future.result() if data832_sam3_future else False
    data832_dinov3_transfer_success = data832_dinov3_future.result() if data832_dinov3_future else False
    data832_combined_transfer_success = data832_combined_future.result() if data832_combined_future else False

    logger.info(
        f"Transfer results — tiff: {data832_tiff_transfer_success}, "
        f"sam3: {data832_sam3_transfer_success}, dino: {data832_dinov3_transfer_success}, "
        f"combined: {data832_combined_transfer_success}"
    )

    # ── STEP 6: Pruning ───────────────────────────────────────────────────────
    logger.info("Scheduling file pruning tasks.")
    prune_controller = get_prune_controller(prune_type=PruneMethod.GLOBUS, config=config)

    try:
        prune_controller.prune(
            file_path=f"{folder_name}/{path.name}",
            source_endpoint=config.nersc832_alsdev_pscratch_raw,
            check_endpoint=None,
            days_from_now=1.0
        )
    except Exception as e:
        logger.warning(f"Failed to schedule raw data pruning: {e}")

    if nersc_reconstruction_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_tiff,
                source_endpoint=config.nersc832_alsdev_pscratch_scratch,
                check_endpoint=config.data832_scratch if data832_tiff_transfer_success else None,
                days_from_now=1.0
            )
        except Exception as e:
            logger.warning(f"Failed to schedule reconstruction data pruning: {e}")

    if any_seg_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_segment,
                source_endpoint=config.nersc832_alsdev_pscratch_scratch,
                check_endpoint=config.data832_scratch if any([
                    data832_sam3_transfer_success,
                    data832_dinov3_transfer_success,
                ]) else None,
                days_from_now=1.0
            )
        except Exception as e:
            logger.warning(f"Failed to schedule segmentation data pruning: {e}")

    if data832_tiff_transfer_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_tiff,
                source_endpoint=config.data832_scratch,
                check_endpoint=None,
                days_from_now=30.0
            )
        except Exception as e:
            logger.warning(f"Failed to schedule data832 tiff pruning: {e}")

    if any([data832_sam3_transfer_success,
            data832_dinov3_transfer_success,
            data832_combined_transfer_success]):
        try:
            prune_controller.prune(
                file_path=scratch_path_segment,
                source_endpoint=config.data832_scratch,
                check_endpoint=None,
                days_from_now=30.0
            )
        except Exception as e:
            logger.warning(f"Failed to schedule data832 segment pruning: {e}")

    if nersc_reconstruction_success and any_seg_success:
        logger.info("NERSC reconstruction + multi-segmentation flow completed successfully.")
        return True
    else:
        logger.warning(
            f"Flow completed with issues: recon={nersc_reconstruction_success}, "
            f"sam3={sam3_success}, dinov3={dinov3_success}"
        )
        return False


@flow(name="nersc_moon_segment_flow", flow_run_name="nersc_moon_seg-{file_path}")
def nersc_moon_segment_flow(
    file_path: str,
    config: Config832 | None = None,
    num_nodes: int | None = None,
) -> bool:
    """Reconstruct a lunar regolith scan and run DINOv3-moon segmentation.

    Runs reconstruction then DINOv3-moon (ice, particles, pores). No SAM3 or
    combine step — those are petiole-specific. Transfer and pruning follow the
    same pattern as nersc_petiole_segment_flow.

    :param file_path: Path to the raw .h5 file to be processed.
    :param config: Configuration object for the flow.
    :param num_nodes: Number of nodes for reconstruction.
    :return: True if reconstruction and segmentation both succeeded.
    """
    logger = get_run_logger()

    if config is None:
        logger.info("Initializing Config")
        config = Config832()

    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem
    scratch_path_tiff = f"{folder_name}/rec{file_name}"
    scratch_path_segment = f"{folder_name}/seg{file_name}"

    logger.info(f"Starting NERSC reconstruction + DINOv3-moon flow for {file_path=}")

    controller = get_controller(hpc_type=HPC.NERSC, config=config)

    if num_nodes is None:
        num_nodes = config.nersc_recon_settings.get("num_nodes", 4)
    logger.info(f"Configured to use {num_nodes} nodes for reconstruction")

    # ── STEP 1: Reconstruction ────────────────────────────────────────────────
    recon_result = controller.reconstruct(file_path=file_path, num_nodes=num_nodes)

    if isinstance(recon_result, dict):
        nersc_reconstruction_success = recon_result.get("success", False)
        timing = recon_result.get("timing")
        if timing:
            logger.info("=" * 50)
            logger.info("TIMING BREAKDOWN")
            logger.info("=" * 50)
            logger.info(f"  Total job time:      {timing.get('total', 'N/A')}s")
            logger.info(f"  Container pull:      {timing.get('container_pull', 'N/A')}s")
            logger.info(
                f"  File copy:           {timing.get('file_copy', 'N/A')}s "
                f"(skipped: {timing.get('copy_skipped', 'N/A')})"
            )
            logger.info(f"  Metadata detection:  {timing.get('metadata', 'N/A')}s")
            logger.info(f"  RECONSTRUCTION:      {timing.get('reconstruction', 'N/A')}s  <-- actual recon time")
            logger.info(f"  Num slices:          {timing.get('num_slices', 'N/A')}")
            logger.info("=" * 50)
            if all(k in timing for k in ["total", "reconstruction"]):
                overhead = timing["total"] - timing["reconstruction"]
                logger.info(f"  Overhead:            {overhead}s")
                logger.info(f"  Reconstruction %:    {100 * timing['reconstruction'] / timing['total']:.1f}%")
            logger.info("=" * 50)
    else:
        nersc_reconstruction_success = recon_result

    logger.info(f"NERSC reconstruction success: {nersc_reconstruction_success}")

    if not nersc_reconstruction_success:
        logger.error("Reconstruction failed — aborting moon segmentation flow.")
        raise ValueError("Reconstruction at NERSC failed")

    # ── STEP 2: Transfer TIFFs to data832 ────────────────────────────────────
    data832_tiff_future = None
    try:
        data832_tiff_future = globus_transfer_task.submit(
            file_path=scratch_path_tiff,
            source=config.nersc832_alsdev_pscratch_scratch,
            destination=config.data832_scratch,
            config=config,
        )
        logger.info("TIFF transfer to data832 submitted.")
    except Exception as e:
        logger.error(f"Failed to submit TIFF transfer to data832: {e}")

    # ── STEP 3: DINOv3-moon segmentation ─────────────────────────────────────
    logger.info("Submitting DINOv3-moon segmentation task.")
    moon_future = nersc_segmentation_dinov3_task.submit(
        recon_folder_path=scratch_path_tiff, config=config, project="moon"
    )

    moon_success = moon_future.result()
    logger.info(f"DINOv3-moon segmentation result: {moon_success}")

    # ── STEP 4: Transfer segmentation outputs to data832 ─────────────────────
    data832_moon_future = None
    if moon_success:
        moon_segment_path = f"{folder_name}/seg{file_name}/dino"
        try:
            data832_moon_future = globus_transfer_task.submit(
                file_path=moon_segment_path,
                source=config.nersc832_alsdev_pscratch_scratch,
                destination=config.data832_scratch,
                config=config,
            )
            logger.info("DINOv3-moon transfer to data832 submitted.")
        except Exception as e:
            logger.error(f"Failed to submit DINOv3-moon transfer to data832: {e}")

    # ── STEP 5: Copy to NERSC CFS ─────────────────────────────────────────────
    for cfs_path in [scratch_path_tiff, scratch_path_segment]:
        try:
            globus_transfer_task.submit(
                file_path=cfs_path,
                source=config.nersc832_alsdev_pscratch_scratch,
                destination=config.nersc832_alsdev_scratch,
                config=config,
            )
            logger.info(f"CFS transfer submitted: {cfs_path}")
        except Exception as e:
            logger.error(f"Failed to copy {cfs_path} to NERSC CFS: {e}")

    # ── Resolve futures before pruning ────────────────────────────────────────
    data832_tiff_transfer_success = data832_tiff_future.result() if data832_tiff_future else False
    data832_moon_transfer_success = data832_moon_future.result() if data832_moon_future else False

    logger.info(
        f"Transfer results — tiff: {data832_tiff_transfer_success}, "
        f"moon: {data832_moon_transfer_success}"
    )

    # ── STEP 6: Pruning ───────────────────────────────────────────────────────
    logger.info("Scheduling file pruning tasks.")
    prune_controller = get_prune_controller(prune_type=PruneMethod.GLOBUS, config=config)

    try:
        prune_controller.prune(
            file_path=f"{folder_name}/{path.name}",
            source_endpoint=config.nersc832_alsdev_pscratch_raw,
            check_endpoint=None,
            days_from_now=1.0,
        )
    except Exception as e:
        logger.warning(f"Failed to schedule raw data pruning: {e}")

    try:
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.nersc832_alsdev_pscratch_scratch,
            check_endpoint=config.data832_scratch if data832_tiff_transfer_success else None,
            days_from_now=1.0,
        )
    except Exception as e:
        logger.warning(f"Failed to schedule reconstruction data pruning: {e}")

    if moon_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_segment,
                source_endpoint=config.nersc832_alsdev_pscratch_scratch,
                check_endpoint=config.data832_scratch if data832_moon_transfer_success else None,
                days_from_now=1.0,
            )
        except Exception as e:
            logger.warning(f"Failed to schedule segmentation data pruning: {e}")

    if data832_tiff_transfer_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_tiff,
                source_endpoint=config.data832_scratch,
                check_endpoint=None,
                days_from_now=30.0,
            )
        except Exception as e:
            logger.warning(f"Failed to schedule data832 tiff pruning: {e}")

    if data832_moon_transfer_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_segment,
                source_endpoint=config.data832_scratch,
                check_endpoint=None,
                days_from_now=30.0,
            )
        except Exception as e:
            logger.warning(f"Failed to schedule data832 moon segment pruning: {e}")

    if nersc_reconstruction_success and moon_success:
        logger.info("NERSC reconstruction + DINOv3-moon flow completed successfully.")
        return True
    else:
        logger.warning(
            f"Flow completed with issues: recon={nersc_reconstruction_success}, moon={moon_success}"
        )
        return False


@flow(name="nersc_leaf_segment_flow", flow_run_name="nersc_leaf_seg-{file_path}")
def nersc_leaf_segment_flow(
    file_path: str,
    config: Config832 | None = None,
    num_nodes: int | None = None,
) -> bool:
    """Reconstruct a leaf scan and run DINOv3-leaf segmentation.

    Runs reconstruction then DINOv3-leaf (leaf-specific). No SAM3 or
    combine step — those are petiole-specific. Transfer and pruning follow the
    same pattern as nersc_petiole_segment_flow.

    :param file_path: Path to the raw .h5 file to be processed.
    :param config: Configuration object for the flow.
    :param num_nodes: Number of nodes for reconstruction.
    :return: True if reconstruction and segmentation both succeeded.
    """
    logger = get_run_logger()

    if config is None:
        logger.info("Initializing Config")
        config = Config832()

    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem
    scratch_path_tiff = f"{folder_name}/rec{file_name}"
    scratch_path_segment = f"{folder_name}/seg{file_name}"

    logger.info(f"Starting NERSC reconstruction + DINOv3-leaf flow for {file_path=}")

    controller = get_controller(hpc_type=HPC.NERSC, config=config)

    if num_nodes is None:
        num_nodes = config.nersc_recon_settings.get("num_nodes", 4)
    logger.info(f"Configured to use {num_nodes} nodes for reconstruction")

    # ── STEP 1: Reconstruction ────────────────────────────────────────────────
    recon_result = controller.reconstruct(file_path=file_path, num_nodes=num_nodes)

    if isinstance(recon_result, dict):
        nersc_reconstruction_success = recon_result.get("success", False)
        timing = recon_result.get("timing")
        if timing:
            logger.info("=" * 50)
            logger.info("TIMING BREAKDOWN")
            logger.info("=" * 50)
            logger.info(f"  Total job time:      {timing.get('total', 'N/A')}s")
            logger.info(f"  Container pull:      {timing.get('container_pull', 'N/A')}s")
            logger.info(
                f"  File copy:           {timing.get('file_copy', 'N/A')}s "
                f"(skipped: {timing.get('copy_skipped', 'N/A')})"
            )
            logger.info(f"  Metadata detection:  {timing.get('metadata', 'N/A')}s")
            logger.info(f"  RECONSTRUCTION:      {timing.get('reconstruction', 'N/A')}s  <-- actual recon time")
            logger.info(f"  Num slices:          {timing.get('num_slices', 'N/A')}")
            logger.info("=" * 50)
            if all(k in timing for k in ["total", "reconstruction"]):
                overhead = timing["total"] - timing["reconstruction"]
                logger.info(f"  Overhead:            {overhead}s")
                logger.info(f"  Reconstruction %:    {100 * timing['reconstruction'] / timing['total']:.1f}%")
            logger.info("=" * 50)
    else:
        nersc_reconstruction_success = recon_result

    logger.info(f"NERSC reconstruction success: {nersc_reconstruction_success}")

    if not nersc_reconstruction_success:
        logger.error("Reconstruction failed — aborting leaf segmentation flow.")
        raise ValueError("Reconstruction at NERSC failed")

    # ── STEP 2: Transfer TIFFs to data832 ────────────────────────────────────
    data832_tiff_future = None
    try:
        data832_tiff_future = globus_transfer_task.submit(
            file_path=scratch_path_tiff,
            source=config.nersc832_alsdev_pscratch_scratch,
            destination=config.data832_scratch,
            config=config,
        )
        logger.info("TIFF transfer to data832 submitted.")
    except Exception as e:
        logger.error(f"Failed to submit TIFF transfer to data832: {e}")

    # ── STEP 3: DINOv3-leaf segmentation ─────────────────────────────────────
    logger.info("Submitting DINOv3-leaf segmentation task.")
    leaf_future = nersc_segmentation_dinov3_task.submit(
        recon_folder_path=scratch_path_tiff, config=config, project="leaf"
    )

    leaf_success = leaf_future.result()
    logger.info(f"DINOv3-leaf segmentation result: {leaf_success}")

    # ── STEP 4: Transfer segmentation outputs to data832 ─────────────────────
    data832_leaf_future = None
    if leaf_success:
        leaf_segment_path = f"{folder_name}/seg{file_name}/dino"
        try:
            data832_leaf_future = globus_transfer_task.submit(
                file_path=leaf_segment_path,
                source=config.nersc832_alsdev_pscratch_scratch,
                destination=config.data832_scratch,
                config=config,
            )
            logger.info("DINOv3-leaf transfer to data832 submitted.")
        except Exception as e:
            logger.error(f"Failed to submit DINOv3-leaf transfer to data832: {e}")

    # ── STEP 5: Copy to NERSC CFS ─────────────────────────────────────────────
    for cfs_path in [scratch_path_tiff, scratch_path_segment]:
        try:
            globus_transfer_task.submit(
                file_path=cfs_path,
                source=config.nersc832_alsdev_pscratch_scratch,
                destination=config.nersc832_alsdev_scratch,
                config=config,
            )
            logger.info(f"CFS transfer submitted: {cfs_path}")
        except Exception as e:
            logger.error(f"Failed to copy {cfs_path} to NERSC CFS: {e}")

    # ── Resolve futures before pruning ────────────────────────────────────────
    data832_tiff_transfer_success = data832_tiff_future.result() if data832_tiff_future else False
    data832_leaf_transfer_success = data832_leaf_future.result() if data832_leaf_future else False

    logger.info(
        f"Transfer results — tiff: {data832_tiff_transfer_success}, "
        f"leaf: {data832_leaf_transfer_success}"
    )

    # ── STEP 6: Pruning ───────────────────────────────────────────────────────
    logger.info("Scheduling file pruning tasks.")
    prune_controller = get_prune_controller(prune_type=PruneMethod.GLOBUS, config=config)

    try:
        prune_controller.prune(
            file_path=f"{folder_name}/{path.name}",
            source_endpoint=config.nersc832_alsdev_pscratch_raw,
            check_endpoint=None,
            days_from_now=1.0,
        )
    except Exception as e:
        logger.warning(f"Failed to schedule raw data pruning: {e}")

    try:
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.nersc832_alsdev_pscratch_scratch,
            check_endpoint=config.data832_scratch if data832_tiff_transfer_success else None,
            days_from_now=1.0,
        )
    except Exception as e:
        logger.warning(f"Failed to schedule reconstruction data pruning: {e}")

    if leaf_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_segment,
                source_endpoint=config.nersc832_alsdev_pscratch_scratch,
                check_endpoint=config.data832_scratch if data832_leaf_transfer_success else None,
                days_from_now=1.0,
            )
        except Exception as e:
            logger.warning(f"Failed to schedule segmentation data pruning: {e}")

    if data832_tiff_transfer_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_tiff,
                source_endpoint=config.data832_scratch,
                check_endpoint=None,
                days_from_now=30.0,
            )
        except Exception as e:
            logger.warning(f"Failed to schedule data832 tiff pruning: {e}")

    if data832_leaf_transfer_success:
        try:
            prune_controller.prune(
                file_path=scratch_path_segment,
                source_endpoint=config.data832_scratch,
                check_endpoint=None,
                days_from_now=30.0,
            )
        except Exception as e:
            logger.warning(f"Failed to schedule data832 leaf segment pruning: {e}")

    if nersc_reconstruction_success and leaf_success:
        logger.info("NERSC reconstruction + DINOv3-leaf flow completed successfully.")
        return True
    else:
        logger.warning(
            f"Flow completed with issues: recon={nersc_reconstruction_success}, leaf={leaf_success}"
        )
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
    )

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


@task(name="nersc_multiresolution_task")
def nersc_multiresolution_task(
    file_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Run multiresolution task at NERSC.

    :param file_path: Path to the reconstructed data folder to be processed.
    :param config: Configuration object for the flow.
    :return: True if the task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    if config is None:
        logger.info("No config provided, using default Config832.")
        config = Config832()

    # Initialize the Tomography Controller and run the segmentation
    logger.info("Initializing NERSC Tomography HPC Controller.")
    tomography_controller = get_controller(
        hpc_type=HPC.NERSC,
        config=config
    )
    logger.info(f"Starting NERSC multiresolution task for {file_path=}")
    nersc_multiresolution_success = tomography_controller.build_multi_resolution(
        file_path=file_path,
    )
    if not nersc_multiresolution_success:
        logger.error("Multiresolution Failed.")
    else:
        logger.info("Multiresolution Successful.")
    return nersc_multiresolution_success


@flow(name="nersc_multiresolution_integration_test", flow_run_name="nersc_multiresolution_integration_test")
def nersc_multiresolution_integration_test() -> bool:
    """
    Integration test for the NERSC multiresolution task.

    :return: True if the multiresolution task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info("Starting NERSC multiresolution integration test.")
    file_path = 'DD-00842_hexemer/20260213_155826_petiole49.h5'  # 'test'  #
    flow_success = nersc_multiresolution_task(
        file_path=file_path,
        config=Config832()
    )
    logger.info(f"Flow success: {flow_success}")
    return flow_success


@task(name="nersc_segmentation_sam3_task")
def nersc_segmentation_sam3_task(
    recon_folder_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Run segmentation task at NERSC.

    :param recon_folder_path: Path to the reconstructed data folder to be processed.
    :param config: Configuration object for the flow.
    :return: True if the task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    if config is None:
        logger.info("No config provided, using default Config832.")
        config = Config832()

    # Initialize the Tomography Controller and run the segmentation
    logger.info("Initializing NERSC Tomography HPC Controller.")
    tomography_controller = get_controller(
        hpc_type=HPC.NERSC,
        config=config
    )
    logger.info(f"Starting NERSC segmentation task for {recon_folder_path=}")
    nersc_segmentation_success = tomography_controller.segmentation_sam3(
        recon_folder_path=recon_folder_path,
    )
    if isinstance(nersc_segmentation_success, dict):
        success = nersc_segmentation_success["success"]
        logger.info(f"Segmentation success: {success}")
    else:
        success = bool(nersc_segmentation_success)
    if not success:
        logger.error("Segmentation Failed.")
    return nersc_segmentation_success


@task(name="nersc_segmentation_dinov3_task")
def nersc_segmentation_dinov3_task(
    recon_folder_path: str,
    config: Optional[Config832] = None,
    project: Optional[str] = "petiole",
) -> bool:
    logger = get_run_logger()
    if config is None:
        logger.info("No config provided, using default Config832.")
        config = Config832()
    tomography_controller = get_controller(hpc_type=HPC.NERSC, config=config)
    logger.info(f"Starting NERSC DINOv3 segmentation task for {recon_folder_path=}, {project=}")
    success = tomography_controller.segmentation_dinov3(recon_folder_path=recon_folder_path, project=project)
    if not success:
        logger.error("DINOv3 segmentation failed.")
    else:
        logger.info("DINOv3 segmentation successful.")
    return success


@task(name="nersc_combine_segmentations_task")
def nersc_combine_segmentations_task(
    recon_folder_path: str,
    config: Optional[Config832] = None,
) -> bool:
    logger = get_run_logger()
    if config is None:
        logger.info("No config provided, using default Config832.")
        config = Config832()
    tomography_controller = get_controller(hpc_type=HPC.NERSC, config=config)
    logger.info(f"Starting NERSC combine segmentations task for {recon_folder_path=}")
    success = tomography_controller.combine_segmentations(recon_folder_path=recon_folder_path)
    if not success:
        logger.error("Combine segmentations failed.")
    else:
        logger.info("Combine segmentations successful.")
    return success


@flow(name="nersc_segmentation_sam3_integration_test", flow_run_name="nersc_segmentation_sam3_integration_test")
def nersc_segmentation_sam3_integration_test() -> bool:
    """
    Integration test for the NERSC SAM3 segmentation task.

    :return: True if the segmentation task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info("Starting NERSC SAM3 segmentation integration test.")
    recon_folder_path = 'synaps-i/rec20211222_125057_petiole4'  # 'test'  #
    flow_success = nersc_segmentation_sam3_task(
        recon_folder_path=recon_folder_path,
        config=Config832()
    )
    logger.info(f"Flow success: {flow_success}")
    return flow_success


if __name__ == "__main__":
    nersc_segmentation_dinov3_task(
        recon_folder_path='dabramov/recleaf/',
        config=Config832(),
        project="leaf"
    )
