from concurrent.futures import Future
from pathlib import Path
import time
from typing import Optional

from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.variables import Variable

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.job_controller import get_controller, HPC, TomographyHPCController
from orchestration.prune_controller import get_prune_controller, PruneMethod
from orchestration.transfer_controller import get_transfer_controller, CopyMethod


class ALCFTomographyHPCController(TomographyHPCController):
    """
    Implementation of TomographyHPCController for ALCF. Methods here leverage Globus Compute for processing tasks.
    There is a @staticmethod wrapper for each compute task submitted via Globus Compute.
    Also, there is a shared wait_for_globus_compute_future method that waits for the task to complete.

    :param TomographyHPCController: Abstract class for tomography HPC controllers.
    """

    def __init__(
        self,
        config: Config832
    ) -> None:
        """
        Initialize the ALCF Tomography HPC Controller.

        :param config: Configuration object for the controller.
        """
        super().__init__(config)
        # Load allocation root from the Prefect JSON block
        # The block must be registered with the name "alcf-allocation-root-path"
        logger = get_run_logger()
        allocation_data = Variable.get("alcf-allocation-root-path", _sync=True)
        self.allocation_root = allocation_data.get("alcf-allocation-root-path")  # eagle/SYNAPS-I/
        if not self.allocation_root:
            raise ValueError("Allocation root not found in JSON block 'alcf-allocation-root-path'")
        logger.info(f"Allocation root loaded: {self.allocation_root}")

    def reconstruct(
        self,
        file_path: str = "",
    ) -> bool:
        """
        Run tomography reconstruction at ALCF through Globus Compute.

        :param file_path : Path to the file to be processed.
        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()
        file_name = Path(file_path).stem + ".h5"
        folder_name = Path(file_path).parent.name

        rundir = f"{self.allocation_root}/data/bl832/raw"
        recon_script = f"{self.allocation_root}/reconstruction/scripts/globus_reconstruction.py"

        gcc = Client(code_serialization_strategy=CombinedCode())

        # TODO: Update globus-compute-endpoint Secret block with the new endpoint UUID
        # We will probably have 2 endpoints, one for recon, one for segmentation
        with Executor(endpoint_id=Secret.load("globus-compute-endpoint").get(), client=gcc) as fxe:
            logger.info(f"Running Tomopy reconstruction on {file_name} at ALCF")
            future = fxe.submit(
                self._reconstruct_wrapper,
                rundir,
                recon_script,
                file_name,
                folder_name
            )
            result = self._wait_for_globus_compute_future(future, "reconstruction", check_interval=10)
            return result

    @staticmethod
    def _reconstruct_wrapper(
        rundir: str = "/eagle/SYNAPS-I/data/bl832/raw",
        script_path: str = "/eagle/SYNAPS-I/reconstruction/scripts/globus_reconstruction.py",
        h5_file_name: str = None,
        folder_path: str = None
    ) -> str:
        """
        Python function that wraps around the application call for Tomopy reconstruction on ALCF

        :param rundir: the directory on the eagle file system (ALCF) where the input data are located
        :param script_path: the path to the script that will run the reconstruction
        :param h5_file_name: the name of the h5 file to be reconstructed
        :param folder_path: the path to the folder containing the h5 file
        :return: confirmation message
        """
        import os
        import subprocess
        import time

        rec_start = time.time()

        # Move to directory where data are located
        os.chdir(rundir)

        # Run reconstruction.py
        command = f"python {script_path} {h5_file_name} {folder_path}"
        recon_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        rec_end = time.time()

        print(f"Reconstructed data in {folder_path}/{h5_file_name} in {rec_end-rec_start} seconds;\n {recon_res}")

        return (
            f"Reconstructed data specified in {folder_path} / {h5_file_name} in {rec_end-rec_start} seconds;\n"
            f"{recon_res}"
        )

    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        """
        Tiff to Zarr code that is executed using Globus Compute

        :param file_path: Path to the file to be processed.
        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()

        file_name = Path(file_path).stem
        folder_name = Path(file_path).parent.name

        tiff_scratch_path = f"{self.allocation_root}/data/scratch/{folder_name}/rec{file_name}/"
        raw_path = f"{self.allocation_root}/data/raw/{folder_name}/{file_name}.h5"

        iri_als_bl832_rundir = f"{self.allocation_root}/data/raw"
        iri_als_bl832_conversion_script = f"{self.allocation_root}/scripts/tiff_to_zarr.py"

        gcc = Client(code_serialization_strategy=CombinedCode())

        with Executor(endpoint_id=Secret.load("globus-compute-endpoint").get(), client=gcc) as fxe:
            logger.info(f"Running Tiff to Zarr on {raw_path} at ALCF")
            future = fxe.submit(
                self._build_multi_resolution_wrapper,
                iri_als_bl832_rundir,
                iri_als_bl832_conversion_script,
                tiff_scratch_path,
                raw_path
            )
            result = self._wait_for_globus_compute_future(future, "tiff to zarr conversion", check_interval=10)
            return result

    @staticmethod
    def _build_multi_resolution_wrapper(
        rundir: str = "/eagle/IRIProd/ALS/data/raw",
        script_path: str = "/eagle/IRIProd/ALS/scripts/tiff_to_zarr.py",
        recon_path: str = None,
        raw_path: str = None
    ) -> str:
        """
        Python function that wraps around the application call for Tiff to Zarr on ALCF

        :param rundir: the directory on the eagle file system (ALCF) where the input data are located
        :param script_path: the path to the script that will convert the tiff files to zarr
        :param recon_path: the path to the reconstructed data
        :param raw_path: the path to the raw data
        :return: confirmation message
        """
        import os
        import subprocess

        # Move to directory where data are located
        os.chdir(rundir)

        # Convert tiff files to zarr
        command = (f"python {script_path} {recon_path} --raw_directory {raw_path}")
        zarr_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return (
            f"Converted tiff files to zarr;\n {zarr_res}"
        )

    def segmentation(
        self,
        recon_folder_path: str = "",
    ) -> bool:
        """
        Run tomography segmentation at ALCF through Globus Compute.

        :param recon_folder_path: Path to the reconstructed data folder to be processed.
        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()

        SEGMENTATION_VERSION = "v2"  # "v2"

        # Operate on reconstructed data
        rundir = f"{self.allocation_root}/data/bl832/scratch/reconstruction/{recon_folder_path}"
        output_dir = f"{self.allocation_root}/data/bl832/scratch/segmentation/{recon_folder_path}"

        gcc = Client(code_serialization_strategy=CombinedCode())

        # TODO: Update globus-compute-endpoint Secret block with the new endpoint UUID
        # We will probably have 2 endpoints, one for recon, one for segmentation
        endpoint_id = "168c595b-9493-42db-9c6a-aad960913de2"
        # with Executor(endpoint_id=Secret.load("globus-compute-endpoint").get(), client=gcc) as fxe:

        if SEGMENTATION_VERSION == "v1":
            segmentation_module = "src.inference"
            workdir = f"{self.allocation_root}/segmentation/scripts/forge_feb_seg_model_demo"

            with Executor(endpoint_id=endpoint_id, client=gcc) as fxe:
                logger.info(f"Running segmentation on {recon_folder_path} at ALCF")
                future = fxe.submit(
                    self._segmentation_wrapper_v1,
                    input_dir=rundir,
                    output_dir=output_dir,
                    script_module=segmentation_module,
                    workdir=workdir
                )
                result = self._wait_for_globus_compute_future(future, "segmentation", check_interval=10)

        elif SEGMENTATION_VERSION == "v2":
            segmentation_module = "src.inference_v2_optimized2"  # "src.inference_v2"
            workdir = f"{self.allocation_root}/segmentation/scripts/forge_feb_seg_model_demo_v2/forge_feb_seg_model_demo"
            with Executor(endpoint_id=endpoint_id, client=gcc) as fxe:
                logger.info(f"Running segmentation on {recon_folder_path} at ALCF")
                future = fxe.submit(
                    self._segmentation_wrapper_v2,
                    input_dir=rundir,
                    output_dir=output_dir,
                    script_module=segmentation_module,
                    workdir=workdir
                )
                result = self._wait_for_globus_compute_future(future, "segmentation", check_interval=10)

        return result

    @staticmethod
    def _segmentation_wrapper_v1(
        input_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/reconstruction/",
        output_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/segmentation/",
        script_module: str = "src.inference",
        workdir: str = "/eagle/SYNAPS-I/segmentation/scripts/forge_feb_seg_model_demo",
        nproc_per_node: int = 4,  # 1 works
        nnodes: int = 1,
        nnode_rank: int = 0,
        master_addr: str = "localhost",
        master_port: str = "29500",
        patch_size: int = 512,
        batch_size: int = 1,
        num_workers: int = 4,
        confidence: float = 0.5,
        prompts: list[str] = ["background", "cell"],
    ) -> str:
        """
        Python function that wraps around the application call for segmentation on ALCF

        :param rundir: the directory on the eagle file system (ALCF) where the input data are located
        :param script_path: the path to the script that will run the segmentation
        :param folder_path: the path to the folder containing the TIFF data to be segmented
        :return: confirmation message
        """
        import os
        import subprocess
        import time

        seg_start = time.time()

        # Move to directory where the segmentation code is located
        os.chdir(workdir)

        # Run segmentation.py
        command = [
            "python", "-m", "torch.distributed.run",
            f"--nproc_per_node={nproc_per_node}",
            f"--nnodes={nnodes}",
            f"--node_rank={nnode_rank}",
            f"--master_addr={master_addr}",
            f"--master_port={master_port}",
            "-m", script_module,
            "--input-dir", input_dir,
            "--output-dir", output_dir,
            "--patch-size", str(patch_size),
            "--batch-size", str(batch_size),
            "--num-workers", str(num_workers),
            "--confidence", str(confidence),
            "--prompts", *prompts,
        ]

        segment_res = subprocess.run(command)  # stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if segment_res.returncode != 0:
            raise RuntimeError(f"Segmentation failed with return code {segment_res.returncode}")

        seg_end = time.time()

        print(f"Segmented data in {input_dir} in {seg_end-seg_start} seconds;\n {segment_res}")
        return (
            f"Segmented data specified in {input_dir} in {seg_end-seg_start} seconds;\n"
            f"{segment_res}"
        )

    @staticmethod
    def _segmentation_wrapper_v2(
        input_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/reconstruction/",
        output_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/segmentation/",
        # script_module: str = "src.inference_v2",
        script_module: str = "src.inference_v2_optimized",
        workdir: str = "/eagle/SYNAPS-I/segmentation/scripts/forge_feb_seg_model_demo_v2/forge_feb_seg_model_demo",
        nproc_per_node: int = 4,
        nnodes: int = 2,
        patch_size: int = 640,
        batch_size: int = 8,
        confidence: float = 0.5,
        prompts: list[str] = ["Cortex", "Phloem Fibers", "Air-based Pith cells", "Water-based Pith cells", "Xylem vessels"],
        bpe_path: str = "/eagle/SYNAPS-I/segmentation/sam3_finetune/sam3/bpe_simple_vocab_16e6.txt.gz",
        finetuned_checkpoint: str = "/eagle/SYNAPS-I/segmentation/sam3_finetune/sam3/checkpoint.pt",
        original_checkpoint: str = "/eagle/SYNAPS-I/segmentation/sam3_finetune/sam3/sam3.pt",
        use_finetuned: bool = True,
    ) -> str:
        import os
        import subprocess
        import time

        seg_start = time.time()
        os.chdir(workdir)

        # Get PBS info
        pbs_nodefile = os.environ.get("PBS_NODEFILE")
        pbs_jobid = os.environ.get("PBS_JOBID", "12345")

        print("=== PBS DEBUG ===")
        print(f"PBS_NODEFILE: {pbs_nodefile}")
        print(f"PBS_JOBID: {pbs_jobid}")

        if pbs_nodefile and os.path.exists(pbs_nodefile):
            with open(pbs_nodefile, 'r') as f:
                all_lines = [line.strip() for line in f if line.strip()]
            unique_nodes = list(dict.fromkeys(all_lines))
            actual_nnodes = len(unique_nodes)
            master_addr = unique_nodes[0]
            print(f"PBS_NODEFILE contents: {all_lines}")
            print(f"Unique nodes ({actual_nnodes}): {unique_nodes}")
            print(f"Master: {master_addr}")
        else:
            actual_nnodes = 1
            master_addr = "localhost"
            print("No PBS_NODEFILE, single node mode")

        # Use explicit path to torchrun from the virtual environment
        venv_path = "/eagle/SYNAPS-I/segmentation/env"
        # torchrun_path = f"{venv_path}/bin/torchrun"
        # python_path = f"{venv_path}/bin/python"

        # Build torchrun arguments
        torchrun_args = [
            f"--nnodes={actual_nnodes}",
            f"--nproc_per_node={nproc_per_node}",
            f"--rdzv_id={pbs_jobid}",
            "--rdzv_backend=c10d",
            f"--rdzv_endpoint={master_addr}:29500",
            "-m", script_module,
            "--input-dir", input_dir,
            "--output-dir", output_dir,
            "--patch-size", str(patch_size),
            "--batch-size", str(batch_size),
            "--confidence", str(confidence),
            "--prompts",
        ]
        # torchrun_args.extend(prompts)
        torchrun_args.extend([f'"{p}"' for p in prompts])

        torchrun_args.extend(["--bpe-path", bpe_path])

        if use_finetuned:
            torchrun_args.extend([
                "--finetuned-checkpoint", finetuned_checkpoint,
                "--original-checkpoint", original_checkpoint,
            ])
        else:
            torchrun_args.extend(["--original-checkpoint", original_checkpoint])

        torchrun_cmd = f"{venv_path}/bin/python -m torch.distributed.run " + " ".join(torchrun_args)

        # Environment + NCCL setup - activate venv and set PATH explicitly
        env_setup = (
            f"source {venv_path}/bin/activate && "
            f"export PATH={venv_path}/bin:$PATH && "
            "export HF_HUB_CACHE=/eagle/SYNAPS-I/segmentation/.cache/huggingface && "
            "export HF_HOME=$HF_HUB_CACHE && "
            "export CUDA_DEVICE_ORDER=PCI_BUS_ID && "
            "export NCCL_NET_GDR_LEVEL=PHB && "
            "export NCCL_CROSS_NIC=1 && "
            "export NCCL_COLLNET_ENABLE=1 && "
            'export NCCL_NET="AWS Libfabric" && '
            "export LD_LIBRARY_PATH=/soft/libraries/aws-ofi-nccl/v1.9.1-aws/lib:$LD_LIBRARY_PATH && "
            "export LD_LIBRARY_PATH=/soft/libraries/hwloc/lib/:$LD_LIBRARY_PATH && "
            "export FI_CXI_DISABLE_HOST_REGISTER=1 && "
            "export FI_MR_CACHE_MONITOR=userfaultfd && "
            "export FI_CXI_DEFAULT_CQ_SIZE=131072 && "
            f"cd {workdir} && "
        )

        if actual_nnodes > 1:
            # Use mpiexec to launch torchrun on all nodes
            command = [
                "mpiexec",
                "-n", str(actual_nnodes),
                "-ppn", "1",
                "-hostfile", pbs_nodefile,
                "--cpu-bind", "depth",
                "-d", "16",
                "bash", "-c", env_setup + torchrun_cmd
            ]
        else:
            command = ["bash", "-c", env_setup + torchrun_cmd]

        print(f"Running: {' '.join(command)}")

        result = subprocess.run(command, stdout=None, stderr=None, text=True)
        print(f"STDOUT: {result.stdout[-3000:] if result.stdout else 'None'}")
        print(f"STDERR: {result.stderr[-3000:] if result.stderr else 'None'}")

        if result.returncode != 0:
            raise RuntimeError(f"Segmentation failed: {result.returncode}\nSTDERR: {result.stderr[-2000:]}")

        return f"Completed in {time.time() - seg_start:.1f}s"

    @staticmethod
    def _wait_for_globus_compute_future(
        future: Future,
        task_name: str,
        check_interval: int = 20,
        walltime: int = 3600  # seconds = 60 minutes
    ) -> bool:
        """
        Wait for a Globus Compute task to complete, assuming that if future.done() is False, the task is running.

        :param future: The future object returned from the Globus Compute Executor submit method.
        :param task_name: A descriptive name for the task being executed (used for logging).
        :param check_interval: The interval (in seconds) between status checks.
        :param walltime: The maximum time (in seconds) to wait for the task to complete.
        :return: True if the task completed successfully within walltime, False otherwise.
        """
        logger = get_run_logger()

        start_time = time.time()
        success = False

        try:
            previous_state = None
            while not future.done():
                elapsed_time = time.time() - start_time
                if elapsed_time > walltime:
                    logger.error(f"The {task_name} task exceeded the walltime of {walltime} seconds."
                                 "Cancelling the Globus Compute job.")
                    future.cancel()
                    return False

                # Check if the task was cancelled
                if future.cancelled():
                    logger.warning(f"The {task_name} task was cancelled.")
                    return False
                # Assume the task is running if not done and not cancelled
                elif previous_state != 'running':
                    logger.info(f"The {task_name} task is running...")
                    previous_state = 'running'

                time.sleep(check_interval)  # Wait before the next status check

            # Task is done, check if it was cancelled or raised an exception
            if future.cancelled():
                logger.warning(f"The {task_name} task was cancelled after completion.")
                return False

            exception = future.exception()
            if exception:
                logger.error(f"The {task_name} task raised an exception: {exception}")
                return False

            # Task completed successfully
            result = future.result()
            logger.info(f"The {task_name} task completed successfully with result: {result}")
            success = True

        except Exception as e:
            logger.error(f"An error occurred while waiting for the {task_name} task: {str(e)}")
            success = False

        finally:
            # Log the total time taken for the task
            elapsed_time = time.time() - start_time
            logger.info(f"Total duration of the {task_name} task: {elapsed_time:.2f} seconds.")

        return success


@flow(name="alcf_recon_flow", flow_run_name="alcf_recon-{file_path}")
def alcf_recon_flow(
    file_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Process and transfer a file from bl832 to ALCF and run reconstruction and segmentation.

    :param file_path: The path to the file to be processed.
    :param config: Configuration object for the flow.
    :return: True if the flow completed successfully, False otherwise.
    """
    logger = get_run_logger()

    if config is None:
        config = Config832()
    # set up file paths
    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem
    h5_file_name = file_name + '.h5'
    scratch_path_tiff = folder_name + '/rec' + file_name + '/'
    scratch_path_zarr = folder_name + '/rec' + file_name + '.zarr/'

    # initialize transfer_controller with globus
    logger.info("Initializing Globus Transfer Controller.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    # STEP 1: Transfer data from data832 to ALCF
    logger.info("Copying raw data to ALCF.")
    data832_raw_path = f"{folder_name}/{h5_file_name}"
    alcf_transfer_success = transfer_controller.copy(
        file_path=data832_raw_path,
        source=config.data832_raw,
        destination=config.alcf832_synaps_raw
    )
    logger.info(f"Transfer status: {alcf_transfer_success}")

    if not alcf_transfer_success:
        logger.error("Transfer failed due to configuration or authorization issues.")
        raise ValueError("Transfer to ALCF Failed")
    else:
        logger.info("Transfer to ALCF Successful.")

        # STEP 2: Run Tomopy Reconstruction on Globus Compute
        logger.info(f"Starting ALCF reconstruction flow for {file_path=}")

        # Initialize the Tomography Controller and run the reconstruction
        logger.info("Initializing ALCF Tomography HPC Controller.")
        tomography_controller = get_controller(
            hpc_type=HPC.ALCF,
            config=config
        )
        logger.info(f"Starting ALCF reconstruction task for {file_path=}")
        alcf_reconstruction_success = tomography_controller.reconstruct(
            file_path=file_path,
        )
        if not alcf_reconstruction_success:
            logger.error("Reconstruction Failed.")
            raise ValueError("Reconstruction at ALCF Failed")
        else:
            logger.info("Reconstruction Successful.")

            # STEP 3: Send reconstructed data (tiff) to data832
            logger.info(f"Transferring {file_name} from {config.alcf832_synaps_recon} "
                        f"at ALCF to {config.data832_scratch} at data832")
            data832_tiff_transfer_success = transfer_controller.copy(
                file_path=scratch_path_tiff,
                source=config.alcf832_synaps_recon,
                destination=config.data832_scratch
            )
            logger.info(f"Transfer reconstructed TIFF data to data832 success: {data832_tiff_transfer_success}")

            # STEP 4: Run the Tiff to Zarr Globus Flow
            logger.info(f"Starting ALCF tiff to zarr flow for {file_path=}")
            alcf_multi_res_success = tomography_controller.build_multi_resolution(
                file_path=file_path,
            )
            if not alcf_multi_res_success:
                logger.error("Tiff to Zarr Failed.")
                raise ValueError("Tiff to Zarr at ALCF Failed")
            else:
                logger.info("Tiff to Zarr Successful.")
                # STEP 5: Send reconstructed data (zarr) to data832
                logger.info(f"Transferring {file_name} from {config.alcf832_synaps_recon} "
                            f"at ALCF to {config.data832_scratch} at data832")
                data832_zarr_transfer_success = transfer_controller.copy(
                    file_path=scratch_path_zarr,
                    source=config.alcf832_synaps_recon,
                    destination=config.data832_scratch
                )

    # Place holder in case we want to transfer to NERSC for long term storage
    # nersc_transfer_success = False

    # STEP 6: Schedule Pruning of files
    logger.info("Scheduling file pruning tasks.")
    prune_controller = get_prune_controller(
        prune_type=PruneMethod.GLOBUS,
        config=config
    )

    # Prune from ALCF raw
    if alcf_transfer_success:
        logger.info("Scheduling pruning of ALCF raw data.")
        prune_controller.prune(
            file_path=data832_raw_path,
            source_endpoint=config.alcf832_synaps_raw,
            check_endpoint=None,
            days_from_now=2.0
        )

    # Prune TIFFs from ALCF scratch/reconstruction
    if alcf_reconstruction_success:
        logger.info("Scheduling pruning of ALCF scratch reconstruction data.")
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.alcf832_synaps_recon,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    # Prune ZARR from ALCF scratch/reconstruction
    if alcf_multi_res_success:
        logger.info("Scheduling pruning of ALCF scratch zarr reconstruction data.")
        prune_controller.prune(
            file_path=scratch_path_zarr,
            source_endpoint=config.alcf832_synaps_recon,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    # Prune reconstructed TIFFs from data832 scratch
    if data832_tiff_transfer_success:
        logger.info("Scheduling pruning of data832 scratch reconstruction TIFF data.")
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # Prune reconstructed ZARR from data832 scratch
    if data832_zarr_transfer_success:
        logger.info("Scheduling pruning of data832 scratch reconstruction ZARR data.")
        prune_controller.prune(
            file_path=scratch_path_zarr,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # TODO: ingest to scicat

    if alcf_reconstruction_success and alcf_multi_res_success:
        return True
    else:
        return False


@flow(name="alcf_forge_recon_segment_flow", flow_run_name="alcf_recon_seg-{file_path}")
def alcf_forge_recon_segment_flow(
    file_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Process and transfer a file from bl832 to ALCF and run reconstruction and segmentation.

    :param file_path: The path to the file to be processed.
    :param config: Configuration object for the flow.
    :return: True if the flow completed successfully, False otherwise.
    """
    logger = get_run_logger()

    if config is None:
        config = Config832()
    # set up file paths
    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem
    h5_file_name = file_name + '.h5'
    scratch_path_tiff = folder_name + '/rec' + file_name + '/'
    scratch_path_segment = folder_name + '/seg' + file_name + '/'

    # initialize transfer_controller with globus
    logger.info("Initializing Globus Transfer Controller.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    # STEP 1: Transfer data from data832 to ALCF
    logger.info("Copying raw data to ALCF.")
    data832_raw_path = f"{folder_name}/{h5_file_name}"
    alcf_transfer_success = transfer_controller.copy(
        file_path=data832_raw_path,
        source=config.data832_raw,
        destination=config.alcf832_synaps_raw
    )
    logger.info(f"Transfer status: {alcf_transfer_success}")

    if not alcf_transfer_success:
        logger.error("Transfer failed due to configuration or authorization issues.")
        raise ValueError("Transfer to ALCF Failed")
    else:
        logger.info("Transfer to ALCF Successful.")

        # STEP 2: Run the Tomopy Reconstruction Globus Flow
        logger.info(f"Starting ALCF reconstruction flow for {file_path=}")

        # Initialize the Tomography Controller and run the reconstruction
        logger.info("Initializing ALCF Tomography HPC Controller.")
        tomography_controller = get_controller(
            hpc_type=HPC.ALCF,
            config=config
        )
        logger.info(f"Starting ALCF reconstruction task for {file_path=}")
        alcf_reconstruction_success = tomography_controller.reconstruct(
            file_path=file_path,
        )
        if not alcf_reconstruction_success:
            logger.error("Reconstruction Failed.")
            raise ValueError("Reconstruction at ALCF Failed")
        else:
            logger.info("Reconstruction Successful.")

            # STEP 3: Send reconstructed data (tiff) to data832
            logger.info(f"Transferring {file_name} from {config.alcf832_synaps_recon} "
                        f"at ALCF to {config.data832_scratch} at data832")
            data832_tiff_transfer_success = transfer_controller.copy(
                file_path=scratch_path_tiff,
                source=config.alcf832_synaps_recon,
                destination=config.data832_scratch
            )
            logger.info(f"Transfer reconstructed TIFF data to data832 success: {data832_tiff_transfer_success}")

            # STEP 4: Run the Segmentation Task at ALCF
            logger.info(f"Starting ALCF segmentation task for {scratch_path_tiff=}")
            alcf_segmentation_success = alcf_segmentation_task(
                recon_folder_path=scratch_path_tiff,
                config=config
            )
            if not alcf_segmentation_success:
                logger.warning("Segmentation at ALCF Failed")
            else:
                logger.info("Segmentation at ALCF Successful")

                # STEP 5: Send segmented data to data832
                logger.info(f"Transferring {file_name} from {config.alcf832_synaps_segment} "
                            f"at ALCF to {config.data832_scratch} at data832")
                segment_transfer_success = transfer_controller.copy(
                    file_path=scratch_path_segment,
                    source=config.alcf832_synaps_segment,
                    destination=config.data832_scratch
                )
                logger.info(f"Transfer segmented data to data832 success: {segment_transfer_success}")

    # STEP 6: Schedule Pruning of files
    logger.info("Scheduling file pruning tasks.")
    prune_controller = get_prune_controller(
        prune_type=PruneMethod.GLOBUS,
        config=config
    )

    # Prune from ALCF raw
    if alcf_transfer_success:
        logger.info("Scheduling pruning of ALCF raw data.")
        prune_controller.prune(
            file_path=data832_raw_path,
            source_endpoint=config.alcf832_synaps_raw,
            check_endpoint=None,
            days_from_now=2.0
        )

    # Prune TIFFs from ALCF scratch/reconstruction
    if alcf_reconstruction_success:
        logger.info("Scheduling pruning of ALCF scratch reconstruction data.")
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.alcf832_synaps_recon,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    # Prune TIFFs from ALCF scratch/segmentation
    if alcf_segmentation_success:
        logger.info("Scheduling pruning of ALCF scratch segmentation data.")
        prune_controller.prune(
            file_path=scratch_path_segment,
            source_endpoint=config.alcf832_synaps_segment,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    # Prune reconstructed TIFFs from data832 scratch
    if data832_tiff_transfer_success:
        logger.info("Scheduling pruning of data832 scratch reconstruction TIFF data.")
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # Prune segmented data from data832 scratch
    if alcf_segmentation_success:
        logger.info("Scheduling pruning of data832 scratch segmentation data.")
        prune_controller.prune(
            file_path=scratch_path_segment,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # TODO: ingest to scicat

    if alcf_reconstruction_success and alcf_segmentation_success:
        return True
    else:
        return False


@task(name="alcf_segmentation_task")
def alcf_segmentation_task(
    recon_folder_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Run segmentation task at ALCF.

    :param recon_folder_path: Path to the reconstructed data folder to be processed.
    :param config: Configuration object for the flow.
    :return: True if the task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    if config is None:
        logger.info("No config provided, using default Config832.")
        config = Config832()

    # Initialize the Tomography Controller and run the segmentation
    logger.info("Initializing ALCF Tomography HPC Controller.")
    tomography_controller = get_controller(
        hpc_type=HPC.ALCF,
        config=config
    )
    logger.info(f"Starting ALCF segmentation task for {recon_folder_path=}")
    alcf_segmentation_success = tomography_controller.segmentation(
        recon_folder_path=recon_folder_path,
    )
    if not alcf_segmentation_success:
        logger.error("Segmentation Failed.")
    else:
        logger.info("Segmentation Successful.")
    return alcf_segmentation_success


@flow(name="alcf_segmentation_integration_test", flow_run_name="alcf_segmentation_integration_test")
def alcf_segmentation_integration_test() -> bool:
    """
    Integration test for the ALCF segmentation task.

    :return: True if the segmentation task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info("Starting ALCF segmentation integration test.")
    recon_folder_path = 'rec20211222_125057_petiole4'  # 'test'  #
    flow_success = alcf_segmentation_task(
        recon_folder_path=recon_folder_path,
        config=Config832()
    )
    logger.info(f"Flow success: {flow_success}")
    return flow_success


if __name__ == "__main__":
    alcf_segmentation_integration_test()
