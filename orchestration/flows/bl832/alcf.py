from concurrent.futures import Future
import datetime
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
from orchestration.transfer_controller import get_transfer_controller, CopyMethod
from orchestration.prefect import schedule_prefect_flow


class ALCFTomographyHPCController(TomographyHPCController):
    """
    Implementation of TomographyHPCController for ALCF. Methods here leverage Globus Compute for processing tasks.
    There is a @staticmethod wrapper for each compute task submitted via Globus Compute.
    Also, there is a shared wait_for_globus_compute_future method that waits for the task to complete.

    Args:
        TomographyHPCController (ABC): Abstract class for tomography HPC controllers.
    """

    def __init__(
        self,
        config: Config832
    ) -> None:
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

        Args:
            file_path (str): Path to the file to be processed.

        Returns:
            bool: True if the task completed successfully, False otherwise.
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

        Args:
            rundir (str): the directory on the eagle file system (ALCF) where the input data are located
            script_path (str): the path to the script that will run the reconstruction
            h5_file_name (str): the name of the h5 file to be reconstructed
            folder_path (str): the path to the folder containing the h5 file

        Returns:
            str: confirmation message
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

        Args:
            file_path (str): Path to the file to be processed.

        Returns:
            bool: True if the task completed successfully, False otherwise.
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

        Args:
            rundir (str): the directory on the eagle file system (ALCF) where the input data are located
            script_path (str): the path to the script that will convert the tiff files to zarr
            recon_path (str): the path to the reconstructed data
            raw_path (str): the path to the raw data
        Returns:
            str: confirmation message
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
        folder_path: str = "",
    ) -> bool:
        """
        Run tomography segmentation at ALCF through Globus Compute.

        :param folder_path: Path to the TIFF folder to be processed.

        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()

        # Operate on reconstructed data
        rundir = f"{self.allocation_root}/data/bl832/scratch/reconstruction/{Path(folder_path).name}"
        output_dir = f"{self.allocation_root}/data/bl832/scratch/segmentation/{Path(folder_path).name}"
        segmentation_script = f"{self.allocation_root}/segmentation/scripts/forge_feb_seg_model_demo/src/inference.py"

        gcc = Client(code_serialization_strategy=CombinedCode())

        # TODO: Update globus-compute-endpoint Secret block with the new endpoint UUID
        # We will probably have 2 endpoints, one for recon, one for segmentation
        with Executor(endpoint_id=Secret.load("globus-compute-endpoint").get(), client=gcc) as fxe:
            logger.info(f"Running segmentation on {folder_path} at ALCF")
            future = fxe.submit(
                self._segmentation_wrapper,
                input_dir=rundir,
                output_dir=output_dir,
                script_path=segmentation_script,
                output_dir=folder_path,
            )
            result = self._wait_for_globus_compute_future(future, "segmentation", check_interval=10)
            return result

    @staticmethod
    def _segmentation_wrapper(
        input_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/reconstruction/",
        output_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/segmentation/",
        script_path: str = "/eagle/SYNAPS-I/segmentation/scripts/forge_feb_seg_model_demo/src/inference.py",
        nproc_per_node: int = 4,
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

        # Move to directory where data are located
        os.chdir(input_dir)

        # Run segmentation.py
        command = [
            "torchrun",
            f"--nproc_per_node={nproc_per_node}",
            f"--nnodes={nnodes}",
            f"--node_rank={nnode_rank}",
            f"--master_addr={master_addr}",
            f"--master_port={master_port}",
            "-m", script_path,
            "--input-dir", input_dir,
            "--output-dir", output_dir,
            "--patch-size", str(patch_size),
            "--batch-size", str(batch_size),
            "--num-workers", str(num_workers),
            "--confidence", str(confidence),
            "--prompts", *prompts,
        ]

        segment_res = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        seg_end = time.time()

        print(f"Segmented data in {input_dir} in {seg_end-seg_start} seconds;\n {segment_res}")
        return (
            f"Segmented data specified in {input_dir} in {seg_end-seg_start} seconds;\n"
            f"{segment_res}"
        )

    @staticmethod
    def _wait_for_globus_compute_future(
        future: Future,
        task_name: str,
        check_interval: int = 20,
        walltime: int = 1200  # seconds = 20 minutes
    ) -> bool:
        """
        Wait for a Globus Compute task to complete, assuming that if future.done() is False, the task is running.

        Args:
            future: The future object returned from the Globus Compute Executor submit method.
            task_name: A descriptive name for the task being executed (used for logging).
            check_interval: The interval (in seconds) between status checks.
            walltime: The maximum time (in seconds) to wait for the task to complete.

        Returns:
            bool: True if the task completed successfully within walltime, False otherwise.
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


@task(name="schedule_prune_task")
def schedule_prune_task(
    path: str,
    location: str,
    schedule_days: datetime.timedelta,
    source_endpoint=None,
    check_endpoint=None
) -> bool:
    """
    Schedules a Prefect flow to prune files from a specified location.

    Args:
        path (str): The file path to the folder containing the files.
        location (str): The server location (e.g., 'alcf832_raw') where the files will be pruned.
        schedule_days (int): The number of days after which the file should be deleted.
        source_endpoint (str): The source endpoint for the files.
        check_endpoint (str): The endpoint to check for the existence of the files.

    Returns:
        bool: True if the task was scheduled successfully, False otherwise.
    """
    logger = get_run_logger()

    try:
        flow_name = f"delete {location}: {Path(path).name}"
        schedule_prefect_flow(
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
            duration_from_now=schedule_days
        )
        return True
    except Exception as e:
        logger.error(f"Failed to schedule prune task: {e}")
        return False


@task(name="schedule_pruning")
def schedule_pruning(
    alcf_raw_path: str = None,
    alcf_scratch_path_tiff: str = None,
    alcf_scratch_path_zarr: str = None,
    nersc_scratch_path_tiff: str = None,
    nersc_scratch_path_zarr: str = None,
    data832_raw_path: str = None,
    data832_scratch_path_tiff: str = None,
    data832_scratch_path_zarr: str = None,
    one_minute: bool = False,
    config: Config832 = None
) -> bool:
    """
    This function schedules the deletion of files from specified locations on ALCF, NERSC, and data832.

    Args:
        alcf_raw_path (str, optional): The raw path of the h5 file on ALCF.
        alcf_scratch_path_tiff (str, optional): The scratch path for TIFF files on ALCF.
        alcf_scratch_path_zarr (str, optional): The scratch path for Zarr files on ALCF.
        nersc_scratch_path_tiff (str, optional): The scratch path for TIFF files on NERSC.
        nersc_scratch_path_zarr (str, optional): The scratch path for Zarr files on NERSC.
        data832_scratch_path (str, optional): The scratch path on data832.
        one_minute (bool, optional): Defaults to False. Whether to schedule the deletion after one minute.
        config (Config832, optional): Configuration object for the flow.

    Returns:
        bool: True if the tasks were scheduled successfully, False otherwise.
    """
    logger = get_run_logger()

    pruning_config = Variable.get("pruning-config", _sync=True)

    if one_minute:
        alcf_delay = datetime.timedelta(minutes=1)
        nersc_delay = datetime.timedelta(minutes=1)
        data832_delay = datetime.timedelta(minutes=1)
    else:
        alcf_delay = datetime.timedelta(days=pruning_config["delete_alcf832_files_after_days"])
        nersc_delay = datetime.timedelta(days=pruning_config["delete_nersc832_files_after_days"])
        data832_delay = datetime.timedelta(days=pruning_config["delete_data832_files_after_days"])

    # (path, location, days, source_endpoint, check_endpoint)
    delete_schedules = [
        (alcf_raw_path, "alcf832_raw", alcf_delay, config.alcf832_raw, config.data832_raw),
        (alcf_scratch_path_tiff, "alcf832_scratch", alcf_delay, config.alcf832_scratch, config.data832_scratch),
        (alcf_scratch_path_zarr, "alcf832_scratch", alcf_delay, config.alcf832_scratch, config.data832_scratch),
        (nersc_scratch_path_tiff, "nersc832_alsdev_scratch", nersc_delay, config.nersc832_alsdev_scratch, None),
        (nersc_scratch_path_zarr, "nersc832_alsdev_scratch", nersc_delay, config.nersc832_alsdev_scratch, None),
        (data832_raw_path, "data832_raw", data832_delay, config.data832_raw, None),
        (data832_scratch_path_tiff, "data832_scratch", data832_delay, config.data832_scratch, None),
        (data832_scratch_path_zarr, "data832_scratch", data832_delay, config.data832_scratch, None)
    ]

    for path, location, days, source_endpoint, check_endpoint in delete_schedules:
        if path:
            schedule_prune_task(path, location, days, source_endpoint, check_endpoint)
            logger.info(f"Scheduled delete from {location} at {days} days")
        else:
            logger.info(f"Path not provided for {location}, skipping scheduling of deletion task.")

    return True


@flow(name="alcf_recon_flow", flow_run_name="alcf_recon-{file_path}")
def alcf_recon_flow(
    file_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Process and transfer a file from bl832 to ALCF and run reconstruction and segmentation.

    Args:
        file_path (str): The path to the file to be processed.
        config (Config832): Configuration object for the flow.

    Returns:
        bool: True if the flow completed successfully, False otherwise.
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
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    # STEP 1: Transfer data from data832 to ALCF
    logger.info("Copying data to ALCF.")
    data832_raw_path = f"{folder_name}/{h5_file_name}"
    alcf_transfer_success = transfer_controller.copy(
        file_path=data832_raw_path,
        source=config.data832_raw,
        destination=config.alcf832_raw
    )
    logger.info(f"Transfer status: {alcf_transfer_success}")

    if not alcf_transfer_success:
        logger.error("Transfer failed due to configuration or authorization issues.")
        raise ValueError("Transfer to ALCF Failed")
    else:
        logger.info("Transfer to ALCF Successful.")

        # STEP 2A: Run the Tomopy Reconstruction Globus Flow
        logger.info(f"Starting ALCF reconstruction flow for {file_path=}")

        # Initialize the Tomography Controller and run the reconstruction
        tomography_controller = get_controller(
            hpc_type=HPC.ALCF,
            config=config
        )
        alcf_reconstruction_success = tomography_controller.reconstruct(
            file_path=file_path,
        )
        if not alcf_reconstruction_success:
            logger.error("Reconstruction Failed.")
            raise ValueError("Reconstruction at ALCF Failed")
        else:
            logger.info("Reconstruction Successful.")

            # Transfer A: Send reconstructed data (tiff) to data832
            logger.info(f"Transferring {file_name} from {config.alcf832_scratch} "
                        f"at ALCF to {config.data832_scratch} at data832")
            data832_tiff_transfer_success = transfer_controller.copy(
                file_path=scratch_path_tiff,
                source=config.alcf832_scratch,
                destination=config.data832_scratch
            )

            # STEP 3: Run the Segmentation Task at ALCF
            logger.info(f"Starting ALCF segmentation task for {scratch_path_tiff=}")
            alcf_segmentation_success = alcf_segmentation_task(
                recon_folder_path=scratch_path_tiff,
                config=config
            )
            if not alcf_segmentation_success:
                logger.warning("Segmentation at ALCF Failed")
            else:
                logger.info("Segmentation at ALCF Successful")

            # Not running TIFF to Zarr conversion at ALCF for now
            # STEP 2B: Run the Tiff to Zarr Globus Flow
            # logger.info(f"Starting ALCF tiff to zarr flow for {file_path=}")
            # alcf_multi_res_success = tomography_controller.build_multi_resolution(
            #     file_path=file_path,
            # )
            # if not alcf_multi_res_success:
            #     logger.error("Tiff to Zarr Failed.")
            #     raise ValueError("Tiff to Zarr at ALCF Failed")
            # else:
            #     logger.info("Tiff to Zarr Successful.")
            #     # Transfer B: Send reconstructed data (zarr) to data832
            #     logger.info(f"Transferring {file_name} from {config.alcf832_scratch} "
            #                 f"at ALCF to {config.data832_scratch} at data832")
            #     data832_zarr_transfer_success = transfer_controller.copy(
            #         file_path=scratch_path_zarr,
            #         source=config.alcf832_scratch,
            #         destination=config.data832_scratch
            #     )

    # Place holder in case we want to transfer to NERSC for long term storage
    nersc_transfer_success = False

    # data832_tiff_transfer_success, data832_zarr_transfer_success, nersc_transfer_success
    schedule_pruning(
        alcf_raw_path=f"{folder_name}/{h5_file_name}" if alcf_transfer_success else None,
        alcf_scratch_path_tiff=f"{scratch_path_tiff}" if alcf_reconstruction_success else None,
        # alcf_scratch_path_zarr=f"{scratch_path_zarr}" if alcf_multi_res_success else None, # Commenting out zarr for now
        nersc_scratch_path_tiff=f"{scratch_path_tiff}" if nersc_transfer_success else None,
        nersc_scratch_path_zarr=f"{scratch_path_zarr}" if nersc_transfer_success else None,
        data832_raw_path=f"{folder_name}/{h5_file_name}" if alcf_transfer_success else None,
        data832_scratch_path_tiff=f"{scratch_path_tiff}" if data832_tiff_transfer_success else None,
        # data832_scratch_path_zarr=f"{scratch_path_zarr}" if data832_zarr_transfer_success else None, # Commenting out zarr
        one_minute=False,  # Set to False for production durations
        config=config
    )

    # TODO: ingest to scicat

    if alcf_reconstruction_success and alcf_segmentation_success:  # and alcf_multi_res_success:
        return True
    else:
        return False


@task(name="alcf_segmentation_task")
def alcf_segmentation_task(
    recon_folder_path: str,
    config: Optional[Config832] = None,
):
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
def alcf_segmentation_integration_test():
    folder_name = 'dabramov'
    file_name = '20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast'
    flow_success = alcf_recon_flow(
        file_path=f"/{folder_name}/{file_name}.h5",
        config=Config832()
    )
    print(flow_success)
