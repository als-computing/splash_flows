import datetime
import logging
from pathlib import Path
from typing import Optional

from prefect import flow, task, get_run_logger
from prefect.variables import Variable

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.job_controller import get_controller, HPC, TomographyHPCController
from orchestration.jobs.alcf.controller import ALCFJobController
from orchestration.transfer_controller import get_transfer_controller, CopyMethod
from orchestration.prefect import schedule_prefect_flow
from orchestration.tiled import register_file_to_tiled

logger = logging.getLogger(__name__)


class ALCFTomographyHPCController(TomographyHPCController, ALCFJobController):
    """ALCF tomography HPC controller for BL832.

    Submits reconstruction and multi-resolution jobs to ALCF via Globus Compute.
    Beamline-agnostic submit/wait primitives are inherited from ALCFJobController.
    """

    def __init__(self, config: Config832) -> None:
        # ALCF doesn't take a pre-built client — Globus Compute Client is
        # constructed inside ALCFJobController.submit() per-call.
        ALCFJobController.__init__(self, config)
        TomographyHPCController.__init__(self, config)

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
        run_logger = get_run_logger()
        file_name = Path(file_path).stem + ".h5"
        folder_name = Path(file_path).parent.name

        iri_als_bl832_rundir = f"{self.allocation_root}/data/raw"
        iri_als_bl832_recon_script = f"{self.allocation_root}/scripts/globus_reconstruction.py"

        run_logger.info(f"Running Tomopy reconstruction on {file_name} at ALCF")
        future = self.submit(
            self._reconstruct_wrapper,
            iri_als_bl832_rundir,
            iri_als_bl832_recon_script,
            file_name,
            folder_name,
        )
        return self.wait_for_future(future, "reconstruction", check_interval=10)

    @staticmethod
    def _reconstruct_wrapper(
        rundir: str = "/eagle/IRIProd/ALS/data/raw",
        script_path: str = "/eagle/IRIProd/ALS/scripts/globus_reconstruction.py",
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
        run_logger = get_run_logger()

        file_name = Path(file_path).stem
        folder_name = Path(file_path).parent.name

        tiff_scratch_path = f"{self.allocation_root}/data/scratch/{folder_name}/rec{file_name}/"
        raw_path = f"{self.allocation_root}/data/raw/{folder_name}/{file_name}.h5"

        iri_als_bl832_rundir = f"{self.allocation_root}/data/raw"
        iri_als_bl832_conversion_script = f"{self.allocation_root}/scripts/tiff_to_zarr.py"

        run_logger.info(f"Running Tiff to Zarr on {raw_path} at ALCF")
        future = self.submit(
            self._build_multi_resolution_wrapper,
            iri_als_bl832_rundir,
            iri_als_bl832_conversion_script,
            tiff_scratch_path,
            raw_path,
        )
        return self.wait_for_future(future, "tiff to zarr conversion", check_interval=10)

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
    Process and transfer a file from a source to the ALCF.

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

            # STEP 2B: Run the Tiff to Zarr Globus Flow
            logger.info(f"Starting ALCF tiff to zarr flow for {file_path=}")
            alcf_multi_res_success = tomography_controller.build_multi_resolution(
                file_path=file_path,
            )
            if not alcf_multi_res_success:
                logger.error("Tiff to Zarr Failed.")
                raise ValueError("Tiff to Zarr at ALCF Failed")
            else:
                logger.info("Tiff to Zarr Successful.")
                # Transfer B: Send reconstructed data (zarr) to data832
                logger.info(f"Transferring {file_name} from {config.alcf832_scratch} "
                            f"at ALCF to {config.data832_scratch} at data832")
                data832_zarr_transfer_success = transfer_controller.copy(
                    file_path=scratch_path_zarr,
                    source=config.alcf832_scratch,
                    destination=config.data832_scratch
                )

                beegfs_zarr_transfer_success = transfer_controller.copy(
                    file_path=scratch_path_zarr,
                    source=config.alcf832_scratch,
                    destination=config.beegfs_scratch
                )

                if beegfs_zarr_transfer_success:
                    logger.info("Successfully transferred Zarr to beegfs. Now ingesting to Tiled.")
                    register_file_to_tiled(
                        path=Path(config.beegfs_scratch.root_path+scratch_path_zarr),
                        prefix="beamlines/bl832/scratch",
                        overwrite=False,
                        tags=["8.3.2", folder_name],
                    )
                else:
                    logger.error("Failed to transfer Zarr to beegfs, skipping registration to Tiled.")

    # Place holder in case we want to transfer to NERSC for long term storage
    nersc_transfer_success = False

    data832_tiff_transfer_success, data832_zarr_transfer_success, nersc_transfer_success
    schedule_pruning(
        alcf_raw_path=f"{folder_name}/{h5_file_name}" if alcf_transfer_success else None,
        alcf_scratch_path_tiff=f"{scratch_path_tiff}" if alcf_reconstruction_success else None,
        alcf_scratch_path_zarr=f"{scratch_path_zarr}" if alcf_multi_res_success else None,
        nersc_scratch_path_tiff=f"{scratch_path_tiff}" if nersc_transfer_success else None,
        nersc_scratch_path_zarr=f"{scratch_path_zarr}" if nersc_transfer_success else None,
        data832_raw_path=f"{folder_name}/{h5_file_name}" if alcf_transfer_success else None,
        data832_scratch_path_tiff=f"{scratch_path_tiff}" if data832_tiff_transfer_success else None,
        data832_scratch_path_zarr=f"{scratch_path_zarr}" if data832_zarr_transfer_success else None,
        one_minute=False,  # Set to False for production durations
        config=config
    )

    # TODO: ingest to scicat

    if alcf_reconstruction_success and alcf_multi_res_success:
        return True
    else:
        return False


if __name__ == "__main__":
    folder_name = 'dabramov'
    file_name = '20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast'
    flow_success = alcf_recon_flow(
        file_path=f"/{folder_name}/{file_name}.h5",
        config=Config832()
    )
    print(flow_success)
