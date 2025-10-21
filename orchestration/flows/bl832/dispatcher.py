import asyncio
from datetime import datetime
from dateutil.parser import isoparse

from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON
from prefect.deployments.deployments import run_deployment
from pydantic import BaseModel, ValidationError, Field
from typing import Any, List, Optional, Union

from orchestration.flows.bl832.move import process_new_832_file_task
from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.scicat_ingestor import TomographyIngestorController


# ------------------------------------------------------------------------------------------------------------------------
# Decision Flow: Dispatcher
# ------------------------------------------------------------------------------------------------------------------------
# This flow reads decision settings and launches tasks accordingly.
# ------------------------------------------------------------------------------------------------------------------------
# The dispatcher flow reads decision settings and launches tasks accordingly.
# It first runs the new_832_file_flow/new_file_832 flow synchronously.
# Then, it prepares the ALCF and NERSC flows to run asynchronously based on the decision settings.
# ------------------------------------------------------------------------------------------------------------------------
class FlowParameterMapper:
    """
    Class to define and map the parameters required for each flow.
    """
    flow_parameters = {
        # From alcf.py
        "alcf_recon_flow/alcf_recon_flow": [
            "file_path",
            "config"],
        # From move.py
        "new_832_file_flow/new_file_832": [
            "file_path",
            "is_export_control",
            "config"],
        # From nersc.py
        "nersc_recon_flow/nersc_recon_flow": [
            "file_path",
            "config"]
    }

    @classmethod
    def get_flow_parameters(cls, flow_name: str, available_params: dict) -> dict:
        """
        Get a dictionary of parameters required for a specific flow based on available parameters.

        :param flow_name: Name of the flow to get parameters for.
        :param available_params: Dictionary of all available parameters.
        :return: Dictionary of parameters for the flow.
        """
        # Get the list of required parameters for the specified flow
        required_params = cls.flow_parameters.get(flow_name)
        if required_params is None:
            raise ValueError(f"Flow name '{flow_name}' not found in flow parameters mapping.")
        # Filter and return only those parameters that are available in the provided dictionary
        return {param: available_params[param] for param in required_params if param in available_params}


class DecisionFlowInputModel(BaseModel):
    """
    Pydantic model to validate input parameters for the decision flow.
    """
    file_path: Optional[str] = Field(default=None)
    is_export_control: Optional[bool] = Field(default=False)
    config: Optional[Union[dict, Any]] = Field(default_factory=dict)


@task(name="setup_decision_settings")
def setup_decision_settings(alcf_recon: bool, nersc_recon: bool, new_file_832: bool) -> dict:
    """
    This task is used to define the settings for the decision making process of the BL832 beamline.

    :param alcf_recon: Boolean indicating whether to run the ALCF reconstruction flow.
    :param nersc_recon: Boolean indicating whether to run the NERSC reconstruction flow.
    :param nersc_move: Boolean indicating whether to move files to NERSC.
    :return: A dictionary containing the settings for each flow.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Setting up decision settings: alcf_recon={alcf_recon}, "
                    f"nersc_recon={nersc_recon}, new_file_832={new_file_832}")
        # Define which flows to run based on the input settings
        settings = {
            "alcf_recon_flow/alcf_recon_flow": alcf_recon,
            "nersc_recon_flow/nersc_recon_flow": nersc_recon,
            "new_832_file_flow/new_file_832": new_file_832
        }
        # Save the settings in a JSON block for later retrieval by other flows
        settings_json = JSON(value=settings)
        settings_json.save(name="decision-settings", overwrite=True)
    except Exception as e:
        logger.error(f"Failed to set up decision settings: {e}")
        raise
    return settings


@task(name="run_recon_flow_async")
async def run_recon_flow_async(flow_name: str, parameters: dict) -> None:
    """
    This task is used to run a specific flow with dynamically provided parameters.

    :param flow_name: Name of the flow to run.
    :param parameters: Dictionary of parameters to pass to the flow.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Running {flow_name} with parameters: {parameters}")
        # Run the specified flow deployment with the provided parameters
        await run_deployment(name=flow_name, parameters=parameters)
    except Exception as e:
        logger.error(f"Failed to run flow {flow_name}: {e}")
        raise


@flow(name="dispatcher", flow_run_name="dispatcher-{file_path}")
async def dispatcher(
    file_path: Optional[str] = None,
    is_export_control: bool = False,
    config: Optional[Union[dict, Any]] = None
) -> None:
    """
    Dispatcher flow that reads decision settings and launches tasks accordingly.
    """
    logger = get_run_logger()
    try:
        inputs = DecisionFlowInputModel(
            file_path=file_path,
            is_export_control=is_export_control,
            config=config,
        )
    except ValidationError as e:
        logger.error(f"Invalid input parameters: {e}")
        raise

    # Run new_file_832 first (synchronously)
    available_params = inputs.dict()
    try:
        decision_settings = await JSON.load("decision-settings")
        if decision_settings.value.get("new_832_file_flow/new_file_832"):
            logger.info("Running new_file_832 flow...")
            process_new_832_file_task(
                file_path=available_params.get("file_path"),
                is_export_control=available_params.get("is_export_control", False),
                send_to_nersc=True,
                config=available_params.get("config")
            )

            logger.info("Completed new_file_832 task.")
    except Exception as e:
        logger.error(f"new_832_file_flow/new_file_832 task failed: {e}")
        # Optionally, raise a specific ValueError
        raise ValueError("new_file_832 task Failed") from e

    # Prepare ALCF and NERSC flows to run asynchronously, based on settings
    tasks = []
    if decision_settings.value.get("alcf_recon_flow/alcf_recon_flow"):
        alcf_params = FlowParameterMapper.get_flow_parameters("alcf_recon_flow/alcf_recon_flow", available_params)
        tasks.append(run_recon_flow_async("alcf_recon_flow/alcf_recon_flow", alcf_params))

    if decision_settings.value.get("nersc_recon_flow/nersc_recon_flow"):
        nersc_params = FlowParameterMapper.get_flow_parameters("nersc_recon_flow/nersc_recon_flow", available_params)
        tasks.append(run_recon_flow_async("nersc_recon_flow/nersc_recon_flow", nersc_params))

    # Run ALCF and NERSC flows in parallel, if any
    if tasks:
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Failed to run one or more tasks: {e}")
            raise
    else:
        logger.info("No ALCF or NERSC tasks to run based on decision settings.")

    return None


# ---------------------------------------------------------------------------
# Tape Transfer Flow: Archive a single 832 project (raw)
# ---------------------------------------------------------------------------
@flow(name="archive_832_project_dispatcher")
def archive_832_project_dispatcher(
    config: Config832,
    file_path: Union[str, List[str]] = None,
    scicat_id: Optional[Union[str, List[str]]] = None
) -> None:
    """
    Flow to archive one or more beamline 832 projects to tape.
    Accepts a single file path (str) or a list of file paths, and for each one,
    calls the CFStoHPSSTransferController via run_specific_flow.

    Parameters
    ----------
    config : Config832
        Configuration object containing endpoint details.
    file_path : Union[str, List[str]]
        A single file path or a list of file paths to be archived (path on CFS).
    scicat_id : Optional[Union[str, List[str]]]
        Optional SciCat ID(s) for the project(s). Must be in the same order as file_path(s).
    """

    # Normalize file_path into a list if it's a single string.
    logger = get_run_logger()

    # Build pairs with strict 1:1 length check (only if scicat_id provided)
    if scicat_id is None:
        projects = zip(file_path, [None] * len(file_path))
    else:
        if len(file_path) != len(scicat_id):
            raise ValueError(
                f"Length mismatch: file_path({len(file_path)}) != scicat_id({len(scicat_id)})"
            )
        projects = zip(file_path, scicat_id)

    for fp, scid in projects:
        try:
            run_specific_flow(
                "cfs_to_hpss_flow/cfs_to_hpss_flow",
                {
                    "file_path": fp,
                    "source": config.nersc832,     # NERSC FileSystem Endpoint
                    "destination": config.hpss_alsdev,  # HPSS Endpoint
                    "config": config
                }
            )
            logger.info(f"Scheduled tape transfer for project: {fp}")

        except Exception as e:
            logger.error(f"Error scheduling transfer for {fp}: {e}")

        # Ingest the project into SciCat if scicat_id is provided.
        ingestor = TomographyIngestorController(
            config=config,
            scicat_client=config.scicat
        )

        if scid:
            logger.info("Ingesting new file path into SciCat...")

            try:
                ingestor.add_new_dataset_location(
                    dataset_id=scid,
                    datafile_path=config.hpss_alsdev.root_path + "/" + fp.split("/")[-1],
                    source_folder_host="HPSS"
                )
                logger.info(f"Updated SciCat dataset {scicat_id} with new location for project: {fp}")
            except Exception as e:
                logger.error(f"Error updating dataset location for project {fp} into SciCat: {e}")


# ---------------------------------------------------------------------------
# Tape Transfer Flow: Process pending projects
# ---------------------------------------------------------------------------
# Scheduled to run every 6 months to process tape transfers.
# ---------------------------------------------------------------------------
@flow(name="archive_832_projects_from_previous_cycle_dispatcher")
def archive_832_projects_from_previous_cycle_dispatcher(
    config: Config832,
) -> None:
    """
    Archives the previous cycle's projects from the NERSC / CFS / 8.3.2 / RAW directory.

    The schedule is as follows:
      - On/around January 2 (assuming NERSC is up):
        Archive projects with modification dates between January 1 and July 15 (previous year)
      - On/around July 4 (assuming NERSC is up):
        Archive projects with modification dates between July 16 and December 31 (previous year)

    The flow lists projects via Globus Transfer's operation_ls, filters them based on modification times,
    and then calls the cfs_to_hpss_flow for each eligible project.
    """
    logger = get_run_logger()
    now = datetime.now()

    # Validate that today is a scheduled trigger day and set the archive window accordingly.
    # -------------------------
    # Compute "last complete cycle" inline (no helpers).
    # -------------------------
    if (now.month < 7) or (now.month == 7 and now.day <= 15):
        # Before or on Jul 15: most recent completed cycle is previous year's H2.
        y = now.year - 1
        label = "Cycle 2"
        archive_start = datetime(y, 7, 16, 0, 0, 0)
        archive_end = datetime(y, 12, 31, 23, 59, 59)
    else:
        # Jul 16 or later: most recent completed cycle is current year's H1.
        y = now.year
        label = "Cycle 1"
        archive_start = datetime(y, 1, 1, 0, 0, 0)
        archive_end = datetime(y, 7, 15, 23, 59, 59)

    logger.info(f"Archive window for {label}: {archive_start} to {archive_end}")

    # List projects using Globus Transfer's operation_ls.
    try:
        # config.tc: configured Globus Transfer client.
        # config.nersc832.endpoint_id: the NERSC endpoint ID.
        # config.nersc832_alsdev_raw.root_path: the NERSC CFS directory path.
        projects = config.tc.operation_ls(
            endpoint_id=config.nersc832.uuid,
            path=config.nersc832_alsdev_raw.root_path,
            orderby=["name", "last_modified"],
        ).get("DATA", [])
    except Exception as e:
        logger.error(f"Failed to list projects: {e}")
        return

    logger.info(f"Found {len(projects)} items in the {projects.path} directory.")

    # Process each project: check its modification time and trigger transfer if within the archive window.
    for project in projects:
        project_name = project.get("name")
        last_mod_str = project.get("last_modified")
        if not project_name or not last_mod_str:
            logger.warning(f"Skipping project due to missing name or last_modified: {project}")
            continue

        try:
            last_mod = isoparse(last_mod_str)
        except Exception as e:
            logger.warning(f"Error parsing modification time for project {project_name}: {e}")
            continue

        if archive_start <= last_mod <= archive_end:
            logger.info(f"Project {project_name} last modified at {last_mod} is within the archive window.")
            try:
                # Call the transfer flow for this project.
                # This should be blocking to ensure sequential processing (HPSS has limitations for concurrent transfers).
                run_specific_flow(
                    "cfs_to_hpss_flow/cfs_to_hpss_flow",
                    {
                        "file_path": config.nersc832_alsdev_raw.root_path + "/" + project['name'],
                        "source": config.nersc832,
                        "destination": config.hpss_alsdev,
                        "config": config
                    }
                )

            except Exception as e:
                logger.error(
                    f"Error archiving project {project_name}: {e}. Logs are available on NERSC at "
                    f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{config.beamline_id}/{project_name}_to_hpss_*.log"
                    f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{config.beamline_id}/{project_name}_to_hpss_*.err"
                )
            # Ingest the project into SciCat.
            logger.info("Ingesting new file path into SciCat...")
            ingestor = TomographyIngestorController(
                config=config,
                scicat_client=config.scicat
            )

            # Add a loop to get each file name in projects, and upate path in SciCat.
            for scan in config.tc.operation_ls(
                endpoint_id=config.nersc832.uuid,
                path=config.nersc832_alsdev_raw.root_path + "/" + project['name'],
                orderby=["name", "last_modified"],
            ):
                logger.info(f"Found scan: {scan['name']}")

                logger.info("Looking for dataset in SciCat...")

                try:
                    scicat_id = ingestor._find_dataset(
                        file_name=scan['name']
                    )
                    logger.info(f"Found existing dataset in SciCat with ID: {scicat_id}")
                except Exception as e:
                    logger.warning(f"Error finding dataset in SciCat for scan {scan['name']}: {e}")

                logger.info("Updating dataset location in SciCat...")
                try:
                    if scicat_id:
                        ingestor.add_new_dataset_location(
                            dataset_id=scicat_id,
                            datafile_path=config.hpss_alsdev.root_path + "/" + project['name'] + "/" + scan['name'],
                            source_folder_host="HPSS"
                        )
                    else:
                        logger.warning(f"Skipping dataset location update for scan {scan['name']} as SciCat ID was not found.")
                except Exception as e:
                    logger.warning(f"Error updating dataset location for project {project} into SciCat: {e}")
        else:
            logger.info(f"Project {project_name} last modified at {last_mod} is outside the archive window.")


# ---------------------------------------------------------------------------
# Tape Transfer Flow: Archive all 832 projects (raw)
# ---------------------------------------------------------------------------
@flow(name="archive_all_832_raw_projects_dispatcher")
def archive_all_832_projects_dispatcher(
    config: Config832,
) -> None:
    """
    Scheduled flow to process tape transfers.
    It should call the CFStoHPSSTransferController (not shown) and, upon success, mark projects as moved.
    """
    logger = get_run_logger()

    logger.info(f"Checking for projects at {config.nersc832_alsdev_raw.root_path} to archive to tape...")

    # ARCHIVE ALL PROJECTS IN THE NERSC / CFS / 8.3.2 / RAW DIRECTORY
    # Use the Globus SDK transfer controller (config.tc) to list all projects.
    # Note this is different from the controller classes in this repo.
    for project in config.tc.operation_ls(
        endpoint_id=config.nersc832.uuid,
        path=config.nersc832_alsdev_raw.root_path,
        orderby=["name", "last_modified"],
    ):
        logger.info(f"Found project: {project['name']}")
        try:
            # Call the transfer flow for this project.
            # This should be blocking to ensure sequential processing (HPSS has limitations for concurrent transfers).
            run_specific_flow(
                "cfs_to_hpss_flow/cfs_to_hpss_flow",
                {
                    "file_path": config.nersc832_alsdev_raw.root_path + "/" + project['name'],
                    "source": config.nersc832,  # NERSC FileSystem Endpoint (not globus)
                    "destination": config.hpss_alsdev,  # HPSS Endpoint
                    "config": config
                }
            )
        except Exception as e:
            logger.error(
                f"Error archiving project {project['name']}: {e}. Logs are available on NERSC at "
                f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{config.beamline_id}/{project['name']}_to_hpss_*.log"
                f"/global/cfs/cdirs/als/data_mover/hpss_transfer_logs/{config.beamline_id}/{project['name']}_to_hpss_*.err"
            )

        ingestor = TomographyIngestorController(
            config=config,
            scicat_client=config.scicat
        )

        # Update the path for each scan within the project into SciCat.
        for scan in config.tc.operation_ls(
            endpoint_id=config.nersc832.uuid,
            path=config.nersc832_alsdev_raw.root_path + "/" + project['name'],
            orderby=["name", "last_modified"],
        ):
            try:
                logger.info(f"Found scan: {scan['name']}")

                logger.info("Ingesting new file path into SciCat...")
                scicat_id = ingestor._find_dataset(
                    file_name=scan['name']
                )
            except Exception as e:
                logger.warning(f"Error finding dataset for scan {scan['name']}: {e}")
            try:
                if scicat_id:
                    logger.info(f"Found existing dataset in SciCat with ID: {scicat_id}")
                    ingestor.add_new_dataset_location(
                        dataset_id=scicat_id,
                        datafile_path=config.hpss_alsdev.root_path + "/" + project['name'] + "/" + scan['name'],
                        source_folder_host="HPSS"
                    )
                else:
                    logger.warning(f"Skipping dataset location update for scan {scan['name']} as SciCat ID was not found.")
            except Exception as e:
                logger.error(f"Error updating dataset location for project {project} into SciCat: {e}")
