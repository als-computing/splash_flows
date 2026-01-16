from enum import Enum
import logging
from pathlib import Path
from typing import Optional, Union, Any

from prefect import flow, runtime

from orchestration.flows.dichroism.config import ConfigDichroism
from orchestration.flows.dichroism.move import process_new_402_file_task, process_new_631_file_task

logger = logging.getLogger(__name__)


class DichroismBeamlineEnum(str, Enum):
    BL402 = "BL402"
    BL631 = "BL631"


def generate_flow_run_name() -> str:
    """Generate flow run name from runtime parameters."""
    params = runtime.flow_run.parameters
    file_path = params.get("file_path")
    if file_path is None:
        return "dispatcher-no_file"
    elif isinstance(file_path, str):
        return f"dispatcher-{Path(file_path).name}"
    elif len(file_path) == 1:
        return f"dispatcher-{Path(file_path[0]).name}"
    else:
        return f"dispatcher-{Path(file_path[0]).name}_+{len(file_path)-1}_more"


# TODO Once this PR (https://github.com/als-computing/splash_flows/pull/62) is merged, we can use config: Config402
@flow(name="dispatcher", flow_run_name=generate_flow_run_name)
def dispatcher(
    file_path: Optional[Union[str, list[str]]] = None,
    is_export_control: bool = False,
    config: Optional[Union[dict, Any]] = None,
    beamline: Optional[DichroismBeamlineEnum] = None
) -> None:
    """
    Dispatcher flow for BL402 and BL631 beamlines that launches the new_402_file_flow.

    :param file_path: Path(s) to the file(s) to be processed.
    :param is_export_control: Flag indicating if export control measures should be applied.
                              (Not used in the current BL402 processing)
    :param config: Configuration settings for processing.
                   Expected to be an instance of ConfigDichroism or a dict that can be converted.
    :param beamline: Beamline identifier (must be either BL402, BL631).
    :raises ValueError: If no configuration is provided.
    :raises TypeError: If the provided configuration is not a dict or ConfigDichroism.
    """

    # Normalize file_path to a list
    if file_path is None:
        file_paths = []
    elif isinstance(file_path, str):
        file_paths = [file_path]
    else:
        file_paths = file_path

    num_files = len(file_paths)

    logger.info(f"Starting dispatcher flow for Dichroism with {num_files} file(s)")
    logger.info(f"Parameters received: file_path={file_path}, is_export_control={is_export_control}, beamline={beamline}")

    # Validate inputs and raise errors if necessary. The ValueErrors prevent the rest of the flow from running.
    if not file_paths:  # returns True for empty list
        logger.error("No file_path provided to dispatcher.")
        raise ValueError("File path is required for processing.")

    if is_export_control:
        logger.error("Data is under export control. Processing is not allowed.")
        raise ValueError("Data is under export control. Processing is not allowed.")

    if config is None:
        logger.info("No config provided, initializing default ConfigDichroism.")
        config = ConfigDichroism()

    if beamline is None:
        logger.error("No beamline specified.")
        raise ValueError("Beamline must be specified as either BL402 or BL631.")

    if beamline == DichroismBeamlineEnum.BL402:
        logger.info("Dispatching to BL402 processing flow.")
        try:
            process_new_402_file_task(
                file_path=file_path,
                config=config
            )
            logger.info("Dispatcher flow completed successfully for BL402.")
        except Exception as e:
            logger.error(f"Error during processing in dispatcher flow for BL402: {e}")
            raise
    elif beamline == DichroismBeamlineEnum.BL631:
        logger.info("Dispatching to BL631 processing flow.")
        try:
            process_new_631_file_task(
                file_path=file_path,
                config=config
            )
            logger.info("Dispatcher flow completed successfully for BL631.")
        except Exception as e:
            logger.error(f"Error during processing in dispatcher flow for BL631: {e}")
            raise
    else:
        logger.error(f"Invalid beamline specified: {beamline}")
        raise ValueError(f"Invalid beamline specified: {beamline}. Must be either BL402 or BL631.")
    logger.info("Dispatcher flow finished.")
