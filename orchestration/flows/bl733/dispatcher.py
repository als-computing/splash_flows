import logging
from pathlib import Path
from prefect import flow, runtime
from typing import Optional, Union, Any

from orchestration.flows.bl733.move import process_new_733_file_task
from orchestration.flows.bl733.config import Config733

logger = logging.getLogger(__name__)


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


# TODO Once this PR (https://github.com/als-computing/splash_flows/pull/62) is merged, we can use config: Config733
@flow(name="dispatcher", flow_run_name=generate_flow_run_name)
def dispatcher(
    file_path: Optional[Union[str, list[str]]] = None,
    is_export_control: bool = False,
    config: Optional[Union[dict, Any]] = None,
) -> None:
    """
    Dispatcher flow for BL733 beamline that launches the new_733_file_flow.

    :param file_path: Path(s) to the file(s) to be processed. Can be a single path (str)
                      or multiple paths (list[str]).
    :param is_export_control: Flag indicating if export control measures should be applied.
                              (Not used in the current BL733 processing)
    :param config: Configuration settings for processing.
                   If not provided, a default Config733 is instantiated.
    :raises ValueError: If no file_path is provided.
    """

    # Normalize file_path to a list
    if file_path is None:
        file_paths = []
    elif isinstance(file_path, str):
        file_paths = [file_path]
    else:
        file_paths = file_path

    num_files = len(file_paths)

    logger.info(f"Starting dispatcher flow for BL 7.3.3 with {num_files} file(s)")
    logger.info(f"Parameters received: file_path={file_path}, is_export_control={is_export_control}")

    # Validate inputs and raise errors if necessary. The ValueErrors prevent the rest of the flow from running.
    if not file_paths:  # returns True for empty list
        logger.error("No file_path provided to dispatcher.")
        raise ValueError("File path is required for processing.")

    if is_export_control:
        logger.error("Data is under export control. Processing is not allowed.")
        raise ValueError("Data is under export control. Processing is not allowed.")

    if config is None:
        config = Config733()
        logger.info("No config provided. Using default Config733 instance.")
    try:
        process_new_733_file_task(
            file_path=file_path,
            config=config
        )
        logger.info(f"Dispatcher flow completed successfully for {num_files} file(s).")
    except Exception as e:
        logger.error(f"Error during processing in dispatcher flow: {e}")
        raise
