import logging
from prefect import flow
from typing import Optional, Union, Any

from orchestration.flows.bl531.move import process_new_531_file_task
from orchestration.flows.bl531.config import Config531

logger = logging.getLogger(__name__)


# TODO Once this PR (https://github.com/als-computing/splash_flows/pull/62) is merged, we can use config: Config531
@flow(name="dispatcher", flow_run_name="dispatcher-{file_path}")
def dispatcher(
    file_path: Optional[str] = None,
    is_export_control: bool = False,
    config: Optional[Union[dict, Any]] = None,
) -> None:
    """
    Dispatcher flow for BL531 beamline that launches the new_531_file_flow.

    :param file_path: Path to the file to be processed.
    :param is_export_control: Flag indicating if export control measures should be applied.
                              (Not used in the current BL531 processing)
    :param config: Configuration settings for processing.
                   Expected to be an instance of Config531 or a dict that can be converted.
    :raises ValueError: If no configuration is provided.
    :raises TypeError: If the provided configuration is not a dict or Config531.
    """

    logger.info("Starting dispatcher flow for BL 5.3.1")
    logger.info(f"Parameters received: file_path={file_path}, is_export_control={is_export_control}")

    # Validate inputs and raise errors if necessary. The ValueErrors prevent the rest of the flow from running.
    if file_path is None:
        logger.error("No file_path provided to dispatcher.")
        raise ValueError("File path is required for processing.")

    if is_export_control:
        logger.error("Data is under export control. Processing is not allowed.")
        raise ValueError("Data is under export control. Processing is not allowed.")

    if config is None:
        config = Config531()
        logger.info("No config provided. Using default Config531.")

    try:
        process_new_531_file_task(
            file_path=file_path,
            config=config
        )
        logger.info("Dispatcher flow completed successfully.")
    except Exception as e:
        logger.error(f"Error during processing in dispatcher flow: {e}")
        raise
