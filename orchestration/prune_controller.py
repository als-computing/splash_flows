from abc import ABC, abstractmethod
import datetime
import logging
from typing import Generic, TypeVar, Union

from prefect import flow
from prefect.blocks.system import JSON

from orchestration.config import BeamlineConfig
from orchestration.globus.transfer import GlobusEndpoint, prune_one_safe
from orchestration.prefect import schedule_prefect_flow
from orchestration.transfer_endpoints import FileSystemEndpoint, HPSSEndpoint, TransferEndpoint


logger = logging.getLogger(__name__)

Endpoint = TypeVar("Endpoint", bound=TransferEndpoint)


class PruneController(Generic[Endpoint], ABC):
    """
    Abstract class for pruning controllers.
    Provides interface methods for pruning data.
    """
    def __init__(
        self,
        config: BeamlineConfig,
    ) -> None:
        self.config = config

    @abstractmethod
    def prune(
        self,
        file_path: str = None,
        source_endpoint: Endpoint = None,
        check_endpoint: Endpoint = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        """Prune data from the source endpoint.

        Args:
            file_path (str): The path to the file to prune.
            source (Endpoint): The source endpoint.
            destination (Endpoint): The destination endpoint.
            days_from_now (datetime.timedelta): The number of days from now to prune. Defaults to 0.

        Returns:
            bool: True if successful, False otherwise.
        """
        pass


class HPSSPruneController(PruneController[HPSSEndpoint]):
    def __init__(
        self,
        config: BeamlineConfig,
    ) -> None:
        super().__init__(config)

    def prune(
        self,
        file_path: str = None,
        source_endpoint: HPSSEndpoint = None,
        check_endpoint: FileSystemEndpoint = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        flow_name = f"prune_from_{source_endpoint.name}"
        logger.info(f"Running flow: {flow_name}")
        logger.info(f"Pruning {file_path} from source endpoint: {source_endpoint.name}")
        schedule_prefect_flow(
            "prune_hpss_endpoint/prune_hpss_endpoint",
            flow_name,
            {
                "relative_path": file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint,
                "config": self.config
            },

            datetime.timedelta(days=days_from_now),
        )
        return True


class FileSystemPruneController(PruneController[FileSystemEndpoint]):
    def __init__(
        self,
        config
    ) -> None:
        super().__init__(config)

    def prune(
        self,
        file_path: str = None,
        source_endpoint: FileSystemEndpoint = None,
        check_endpoint: FileSystemEndpoint = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        flow_name = f"prune_from_{source_endpoint.name}"
        logger.info(f"Running flow: {flow_name}")
        logger.info(f"Pruning {file_path} from source endpoint: {source_endpoint.name}")
        schedule_prefect_flow(
            "prune_filesystem_endpoint/prune_filesystem_endpoint",
            flow_name,
            {
                "relative_path": file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint,
                "config": self.config
            },

            datetime.timedelta(days=days_from_now),
        )
        return True


class GlobusPruneController(PruneController[GlobusEndpoint]):
    def __init__(
        self,
        config
    ) -> None:
        super().__init__(config)

    def prune(
        self,
        file_path: str = None,
        source_endpoint: GlobusEndpoint = None,
        check_endpoint: GlobusEndpoint = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:

        # globus_settings = JSON.load("globus-settings").value
        # max_wait_seconds = globus_settings["max_wait_seconds"]
        flow_name = f"prune_from_{source_endpoint.name}"
        logger.info(f"Running flow: {flow_name}")
        logger.info(f"Pruning {file_path} from source endpoint: {source_endpoint.name}")
        schedule_prefect_flow(
            "prune_globus_endpoint/prune_globus_endpoint",
            flow_name,
            {
                "relative_path": file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint,
                "config": self.config
            },

            datetime.timedelta(days=days_from_now),
        )
        return True


def get_prune_controller(
    endpoint: TransferEndpoint,
    config: BeamlineConfig
) -> PruneController:
    if isinstance(endpoint, HPSSEndpoint):
        return HPSSPruneController(config)
    elif isinstance(endpoint, FileSystemEndpoint):
        return FileSystemPruneController(config)
    elif isinstance(endpoint, GlobusEndpoint):
        return GlobusPruneController(config)
    else:
        raise ValueError(f"Unsupported endpoint type: {type(endpoint)}")


@flow(name="prune_globus_endpoint")
def prune_globus_files(
    relative_path: str,
    source_endpoint: GlobusEndpoint,
    check_endpoint: Union[GlobusEndpoint, None] = None,
    config: BeamlineConfig = None
):
    """
    Prune files from a source endpoint.

    Args:
        relative_path (str): The path of the file or directory to prune.
        source_endpoint (GlobusEndpoint): The Globus source endpoint to prune from.
        check_endpoint (GlobusEndpoint, optional): The Globus target endpoint to check. Defaults to None.
    """
    globus_settings = JSON.load("globus-settings").value
    max_wait_seconds = globus_settings["max_wait_seconds"]
    flow_name = f"prune_from_{source_endpoint.name}"
    logger.info(f"Running flow: {flow_name}")
    logger.info(f"Pruning {relative_path} from source endpoint: {source_endpoint.name}")
    prune_one_safe(
        file=relative_path,
        if_older_than_days=0,
        tranfer_client=config.tc,
        source_endpoint=source_endpoint,
        check_endpoint=check_endpoint,
        logger=logger,
        max_wait_seconds=max_wait_seconds
    )


@flow(name="prune_filesystem_endpoint")
def prune_filesystem_files(
    relative_path: str,
    source_endpoint: FileSystemEndpoint,
    check_endpoint: Union[FileSystemEndpoint, None] = None,
    config: BeamlineConfig = None
):
    """
    Prune files from a source endpoint.

    Args:
        relative_path (str): The path of the file or directory to prune.
        source_endpoint (GlobusEndpoint): The Globus source endpoint to prune from.
        check_endpoint (GlobusEndpoint, optional): The Globus target endpoint to check. Defaults to None.
    """
    pass


@flow(name="prune_hpss_endpoint")
def prune_hpss_files(
    relative_path: str,
    source_endpoint: HPSSEndpoint,
    check_endpoint: Union[FileSystemEndpoint, None] = None,
    config: BeamlineConfig = None
):
    """
    Prune files from a source endpoint.

    Args:
        relative_path (str): The path of the file or directory to prune.
        source_endpoint (GlobusEndpoint): The Globus source endpoint to prune from.
        check_endpoint (GlobusEndpoint, optional): The Globus target endpoint to check. Defaults to None.
    """
    pass
