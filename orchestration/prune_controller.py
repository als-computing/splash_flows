from abc import ABC, abstractmethod
import datetime
from enum import Enum
import logging
import os
from typing import Generic, TypeVar, Union

from prefect import flow
from prefect.blocks.system import JSON

from orchestration.config import BeamlineConfig
from orchestration.globus.transfer import GlobusEndpoint, prune_one_safe
from orchestration.prefect import schedule_prefect_flow
from orchestration.transfer_endpoints import FileSystemEndpoint, TransferEndpoint


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

    @flow(name="prune_filesystem_endpoint")
    def _prune_filesystem_endpoint(
        relative_path: str,
        source_endpoint: FileSystemEndpoint,
        check_endpoint: Union[FileSystemEndpoint, None] = None,
        config: BeamlineConfig = None
    ):
        """
        Prune files from a File System.

        Args:
            relative_path (str): The path of the file or directory to prune.
            source_endpoint (FileSystemEndpoint): The Globus source endpoint to prune from.
            check_endpoint (FileSystemEndpoint, optional): The Globus target endpoint to check. Defaults to None.
        """
        logger.info(f"Running flow: prune_from_{source_endpoint.name}")
        logger.info(f"Pruning {relative_path} from source endpoint: {source_endpoint.name}")

        # Check if the file exists at the source endpoint
        if not source_endpoint.exists(relative_path):
            logger.warning(f"File {relative_path} does not exist at the source: {source_endpoint.name}.")
            return

        # Check if the file exists at the check endpoint
        if check_endpoint is not None and check_endpoint.exists(relative_path):
            logger.info(f"File {relative_path} exists on the check point: {check_endpoint.name}.")
            logger.info("Safe to prune.")

            # Check if it is a file or directory
            if source_endpoint.is_dir(relative_path):
                logger.info(f"Pruning directory {relative_path}")
                source_endpoint.rmdir(relative_path)
            else:
                logger.info(f"Pruning file {relative_path}")
                os.remove(source_endpoint.full_path(relative_path))
        else:
            logger.warning(f"File {relative_path} does not exist at the check point: {check_endpoint.name}.")
            logger.warning("Not safe to prune.")
        return


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

    @flow(name="prune_globus_endpoint")
    def _prune_globus_endpoint(
        relative_path: str,
        source_endpoint: GlobusEndpoint,
        check_endpoint: Union[GlobusEndpoint, None] = None,
        config: BeamlineConfig = None
    ) -> None:
        """
        Prune files from a Globus endpoint.

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


class PruneMethod(Enum):
    """
    Enum representing different prune methods.
    Use enum names as strings to identify trpruneansfer methods, ensuring a standard set of values.
    """
    GLOBUS = "globus"
    SIMPLE = "simple"
    HPSS = "hpss"


def get_prune_controller(
    prune_type: PruneMethod,
    config: BeamlineConfig
) -> PruneController:
    """
    Get the appropriate prune controller based on the prune type.

    Args:
        prune_type (str): The type of transfer to perform.
        config (BeamlineConfig): The configuration object.

    Returns:
        PruneController: The transfer controller object.
    """
    if prune_type == PruneMethod.GLOBUS:
        return GlobusPruneController(config)
    elif prune_type == PruneMethod.SIMPLE:
        return FileSystemPruneController(config)
    elif prune_type == PruneMethod.HPSS:
        from orchestration.prune_controller import HPSSPruneController
        from orchestration.sfapi import create_sfapi_client
        return HPSSPruneController(
            client=create_sfapi_client(),
            config=config
        )
    else:
        raise ValueError(f"Invalid transfer type: {prune_type}")
