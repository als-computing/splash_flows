from abc import ABC, abstractmethod
import datetime
from enum import Enum
import logging
import os
from typing import Generic, Optional, TypeVar

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
    Abstract base class for pruning controllers.

    This class defines the common interface that all prune controllers must implement,
    regardless of the specific pruning mechanism they use.

    Args:
        config (BeamlineConfig): Configuration object containing endpoints and credentials
    """
    def __init__(
        self,
        config: BeamlineConfig,
    ) -> None:
        """
        Initialize the prune controller with configuration.

        Args:
            config (BeamlineConfig): Configuration object containing endpoints and credentials
        """
        self.config = config
        logger.debug(f"Initialized {self.__class__.__name__} with config for beamline {config.beamline_id}")

    @abstractmethod
    def prune(
        self,
        file_path: str = None,
        source_endpoint: Endpoint = None,
        check_endpoint: Optional[Endpoint] = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        """
        Prune (delete) data from the source endpoint.

        This method either executes the pruning immediately or schedules it for future execution,
        depending on the days_from_now parameter.

        Args:
            file_path (str): The path to the file or directory to prune
            source_endpoint (Endpoint): The endpoint containing the data to be pruned
            check_endpoint (Optional[Endpoint]): If provided, verify data exists here before pruning
            days_from_now (datetime.timedelta): Delay before pruning; if 0, prune immediately

        Returns:
            bool: True if pruning was successful or scheduled successfully, False otherwise
        """
        pass


class FileSystemPruneController(PruneController[FileSystemEndpoint]):
    """
    Controller for pruning files from local file systems.

    This controller handles pruning operations on local or mounted file systems
    using standard file system operations.

    Args:
        config (BeamlineConfig): Configuration object containing file system paths
    """
    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        """
        Initialize the file system prune controller.

        Args:
            config (BeamlineConfig): Configuration object containing file system paths
        """
        super().__init__(config)
        logger.debug(f"Initialized FileSystemPruneController for beamline {config.beamline_id}")

    def prune(
        self,
        file_path: str = None,
        source_endpoint: FileSystemEndpoint = None,
        check_endpoint: Optional[FileSystemEndpoint] = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        """
        Prune (delete) data from a file system endpoint.

        If days_from_now is 0, executes pruning immediately.
        Otherwise, schedules pruning for future execution using Prefect.

        Args:
            file_path (str): The path to the file or directory to prune
            source_endpoint (FileSystemEndpoint): The file system endpoint containing the data
            check_endpoint (Optional[FileSystemEndpoint]): If provided, verify data exists here before pruning
            days_from_now (datetime.timedelta): Delay before pruning; if 0, prune immediately

        Returns:
            bool: True if pruning was successful or scheduled successfully, False otherwise
        """
        if not file_path:
            logger.error("No file_path provided for pruning operation")
            return False

        if not source_endpoint:
            logger.error("No source_endpoint provided for pruning operation")
            return False

        flow_name = f"prune_from_{source_endpoint.name}"
        logger.info(f"Setting up pruning of '{file_path}' from '{source_endpoint.name}'")

        # If days_from_now is 0, prune immediately
        if days_from_now.total_seconds() == 0:
            logger.info(f"Executing immediate pruning of '{file_path}' from '{source_endpoint.name}'")
            return self._prune_filesystem_endpoint(
                relative_path=file_path,
                source_endpoint=source_endpoint,
                check_endpoint=check_endpoint,
                config=self.config
            )
        else:
            # Otherwise, schedule pruning for future execution
            logger.info(f"Scheduling pruning of '{file_path}' from '{source_endpoint.name}' "
                        f"in {days_from_now.total_seconds()/86400:.1f} days")

            try:
                schedule_prefect_flow(
                    deployment_name="prune_filesystem_endpoint/prune_filesystem_endpoint",
                    flow_run_name=flow_name,
                    parameters={
                        "relative_path": file_path,
                        "source_endpoint": source_endpoint,
                        "check_endpoint": check_endpoint,
                        "config": self.config
                    },
                    duration_from_now=days_from_now,
                )
                logger.info(f"Successfully scheduled pruning task for {days_from_now.total_seconds()/86400:.1f} days from now")
                return True
            except Exception as e:
                logger.error(f"Failed to schedule pruning task: {str(e)}", exc_info=True)
                return False

    @staticmethod
    @flow(name="prune_filesystem_endpoint")
    def _prune_filesystem_endpoint(
        relative_path: str,
        source_endpoint: FileSystemEndpoint,
        check_endpoint: Optional[FileSystemEndpoint] = None,
        config: BeamlineConfig = None
    ) -> None:
        """
        Prefect flow that performs the actual filesystem pruning operation.

        Args:
            relative_path (str): The path of the file or directory to prune
            source_endpoint (FileSystemEndpoint): The source endpoint to prune from
            check_endpoint (Optional[FileSystemEndpoint]): If provided, verify data exists here before pruning
            config (Optional[BeamlineConfig]): Configuration object, if needed

        Returns:
            bool: True if pruning was successful, False otherwise
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
    """
    Controller for pruning files from Globus endpoints.

    This controller handles pruning operations on Globus endpoints using
    the Globus Transfer API.

    Args:
        config (BeamlineConfig): Configuration object containing Globus endpoints and credentials
    """
    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        """
        Initialize the file system prune controller.

        Args:
            config (BeamlineConfig): Configuration object containing file system paths
        """
        super().__init__(config)
        logger.debug(f"Initialized FileSystemPruneController for beamline {config.beamline_id}")

    def prune(
        self,
        file_path: str = None,
        source_endpoint: GlobusEndpoint = None,
        check_endpoint: Optional[GlobusEndpoint] = None,
        days_from_now: datetime.timedelta = 0
    ) -> bool:
        """
        Prune (delete) data from a file system endpoint.

        If days_from_now is 0, executes pruning immediately.
        Otherwise, schedules pruning for future execution using Prefect.

        Args:
            file_path (str): The path to the file or directory to prune
            source_endpoint (FileSystemEndpoint): The file system endpoint containing the data
            check_endpoint (Optional[FileSystemEndpoint]): If provided, verify data exists here before pruning
            days_from_now (datetime.timedelta): Delay before pruning; if 0, prune immediately

        Returns:
            bool: True if pruning was successful or scheduled successfully, False otherwise
        """
        if not file_path:
            logger.error("No file_path provided for pruning operation")
            return False

        if not source_endpoint:
            logger.error("No source_endpoint provided for pruning operation")
            return False

        # globus_settings = JSON.load("globus-settings").value
        # max_wait_seconds = globus_settings["max_wait_seconds"]
        flow_name = f"prune_from_{source_endpoint.name}"
        logger.info(f"Setting up pruning of '{file_path}' from '{source_endpoint.name}'")

        # If days_from_now is 0, prune immediately
        if days_from_now.total_seconds() == 0:
            logger.info(f"Executing immediate pruning of '{file_path}' from '{source_endpoint.name}'")
            return self._prune_filesystem_endpoint(
                relative_path=file_path,
                source_endpoint=source_endpoint,
                check_endpoint=check_endpoint,
                config=self.config
            )
        else:
            # Otherwise, schedule pruning for future execution
            logger.info(f"Scheduling pruning of '{file_path}' from '{source_endpoint.name}' "
                        f"in {days_from_now.total_seconds()/86400:.1f} days")

            try:
                schedule_prefect_flow(
                    deployment_name="prune_filesystem_endpoint/prune_filesystem_endpoint",
                    flow_run_name=flow_name,
                    parameters={
                        "relative_path": file_path,
                        "source_endpoint": source_endpoint,
                        "check_endpoint": check_endpoint,
                        "config": self.config
                    },
                    duration_from_now=days_from_now,
                )
                logger.info(f"Successfully scheduled pruning task for {days_from_now.total_seconds()/86400:.1f} days from now")
                return True
            except Exception as e:
                logger.error(f"Failed to schedule pruning task: {str(e)}", exc_info=True)
                return False

    @staticmethod
    @flow(name="prune_globus_endpoint")
    def _prune_globus_endpoint(
        relative_path: str,
        source_endpoint: GlobusEndpoint,
        check_endpoint: Optional[GlobusEndpoint] = None,
        config: BeamlineConfig = None
    ) -> None:
        """
        Prefect flow that performs the actual Globus endpoint pruning operation.

        Args:
            relative_path (str): The path of the file or directory to prune
            source_endpoint (GlobusEndpoint): The Globus endpoint to prune from
            check_endpoint (Optional[GlobusEndpoint]): If provided, verify data exists here before pruning
            config (BeamlineConfig): Configuration object with transfer client
        """
        logger.info(f"Running Globus pruning flow for '{relative_path}' from '{source_endpoint.name}'")

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

    These values are used to select the appropriate prune controller
    through the factory function get_prune_controller().

    Attributes:
        GLOBUS: Use Globus Transfer API for pruning operations
        SIMPLE: Use local file system operations for pruning
        HPSS: Use HPSS tape archive specific commands for pruning
    """
    GLOBUS = "globus"
    SIMPLE = "simple"
    HPSS = "hpss"


def get_prune_controller(
    prune_type: PruneMethod,
    config: BeamlineConfig
) -> PruneController:
    """
    Factory function to get the appropriate prune controller based on the prune type.

    Args:
        prune_type (PruneMethod): The type of pruning to perform
        config (BeamlineConfig): The configuration object containing endpoint information

    Returns:
        PruneController: The appropriate prune controller instance

    Raises:
        ValueError: If an invalid prune type is provided
    """
    logger.debug(f"Creating prune controller of type: {prune_type.name}")

    if prune_type == PruneMethod.GLOBUS:
        logger.debug("Returning GlobusPruneController")
        return GlobusPruneController(config)
    elif prune_type == PruneMethod.SIMPLE:
        logger.debug("Returning FileSystemPruneController")
        return FileSystemPruneController(config)
    elif prune_type == PruneMethod.HPSS:
        logger.debug("Importing and returning HPSSPruneController")
        from orchestration.hpss import HPSSPruneController
        from orchestration.sfapi import create_sfapi_client
        return HPSSPruneController(
            client=create_sfapi_client(),
            config=config
        )
    else:
        error_msg = f"Invalid prune type: {prune_type}"
        logger.error(error_msg)
        raise ValueError(error_msg)
