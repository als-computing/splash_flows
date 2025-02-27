from abc import ABC, abstractmethod
from dotenv import load_dotenv
from enum import Enum
import datetime
import logging
import os
import time
from typing import Generic, Optional, TypeVar

import globus_sdk

# Import the generic Beamline configuration class.
from orchestration.config import BeamlineConfig
from orchestration.globus.transfer import GlobusEndpoint, start_transfer
from orchestration.prometheus_utils import PrometheusMetrics
from orchestration.transfer_endpoints import FileSystemEndpoint, TransferEndpoint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


Endpoint = TypeVar("Endpoint", bound=TransferEndpoint)


class TransferController(Generic[Endpoint], ABC):
    """
    Abstract class for transferring data.

    Args:
        ABC: Abstract Base Class
    """
    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        self.config = config

    @abstractmethod
    def copy(
        self,
        file_path: str = None,
        source: Endpoint = None,
        destination: Endpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint.

        Args:
            file_path (str): The path of the file to copy.
            source (Endpoint): The source endpoint.
            destination (Endpoint): The destination endpoint.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        pass


class GlobusTransferController(TransferController[GlobusEndpoint]):
    def __init__(
        self,
        config: BeamlineConfig,
        prometheus_metrics: Optional[PrometheusMetrics] = None
    ) -> None:
        super().__init__(config)
        self.prometheus_metrics = prometheus_metrics

    """
    Use Globus Transfer to move data between endpoints.

    Args:
        TransferController: Abstract class for transferring data.
    """

    def get_transfer_file_info(
        self,
        task_id: str,
        transfer_client: Optional[globus_sdk.TransferClient] = None
    ) -> Optional[dict]:
        """
        Get information about a completed transfer from the Globus API.

        Args:
            task_id (str): The Globus transfer task ID
            transfer_client (TransferClient, optional): TransferClient instance

        Returns:
            Optional[dict]: Task information including bytes_transferred, or None if unavailable
        """
        if transfer_client is None:
            transfer_client = self.config.tc

        try:
            task_info = transfer_client.get_task(task_id)
            task_dict = task_info.data

            if task_dict.get('status') == 'SUCCEEDED':
                bytes_transferred = task_dict.get('bytes_transferred', 0)
                bytes_checksummed = task_dict.get('bytes_checksummed', 0)
                files_transferred = task_dict.get('files_transferred', 0)
                effective_bytes_per_second = task_dict.get('effective_bytes_per_second', 0)
                return {
                    'bytes_transferred': bytes_transferred,
                    'bytes_checksummed': bytes_checksummed,
                    'files_transferred': files_transferred,
                    'effective_bytes_per_second': effective_bytes_per_second
                }

            return None

        except Exception as e:
            logger.error(f"Error getting transfer task info: {e}")
            return None

    def collect_and_push_metrics(
        self,
        start_time: float,
        end_time: float,
        file_path: str,
        source: GlobusEndpoint,
        destination: GlobusEndpoint,
        file_size: int,
        transfer_speed: float,
        success: bool
    ) -> None:
        """
        Collect transfer metrics and push them to Prometheus.

        Args:
            start_time (float): Transfer start time as UNIX timestamp.
            end_time (float): Transfer end time as UNIX timestamp.
            file_path (str): The path of the transferred file.
            source (GlobusEndpoint): The source endpoint.
            destination (GlobusEndpoint): The destination endpoint.
            file_size (int): Size of the transferred file in bytes.
            transfer_speed (float): Transfer speed (bytes/second) provided by Globus
            success (bool): Whether the transfer was successful.
        """
        try:
            # Get machine_name
            machine_name = destination.name

            # Convert UNIX timestamps to ISO format strings
            start_datetime = datetime.datetime.fromtimestamp(start_time, tz=datetime.timezone.utc)
            end_datetime = datetime.datetime.fromtimestamp(end_time, tz=datetime.timezone.utc)
            start_timestamp = start_datetime.isoformat()
            end_timestamp = end_datetime.isoformat()

            # Calculate duration in seconds
            duration_seconds = end_time - start_time

            # Calculate transfer speed (bytes per second)
            # transfer_speed = file_size / duration_seconds if duration_seconds > 0 and file_size > 0 else 0

            # Prepare metrics dictionary
            metrics = {
                "timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "local_path": os.path.join(source.root_path, file_path),
                "remote_path": os.path.join(destination.root_path, file_path),
                "bytes_transferred": file_size,
                "duration_seconds": duration_seconds,
                "transfer_speed": transfer_speed,
                "status": "success" if success else "failed",
                "machine": machine_name
            }

            # Push metrics to Prometheus
            self.prometheus_metrics.push_metrics_to_prometheus(metrics, logger)

        except Exception as e:
            logger.error(f"Error collecting or pushing metrics: {e}")

    def copy(
        self,
        file_path: str = None,
        source: GlobusEndpoint = None,
        destination: GlobusEndpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint.

        Args:
            file_path (str): The path of the file to copy.
            source (GlobusEndpoint): The source endpoint.
            destination (GlobusEndpoint): The destination endpoint.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """

        if not file_path:
            logger.error("No file_path provided")
            return False

        if not source or not destination:
            logger.error("Source or destination endpoint not provided")
            return False

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        # Remove leading slash if present
        if file_path[0] == "/":
            file_path = file_path[1:]

        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")

        # Start the timer
        transfer_start_time = time.time()
        success = False
        task_id = None  # Initialize task_id here to prevent UnboundLocalError
        file_size = 0   # Initialize file_size here as well

        try:
            success, task_id = start_transfer(
                transfer_client=self.config.tc,
                source_endpoint=source,
                source_path=source_path,
                dest_endpoint=destination,
                dest_path=dest_path,
                max_wait_seconds=600,
                logger=logger,
            )

            if success:
                logger.info("Transfer completed successfully.")
            else:
                logger.error("Transfer failed.")

        except globus_sdk.services.transfer.errors.TransferAPIError as e:
            logger.error(f"Failed to submit transfer: {e}")

        finally:
            # Stop the timer and calculate the duration
            transfer_end_time = time.time()

            # Try to get transfer info from the completed task
            if task_id:
                transfer_info = self.get_transfer_file_info(task_id)
                if transfer_info:
                    file_size = transfer_info.get('bytes_transferred', 0)
                    transfer_speed = transfer_info.get('effective_bytes_per_second', 0)
                    logger.info(f"Globus Task Info: Transferred {file_size} bytes ")
                    logger.info(f"Globus Task Info: Effective speed: {transfer_speed} bytes/second")

            # Collect and push metrics if enabled
            if self.prometheus_metrics and file_size > 0:
                self.collect_and_push_metrics(
                    start_time=transfer_start_time,
                    end_time=transfer_end_time,
                    file_path=file_path,
                    source=source,
                    destination=destination,
                    file_size=file_size,
                    transfer_speed=transfer_speed,
                    success=success,
                )

            return success


class SimpleTransferController(TransferController[FileSystemEndpoint]):
    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        super().__init__(config)
    """
    Use a simple 'cp' command to move data within the same system.

    Args:
        TransferController: Abstract class for transferring data.
    """

    def copy(
        self,
        file_path: str = None,
        source: FileSystemEndpoint = None,
        destination: FileSystemEndpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint using the 'cp' command.

        Args:
            file_path (str): The path of the file to copy.
            source (FileSystemEndpoint): The source endpoint.
            destination (FileSystemEndpoint): The destination endpoint.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        if not file_path:
            logger.error("No file_path provided.")
            return False
        if not source or not destination:
            logger.error("Source or destination endpoint not provided.")
            return False

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        if file_path.startswith("/"):
            file_path = file_path[1:]

        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")

        # Start the timer
        start_time = time.time()

        try:
            result = os.system(f"cp -r '{source_path}' '{dest_path}'")
            if result == 0:
                logger.info("Transfer completed successfully.")
                return True
            else:
                logger.error(f"Transfer failed with exit code {result}.")
                return False
        except Exception as e:
            logger.error(f"Transfer failed: {e}")
            return False
        finally:
            # Stop the timer and calculate the duration
            elapsed_time = time.time() - start_time
            logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")


class CopyMethod(Enum):
    """
    Enum representing different transfer methods.
    Use enum names as strings to identify transfer methods, ensuring a standard set of values.
    """
    GLOBUS = "globus"
    SIMPLE = "simple"
    CFS_TO_HPSS = "cfs_to_hpss"
    HPSS_TO_CFS = "hpss_to_cfs"


def get_transfer_controller(
    transfer_type: CopyMethod,
    config: BeamlineConfig,
    prometheus_metrics: Optional[PrometheusMetrics] = None
) -> TransferController:
    """
    Get the appropriate transfer controller based on the transfer type.

    Args:
        transfer_type (str): The type of transfer to perform.
        config (Config832): The configuration object.

    Returns:
        TransferController: The transfer controller object.
    """
    if transfer_type == CopyMethod.GLOBUS:
        return GlobusTransferController(config, prometheus_metrics)
    elif transfer_type == CopyMethod.SIMPLE:
        return SimpleTransferController(config)
    elif transfer_type == CopyMethod.CFS_TO_HPSS:
        from orchestration.hpss import CFSToHPSSTransferController
        from orchestration.sfapi import create_sfapi_client
        return CFSToHPSSTransferController(
            client=create_sfapi_client(),
            config=config
        )
    elif transfer_type == CopyMethod.HPSS_TO_CFS:
        from orchestration.hpss import HPSSToCFSTransferController
        from orchestration.sfapi import create_sfapi_client
        return HPSSToCFSTransferController(
            client=create_sfapi_client(),
            config=config
        )
    else:
        raise ValueError(f"Invalid transfer type: {transfer_type}")


if __name__ == "__main__":
    from orchestration.flows.bl832.config import Config832
    config = Config832()
    transfer_type = CopyMethod.GLOBUS
    globus_transfer_controller = get_transfer_controller(transfer_type, config)
    globus_transfer_controller.copy(
        file_path="dabramov/test.txt",
        source=config.alcf832_raw,
        destination=config.alcf832_scratch
    )

    simple_transfer_controller = get_transfer_controller(CopyMethod.SIMPLE, config)
    success = simple_transfer_controller.copy(
        file_path="test.rtf",
        source=FileSystemEndpoint("source", "/Users/david/Documents/copy_test/test_source/"),
        destination=FileSystemEndpoint("destination", "/Users/david/Documents/copy_test/test_destination/")
    )

    if success:
        logger.info("Simple transfer succeeded.")
    else:
        logger.error("Simple transfer failed.")
