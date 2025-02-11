from abc import ABC, abstractmethod
from dotenv import load_dotenv
from enum import Enum
import logging

from orchestration.flows.bl832.config import Config832

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


class TomographyHPCController(ABC):
    """
    Abstract class for tomography HPC controllers.
    Provides interface methods for reconstruction and building multi-resolution datasets.

    Args:
        ABC: Abstract Base Class
    """
    def __init__(
        self,
        config: Config832
    ) -> None:
        self.config = config

    @abstractmethod
    def reconstruct(
        self,
        file_path: str = "",
    ) -> bool:
        """Perform tomography reconstruction

        :param file_path: Path to the file to reconstruct.
        :return: True if successful, False otherwise.
        """
        pass

    @abstractmethod
    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        """Generate multi-resolution version of reconstructed tomography

        :param file_path: Path to the file for which to build multi-resolution data.
        :return: True if successful, False otherwise.
        """
        pass


class HPC(Enum):
    """
    Enum representing different HPC environments.
    Use enum names as strings to identify HPC sites, ensuring a standard set of values.

    Members:
        ALCF: Argonne Leadership Computing Facility
        NERSC: National Energy Research Scientific Computing Center
    """
    ALCF = "ALCF"
    NERSC = "NERSC"
    OLCF = "OLCF"


def get_controller(
    hpc_type: HPC,
    config: Config832
) -> TomographyHPCController:
    """
    Factory function that returns an HPC controller instance for the given HPC environment.

    :param hpc_type: A string identifying the HPC environment (e.g., 'ALCF', 'NERSC').
    :return: An instance of a TomographyHPCController subclass corresponding to the given HPC environment.
    :raises ValueError: If an invalid or unsupported HPC type is specified.
    """
    if not isinstance(hpc_type, HPC):
        raise ValueError(f"Invalid HPC type provided: {hpc_type}")

    if not config:
        raise ValueError("Config object is required.")

    if hpc_type == HPC.ALCF:
        from orchestration.flows.bl832.alcf import ALCFTomographyHPCController
        return ALCFTomographyHPCController(
            config=config
        )
    elif hpc_type == HPC.NERSC:
        from orchestration.sfapi import create_sfapi_client
        from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
        return NERSCTomographyHPCController(
            client=create_sfapi_client(),
            config=config
        )
    elif hpc_type == HPC.OLCF:
        # TODO: Implement OLCF controller
        pass
    else:
        raise ValueError(f"Unsupported HPC type: {hpc_type}")


def do_it_all() -> None:
    controller = get_controller("ALCF")
    controller.reconstruct()
    controller.build_multi_resolution()

    file_path = ""
    controller = get_controller("NERSC")
    controller.reconstruct(
        file_path=file_path,
    )
    controller.build_multi_resolution(
        file_path=file_path,
    )


if __name__ == "__main__":
    do_it_all()
    logger.info("Done.")
