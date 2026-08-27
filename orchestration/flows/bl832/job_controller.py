from abc import ABC, abstractmethod
from dotenv import load_dotenv
import logging

from orchestration.config import BeamlineConfig
from orchestration.jobs.controller import JobTarget
from orchestration.jobs.nersc.login import NERSCLoginMethod  # noqa: F401 — re-exported for callers

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


# HPC is a BL832-scoped alias for backwards compat.
# New code should use JobTarget from orchestration.jobs.controller.
# TODO: retire this alias after all call sites are migrated.
HPC = JobTarget


class TomographyHPCController(ABC):
    """Abstract class for tomography HPC controllers.

    Provides interface methods for reconstruction and building multi-resolution
    datasets. Stays in bl832/job_controller.py because reconstruct and
    build_multi_resolution are tomography-specific, not generic infrastructure.
    """

    def __init__(self, config: BeamlineConfig) -> None:
        self.config = config

    @abstractmethod
    def reconstruct(self, file_path: str = "") -> bool:
        """Perform tomography reconstruction.

        :param file_path: Path to the file to reconstruct.
        :return: True if successful, False otherwise.
        """
        pass

    @abstractmethod
    def build_multi_resolution(self, file_path: str = "") -> bool:
        """Generate multi-resolution version of reconstructed tomography.

        :param file_path: Path to the file for which to build multi-resolution data.
        :return: True if successful, False otherwise.
        """
        pass


def get_controller(
    hpc_type: HPC,
    config: BeamlineConfig,
    login_method: NERSCLoginMethod | None = None,
) -> TomographyHPCController:
    """Factory: return the appropriate tomography HPC controller.

    :param hpc_type: Target HPC site (use HPC or JobTarget enum).
    :param config: Beamline configuration object.
    :param login_method: NERSC-only; which API to authenticate against.
    :return: A TomographyHPCController subclass instance.
    :raises ValueError: If hpc_type is invalid or config is missing.
    """
    if not isinstance(hpc_type, HPC):
        raise ValueError(f"Invalid HPC type provided: {hpc_type}")

    if not config:
        raise ValueError("Config object is required.")

    if hpc_type == HPC.ALCF:
        from orchestration.flows.bl832.alcf import ALCFTomographyHPCController
        return ALCFTomographyHPCController(config=config)

    elif hpc_type == HPC.NERSC:
        from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
        from orchestration.jobs.nersc.login import create_nersc_client
        resolved_login_method = (
            login_method if isinstance(login_method, NERSCLoginMethod)
            else NERSCLoginMethod.SFAPI
        )
        client = create_nersc_client(config, resolved_login_method)
        return NERSCTomographyHPCController(
            config=config,
            client=client,
            login_method=resolved_login_method,
        )

    elif hpc_type == HPC.OLCF:
        # TODO: Implement OLCF controller
        raise NotImplementedError("OLCF controller not yet implemented")

    else:
        raise ValueError(f"Unsupported HPC type: {hpc_type}")
