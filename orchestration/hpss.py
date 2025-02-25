"""
This module contains the HPSS flow for BL832.
"""
import logging
from typing import List, Optional

from prefect import flow
from prefect.blocks.core import Block
from pydantic import BaseModel, Field

from orchestration.config import BeamlineConfig
from orchestration.flows.bl832.config import Config832
from orchestration.transfer_controller import get_transfer_controller, CopyMethod
from orchestration.transfer_endpoints import FileSystemEndpoint, HPSSEndpoint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TapeArchiveQueueBlock(Block, BaseModel):
    """
    Custom Prefect block for tracking projects pending tape archiving and those already archived.
    """
    pending_projects: List[str] = Field(default_factory=list)
    moved_projects: List[str] = Field(default_factory=list)

    def add_pending(self, project: str) -> None:
        """Add a project to the pending list if not already added or archived."""
        if project and project not in self.pending_projects and project not in self.moved_projects:
            self.pending_projects.append(project)

    def mark_moved(self, project: str) -> None:
        """Remove a project from pending and add it to the moved (archived) list."""
        if project in self.pending_projects:
            self.pending_projects.remove(project)
        if project and project not in self.moved_projects:
            self.moved_projects.append(project)


class TapeArchiveQueue:
    """
    Reusable controller for managing tape archive project queues.

    Attributes:
        block_name (str): The name used to persist the tape archive queue block.
    """
    def __init__(self, block_name: str = "TAPE_ARCHIVE_QUEUE_BLOCK"):
        self.block_name = block_name

    def load_block(self) -> TapeArchiveQueueBlock:
        """
        Load the persisted tape archive queue block, or create a new one if it doesn't exist.
        """
        try:
            block = TapeArchiveQueueBlock.load(self.block_name)
        except Exception:
            block = TapeArchiveQueueBlock()
        return block

    def save_block(self, block: TapeArchiveQueueBlock) -> None:
        """Persist the tape archive queue block."""
        block.save(self.block_name, overwrite=True)

    def enqueue_project(self, project: str) -> None:
        """Add a project to the pending (to be archived) list."""
        block = self.load_block()
        block.add_pending(project)
        self.save_block(block)
        logger.info(f"Enqueued project '{project}' for tape archiving.")

    def mark_project_as_moved(self, project: str) -> None:
        """Mark a project as moved (archived) by removing it from pending and adding it to moved."""
        block = self.load_block()
        block.mark_moved(project)
        self.save_block(block)
        logger.info(f"Project '{project}' marked as archived.")

    def get_pending_projects(self) -> List[str]:
        """Retrieve the list of projects pending tape archiving."""
        block = self.load_block()
        return block.pending_projects


@flow(name="cfs_to_hpss_flow")
def cfs_to_hpss_flow(
    file_path: str = None,
    source: FileSystemEndpoint = None,
    destination: HPSSEndpoint = None,
    config: BeamlineConfig = Config832()
) -> bool:
    """
    The CFS to HPSS flow for BL832.

    Parameters
    ----------
    file_path : str
        The path of the file to transfer.
    source : FileSystemEndpoint
        The source endpoint.
    destination : HPSSEndpoints
        The destination endpoint.
    """

    logger.info("Running cfs_to_hpss_flow")
    logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

    logger.info("Configuring transfer controller for CFS_TO_HPSS.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.CFS_TO_HPSS,
        config=config
    )

    logger.info("CFSToHPSSTransferController selected. Initiating transfer.")
    result = transfer_controller.copy(
        file_path=file_path,
        source=source,
        destination=destination
    )

    return result


@flow(name="hpss_to_cfs_flow")
def hpss_to_cfs_flow(
    file_path: str = None,
    source: HPSSEndpoint = None,
    destination: FileSystemEndpoint = None,
    files_to_extract: Optional[List[str]] = None,
    config: BeamlineConfig = Config832()
) -> bool:
    """
    The HPSS to CFS flow for BL832.

    Parameters
    ----------
    file_path : str
        The path of the file to transfer.
    source_endpoint : HPSSEndpoint
        The source endpoint.
    destination_endpoint : FileSystemEndpoint
        The destination endpoint.
    """

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.HPSS_TO_CFS,
        config=config
    )

    result = transfer_controller.copy(
        file_path=file_path,
        source=source,
        destination=destination,
        files_to_extract=files_to_extract,
    )

    return result


if __name__ == "__main__":

    config = Config832()
    project_name = "ALS-11193_nbalsara"
    source = FileSystemEndpoint(
        name="CFS",
        root_path="/global/cfs/cdirs/als/data_mover/8.3.2/raw/"
    )
    destination = HPSSEndpoint(
        name="HPSS",
        root_path=config.hpss_alsdev["root_path"]
    )
    cfs_to_hpss_flow(
        file_path=f"{project_name}",
        source=source,
        destination=destination,
        config=config
    )
