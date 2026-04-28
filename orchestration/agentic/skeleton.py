from __future__ import annotations

from academy.agent import action
from academy.handle import Handle
from academy.manager import Manager
from academy.exchange.cloud import HttpExchangeFactory
from concurrent.futures import ProcessPoolExecutor
from agentic_blueprint_catalog.observability.user_agent import UserAgent
from agentic_blueprint_catalog.observability.monitored_agent import MonitoredAgent
import logging
import random
from globus_compute_sdk import Executor as GlobusExecutor
from academy.logging import init_logging
import asyncio


class SegmentationAgent(MonitoredAgent):
    """Segmentation Agent."""

    def __init__(self, user_agent_handle: Handle[UserAgent]) -> None:
        super().__init__(user_agent_handle=user_agent_handle)

    @action
    async def segment(self, scan_id: str, reconstructed_volumes: str) -> str:
        """Segment reconstructed volumes."""
        logging.info(f"Segmenting {scan_id} on {reconstructed_volumes=}")
        asyncio.sleep(random.randint(5, 10) + 5)
        return "Done"


class ReconstructionAgent(MonitoredAgent):
    """Reconstruction Agent."""

    def __init__(
        self,
        segmentation_agent_handle: Handle[SegmentationAgent],
        user_agent_handle: Handle[UserAgent],
    ) -> None:
        """Initialize Reconstruction Agent."""
        super().__init__(user_agent_handle=user_agent_handle)
        self.segmentation_agent_handle = segmentation_agent_handle
        print("Reconstruction agent init done")

    @action
    async def reconstruct(self, scan_id: str, scan_path: str) -> tuple[str, str]:
        """Reconstruct the scan image."""

        logging.info("Reconstructing scan image")
        return " Done!!!!!!", "LogPath"

    @action
    async def sanity_check(self, log_path: str) -> str:
        """Sanity check the logs.

        Till we plug in an LLM we will use random which is almost as good (^_^)
        """
        if random.randint(1, 10) > 5:
            logging.warning("Sanity check failed  (*__*) ")
            return "Check Failed!"
        else:
            logging.info("Sanity check passed !!!!")
            return "Check Passed!"

    @action
    async def reconstruct_and_segment(self, scan_id: str, scan_path: str) -> str:
        """Reconstruct -> Sanity check -> Segment."""
        reconstructed = await self.reconstruct(scan_id, scan_path)
        sanity = await self.sanity_check(reconstructed)
        if sanity == "Check Failed!":
            # self.user_agent.prompt_user_agent("Something went wrong. Should i continue?",
            #                                  ["yes", "shut it down"])
            logging.warning("Reconstruction failed. Aborting segmentation")
            return "Segmentation aborted"

        segmented = await self.segmentation_agent_handle.segment(scan_id, reconstructed)
        return segmented


async def main():

    init_logging()

    executors = {
        "local": ProcessPoolExecutor(max_workers=1),
        # The polaris config will not work until it is installed on the
        # polaris environment loaded via config_key
        "polaris": GlobusExecutor(
            endpoint_id="9a947ba5-f537-4681-acf3-cc66485aadec",
            user_endpoint_config={
                "queue": "debug",
                "walltime": "00:10:00",
                # TODO: FIX THE PATH BELOW!
                "config_key": "source /home/yadunand/setup_academy.sh",
                "account": "ModCon",
            },
        ),
    }
    # Create manager with agents and their assigned executors
    async with await Manager.from_exchange_factory(
        factory=HttpExchangeFactory(),  # Use cloud hosted exchange
        executors=executors,  # Use Process pool for testing
    ) as manager:
        user_agent = await manager.launch(UserAgent)

        seg_agent = await manager.launch(
            SegmentationAgent,
            kwargs={"user_agent_handle": user_agent},
            executor="local",
        )

        reco_agent = await manager.launch(
            ReconstructionAgent,
            kwargs={
                "segmentation_agent_handle": seg_agent,
                "user_agent_handle": user_agent,
            },
            executor="polaris",
        )

        for i in range(5):
            await reco_agent.reconstruct_and_segment(
                scan_id=f"test_{i}", scan_path=f"/imaginary/path/test_{i}"
            )

        await asyncio.sleep(10)


if __name__ == "__main__":
    print("Starting main")
    raise SystemExit(asyncio.run(main()))
