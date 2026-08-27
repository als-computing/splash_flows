# orchestration/jobs/alcf/controller.py

"""Generic ALCF job controller.

Wraps Globus Compute's submit/wait pattern behind a common controller
interface. Beamline-specific job functions (the actual Python callables
executed remotely on ALCF compute nodes) live in
``orchestration/flows/<beamline>/alcf.py`` and subclass this controller.

ALCF execution is fundamentally different from NERSC in shape: instead of
submitting a Slurm batch script and getting a job ID back, you submit a
Python callable (with arguments) and get a :class:`concurrent.futures.Future`
back. The wait/poll logic is correspondingly different too.

Authentication is via a Globus Compute endpoint ID stored in the Prefect
Secret block ``globus-compute-endpoint``. The allocation root path on the
remote filesystem (e.g. ``/eagle/IRIProd/ALS``) is stored in the Prefect
Variable ``alcf-allocation-root-path``.
"""

import logging
import time
from concurrent.futures import Future
from typing import Any, Callable

from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode
from prefect import get_run_logger
from prefect.blocks.system import Secret
from prefect.variables import Variable

from orchestration.config import BeamlineConfig
from orchestration.jobs.controller import JobController

logger = logging.getLogger(__name__)


# Defaults for wait_for_future. Override per-call when a job is known to be
# longer-running or faster-polling than typical.
_DEFAULT_CHECK_INTERVAL_SECONDS: int = 20
_DEFAULT_WALLTIME_SECONDS: int = 1200  # 20 minutes

# Prefect block / variable names. Defined as constants so callers and tests
# have a single place to override.
_GLOBUS_COMPUTE_ENDPOINT_SECRET: str = "globus-compute-endpoint"
_ALLOCATION_ROOT_VARIABLE: str = "alcf-allocation-root-path"


class ALCFJobController(JobController):
    """Generic ALCF job submission and monitoring via Globus Compute.

    Subclass for beamline-specific work (e.g. ``ALCFTomographyJobController``
    in ``flows/bl832/alcf.py``). This class knows nothing about tomography,
    BL832, or any particular pipeline — it only knows how to submit a
    callable to ALCF and wait for it.

    Args:
        config: Beamline configuration object. Stored as ``self.config``
            for subclass use; this controller doesn't read any specific
            fields from it.

    Attributes:
        allocation_root: Remote path prefix on the ALCF filesystem
            (e.g. ``/eagle/IRIProd/ALS``). Read from the
            ``alcf-allocation-root-path`` Prefect Variable. Available for
            subclasses to construct script and data paths.
        endpoint_id: Globus Compute endpoint UUID, loaded from the
            ``globus-compute-endpoint`` Prefect Secret block.
    """

    def __init__(self, config: BeamlineConfig) -> None:
        super().__init__(config)

        allocation_data = Variable.get(_ALLOCATION_ROOT_VARIABLE, _sync=True)
        self.allocation_root: str = allocation_data.get(_ALLOCATION_ROOT_VARIABLE)
        if not self.allocation_root:
            raise ValueError(
                f"Allocation root not found in Prefect Variable "
                f"'{_ALLOCATION_ROOT_VARIABLE}'"
            )
        logger.info(f"Allocation root loaded: {self.allocation_root}")

        self.endpoint_id: str = Secret.load(_GLOBUS_COMPUTE_ENDPOINT_SECRET).get()

    def submit(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Future:
        """Submit a callable to the Globus Compute endpoint.

        The callable is shipped to the ALCF compute node and executed there
        with the supplied args/kwargs. Code-serialization uses
        :class:`CombinedCode` so the function can reference imports and
        helpers defined in the same module without manual packing.

        Args:
            func: The function to run remotely. Should be picklable and
                self-contained — imports inside the function body are
                evaluated on the remote node, not locally.
            *args: Positional arguments passed to ``func`` on the remote node.
            **kwargs: Keyword arguments passed to ``func`` on the remote node.

        Returns:
            A :class:`concurrent.futures.Future`-compatible object. Use
            :meth:`wait_for_future` to poll until done.
        """
        gcc = Client(code_serialization_strategy=CombinedCode())
        # Note: the Executor context manager handles connection cleanup, but
        # the Future remains valid after the executor exits — submission has
        # already been dispatched to the remote endpoint by that point.
        with Executor(endpoint_id=self.endpoint_id, client=gcc) as fxe:
            future = fxe.submit(func, *args, **kwargs)
        return future

    @staticmethod
    def wait_for_future(
        future: Future,
        task_name: str,
        check_interval: int = _DEFAULT_CHECK_INTERVAL_SECONDS,
        walltime: int = _DEFAULT_WALLTIME_SECONDS,
    ) -> bool:
        """Block until a Globus Compute future completes or hits walltime.

        Polls ``future.done()`` every ``check_interval`` seconds. If the
        future is still not done after ``walltime`` seconds total, the
        future is cancelled and ``False`` is returned. Logging uses Prefect's
        run logger so progress shows up in the flow run UI.

        Args:
            future: The future returned by :meth:`submit`.
            task_name: Short descriptive name used in log messages
                (e.g. ``"reconstruction"``, ``"tiff to zarr"``).
            check_interval: Seconds between ``future.done()`` polls.
            walltime: Maximum total seconds to wait before giving up and
                cancelling.

        Returns:
            True if the task completed successfully (future returned without
            raising). False if the task was cancelled, raised an exception,
            timed out, or an error occurred during polling.
        """
        run_logger = get_run_logger()
        start_time = time.time()
        success = False

        try:
            previous_state = None
            while not future.done():
                elapsed_time = time.time() - start_time
                if elapsed_time > walltime:
                    run_logger.error(
                        f"The {task_name} task exceeded the walltime of "
                        f"{walltime} seconds. Cancelling the Globus Compute job."
                    )
                    future.cancel()
                    return False

                if future.cancelled():
                    run_logger.warning(f"The {task_name} task was cancelled.")
                    return False

                # Assume the task is running if not done and not cancelled.
                # Log once per state transition rather than every poll.
                if previous_state != "running":
                    run_logger.info(f"The {task_name} task is running...")
                    previous_state = "running"

                time.sleep(check_interval)

            # Future is done — check whether it was cancelled or raised.
            if future.cancelled():
                run_logger.warning(
                    f"The {task_name} task was cancelled after completion."
                )
                return False

            exception = future.exception()
            if exception:
                run_logger.error(
                    f"The {task_name} task raised an exception: {exception}"
                )
                return False

            result = future.result()
            run_logger.info(
                f"The {task_name} task completed successfully with result: {result}"
            )
            success = True

        except Exception as e:
            run_logger.error(
                f"An error occurred while waiting for the {task_name} task: {e}"
            )
            success = False

        finally:
            elapsed_time = time.time() - start_time
            run_logger.info(
                f"Total duration of the {task_name} task: {elapsed_time:.2f} seconds."
            )

        return success
