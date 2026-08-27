# orchestration/jobs/nersc/shifter.py

"""Shifter container image cache management at NERSC.

Shifter is NERSC's container runtime. Images pulled into the per-system cache
are available to all subsequent jobs via ``shifter --image=<image>`` without
a per-job pull penalty. These helpers manage that cache.

Both functions take a :class:`NERSCJobController` so they can use its
``submit_job`` / ``wait_for_job`` / ``read_remote_file`` primitives. This
keeps the controller focused on the submit/wait/filesystem core and avoids
ballooning it with one-off Shifter operations.
"""

import logging
import time
from typing import TYPE_CHECKING

from orchestration.jobs.nersc.login import NERSCLoginMethod

if TYPE_CHECKING:
    from orchestration.jobs.nersc.controller import NERSCJobController

logger = logging.getLogger(__name__)


def _shifter_pull_script(
    image: str,
    log_dir: str,
    *,
    account: str,
    qos: str = "debug",
    walltime: str = "0:15:00",
) -> str:
    """
    Build the Slurm script that pulls a Shifter image.

    Args:
    - image: The container image to pull
    - log_dir: Directory to store Slurm output and error logs
    - account: Slurm account to charge the pull job to
    - qos: Slurm QoS for the pull job (default: "debug")
    - walltime: Walltime for the pull job (default: "0:15:00")

    Returns:
    - A string containing the Slurm job script to pull the specified Shifter image.
    """
    return f"""#!/bin/bash
#SBATCH -q {qos}
#SBATCH -A {account}
#SBATCH -C cpu
#SBATCH --job-name=shifter_pull
#SBATCH --output={log_dir}/shifter_pull_%j.out
#SBATCH --error={log_dir}/shifter_pull_%j.err
#SBATCH -N 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time={walltime}

echo "Starting Shifter image pull at $(date)"
echo "Image: {image}"

echo "Checking existing images..."
shifterimg images | grep -E "$(echo {image} | sed 's/:/.*/')" || true

echo "Pulling image..."
shifterimg -v pull {image}
PULL_STATUS=$?

if [ $PULL_STATUS -eq 0 ]; then
    echo "Image pull successful"
else
    echo "Image pull failed with status $PULL_STATUS"
    exit 1
fi

echo "Verifying image..."
shifterimg images | grep -E "$(echo {image} | sed 's/:/.*/')"

echo "Completed at $(date)"
"""


def _shifter_check_script(
    image: str,
    output_file: str,
    *,
    account: str,
    qos: str = "debug",
    walltime: str = "0:05:00",
) -> str:
    """
    Build the Slurm script that writes Shifter cache state to a file.

    Args:
    - image: The container image to check for in the Shifter cache
    - output_file: The file to write the check output to
    - account: Slurm account to charge the check job to
    - qos: Slurm QoS for the check job (default: "debug")
    - walltime: Walltime for the check job (default: "0:05:00")

    Returns:
    - A string containing the Slurm job script to check for the specified Shifter image and write the results.
    """
    return f"""#!/bin/bash
#SBATCH -q {qos}
#SBATCH -A {account}
#SBATCH -C cpu
#SBATCH -N 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time={walltime}
shifterimg images | grep -E "$(echo {image} | sed 's/:/.*/g')" > {output_file} 2>&1 || true
"""


def _user_log_dir(controller: "NERSCJobController") -> str:
    """
    Return the per-user log directory used for Shifter Slurm output.
    This is a convention for storing logs in a user-specific location on the shared filesystem.
    It is not an official NERSC requirement, but it helps avoid cluttering home directories and keeps logs organized.
    The path is typically of the form: /pscratch/sd/<first_letter_of_username>/<username>/shifter_logs

    Args:
    - controller: A NERSCJobController instance used to get the username for constructing the log directory path.
    Returns:
    - A string representing the path to the user's log directory for Shifter jobs.
    """
    username = controller.get_nersc_username()
    return f"/pscratch/sd/{username[0]}/{username}/shifter_logs"


def pull_shifter_image(
    controller: "NERSCJobController",
    image: str,
    wait: bool = True,
    account: str = "als",
) -> bool:
    """Pull a container image into NERSC's Shifter cache.

    Run this once when an image is updated, not before every job that uses it.
    After the image is cached, jobs using ``--image=<image>`` start much faster.

    Args:
        controller: A NERSC job controller used to submit and monitor the
            Slurm pull job.
        image: Container image to pull, e.g.
            ``"docker:ghcr.io/als-computing/tomopy-multinode:latest"``.
        wait: If True, block until the pull job finishes and return its
            success state. If False, return True as soon as the job is
            submitted.
        account: Slurm account to charge the pull job to. Defaults to
            ``"als"``; pass a different value for other beamlines.

    Returns:
        ``True`` if the pull succeeded (or was submitted, when ``wait=False``),
        ``False`` if submission or the pull itself failed.
    """
    logger.info(f"Pulling Shifter image: {image}")

    log_dir = _user_log_dir(controller)
    controller.mkdir_remote(log_dir)

    job_script = _shifter_pull_script(image, log_dir, account=account)

    try:
        job_id = controller.submit_job(job_script)
        logger.info(f"Submitted Shifter pull job: {job_id}")

        if not wait:
            logger.info(f"Returning early; check status with job ID {job_id}")
            return True

        time.sleep(30)
        success = controller.wait_for_job(job_id)
        logger.info(f"Shifter image pull {'completed successfully' if success else 'failed'}.")
        return success

    except Exception as e:
        logger.error(f"Error during Shifter image pull: {e}")
        return False


def check_shifter_image(
    controller: "NERSCJobController",
    image: str,
    account: str = "als",
) -> bool:
    """Check whether a container image is already in NERSC's Shifter cache.

    Dispatches on the controller's login method. SFAPI can run ``shifterimg``
    synchronously via the utilities endpoint; IRIAPI must submit a Slurm job,
    wait for it, then read the captured stdout.

    Args:
        controller: A NERSC job controller used to query Shifter.
        image: Container image to check.
        account: Slurm account for the IRIAPI check job. Ignored for SFAPI.

    Returns:
        ``True`` if the image is present in the cache, ``False`` otherwise
        (including on error).
    """
    from sfapi_client.compute import Machine

    logger.info(f"Checking Shifter cache for: {image}")

    try:
        if controller.login_method is NERSCLoginMethod.SFAPI:
            # Synchronous: run shifterimg directly via the utilities endpoint
            perlmutter = controller.client.compute(Machine.perlmutter)
            result = perlmutter.run(
                f"shifterimg images | grep -E \"$(echo {image} | sed 's/:/.*/g')\""
            )
            output = (
                result if isinstance(result, str)
                else getattr(result, "output", None) or getattr(result, "stdout", "") or str(result)
            )

        elif controller.login_method is NERSCLoginMethod.IRIAPI:
            # Async: submit a one-off job, wait, read the captured output file
            log_dir = _user_log_dir(controller)
            controller.mkdir_remote(log_dir)
            output_file = f"{log_dir}/shifter_check_{int(time.time())}.txt"

            job_script = _shifter_check_script(image, output_file, account=account)
            job_id = controller.submit_job(job_script)
            controller.wait_for_job(job_id)
            output = controller.read_remote_file(output_file)

        else:
            raise ValueError(f"Unhandled NERSCLoginMethod: {controller.login_method}")

        if output.strip():
            logger.info(f"Image found in Shifter cache: {output.strip()}")
            return True
        logger.info(f"Image not found in Shifter cache: {image}")
        return False

    except Exception as e:
        logger.warning(f"Error checking Shifter cache: {e}")
        return False
