# orchestration/jobs/nersc/controller.py

"""Generic NERSC job controller.

Wraps the NERSC SFAPI and IRI API behind a common controller interface:
submit Slurm scripts, poll for completion, and perform basic filesystem
operations on Perlmutter. Beamline-specific job script builders live in
``orchestration/flows/<beamline>/nersc.py`` and subclass this controller.

Two authentication modes are supported, selected by :class:`NERSCLoginMethod`:

- :attr:`NERSCLoginMethod.SFAPI` — Iris-registered OAuth2 (NERSC OIDC).
  Operations go through ``sfapi_client.Client``.

- :attr:`NERSCLoginMethod.IRIAPI` — Globus bearer token. Operations are
  raw ``httpx`` calls to the IRI API.

The login method also selects the ``nersc_resources`` sub-dict
(``config.nersc_resources["iri"]`` vs ``["sfapi"]``), which carries the API
base URL and resource UUIDs used in URL construction.
"""

import json
import logging
import os
import time

import httpx
from sfapi_client import Client
from sfapi_client.compute import Machine

from orchestration.config import BeamlineConfig
from orchestration.jobs.controller import JobController
from orchestration.jobs.nersc.login import NERSCLoginMethod

logger = logging.getLogger(__name__)


class NERSCJobController(JobController):
    """Generic NERSC job submission and monitoring.

    Subclass for beamline-specific work (e.g. ``NERSCTomographyJobController``
    in ``flows/bl832/nersc.py``). This class knows nothing about tomography,
    BL832, or any particular pipeline — it only knows how to submit a Slurm
    script and wait for it.

    Args:
        config: Beamline configuration object. Must expose
            ``config.nersc_resources`` with ``"iri"`` and ``"sfapi"`` sub-dicts
            populated from the YAML.
        client: Authenticated NERSC client. Build with
            :func:`orchestration.jobs.nersc.login.create_nersc_client`.
        login_method: Which NERSC API the client targets. Determines URL
            construction and which dispatch branch is used in each method.

    Attributes:
        client: The authenticated NERSC client.
        login_method: The selected :class:`NERSCLoginMethod`.
        nersc_resources: Sub-dict of ``config.nersc_resources`` for the chosen
            login method. Contains ``api_base_url``, ``perlmutter_login``,
            ``perlmutter_job_submit``, ``compute_resource``, etc.
    """

    def __init__(
        self,
        config: BeamlineConfig,
        client: Client | httpx.Client | None = None,
        login_method: NERSCLoginMethod = NERSCLoginMethod.IRIAPI,
    ) -> None:
        super().__init__(config)
        self.client = client
        self.login_method = login_method

        if login_method is NERSCLoginMethod.IRIAPI:
            self.nersc_resources: dict[str, str] = config.nersc_resources["iri"]
        elif login_method is NERSCLoginMethod.SFAPI:
            self.nersc_resources = config.nersc_resources["sfapi"]
        else:
            raise ValueError(f"Unsupported NERSCLoginMethod: {login_method}")

    def get_nersc_username(self) -> str:
        """Return the NERSC username, used to construct ``pscratch`` paths.

        SFAPI exposes the username via the user endpoint. IRIAPI does not, so
        the username is read from the ``NERSC_USERNAME`` environment variable.

        Returns:
            NERSC username string.

        Raises:
            ValueError: If IRIAPI is selected and ``NERSC_USERNAME`` is unset.
        """
        if self.login_method is NERSCLoginMethod.SFAPI:
            return self.client.user().name

        username = os.getenv("NERSC_USERNAME")
        if not username:
            raise ValueError(
                "NERSC_USERNAME must be set in the environment when using IRIAPI."
            )
        return username

    def submit_job(self, job_script: str, num_nodes: int = 1) -> str:
        """Submit a Slurm batch script and return the job ID.

        For SFAPI, the script is passed verbatim to ``perlmutter.submit_job``.

        For IRIAPI, the script is parsed: SBATCH headers become attributes in
        the PSI/J-style job spec, and the script body (everything after the
        SBATCH block) becomes the ``pre_launch`` payload. The IRI API does not
        accept raw Slurm scripts, so this translation is necessary.

        Args:
            job_script: The full Slurm batch script to submit.
            num_nodes: Reserved for future use; the actual node count is read
                from the SBATCH ``-N`` line in the script.

        Returns:
            The submitted job ID as a string.

        Raises:
            ValueError: If ``self.login_method`` is unrecognized.
            httpx.HTTPStatusError: If the IRI API submission returns non-2xx.
        """
        if self.login_method is NERSCLoginMethod.SFAPI:
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            return str(job.jobid)

        elif self.login_method is NERSCLoginMethod.IRIAPI:
            return self._submit_job_iriapi(job_script)

        else:
            raise ValueError(f"Unhandled NERSCLoginMethod: {self.login_method}")

    def _submit_job_iriapi(self, job_script: str) -> str:
        """Translate a Slurm script into a PSI/J job spec and POST to IRI API."""
        sbatch_values: dict[str, object] = {}
        for line in job_script.splitlines():
            if not line.startswith("#SBATCH"):
                continue
            if "-q " in line:
                sbatch_values["queue_name"] = line.split("-q ")[-1].strip()
            elif "-A " in line:
                sbatch_values["account"] = line.split("-A ")[-1].strip()
            elif "--time=" in line:
                t = line.split("--time=")[-1].strip()
                parts = t.split(":")
                sbatch_values["duration"] = (
                    int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                )
            elif "-N " in line:
                sbatch_values["node_count"] = int(line.split("-N ")[-1].strip())
            elif "-C " in line:
                sbatch_values["constraint"] = line.split("-C ")[-1].strip()
            elif "--output=" in line:
                sbatch_values["stdout_path"] = line.split("--output=")[-1].strip()
            elif "--error=" in line:
                sbatch_values["stderr_path"] = line.split("--error=")[-1].strip()
            elif "--reservation=" in line:
                sbatch_values["reservation"] = line.split("--reservation=")[-1].strip()

        # Script body: everything except shebang and SBATCH headers.
        script_body = "\n".join(
            line for line in job_script.splitlines()
            if not line.startswith("#SBATCH") and not line.startswith("#!/")
        ).strip()

        constraint = sbatch_values.get("constraint", "cpu")
        is_gpu = "gpu" in str(constraint).lower()

        resources = {
            "node_count": sbatch_values.get("node_count", 1),
            "processes_per_node": 1,
            "exclusive_node_use": True,
        }
        if is_gpu:
            resources["gpu_cores_per_process"] = 4
        else:
            resources["cpu_cores_per_process"] = 128

        attributes = {
            "duration": sbatch_values.get("duration", 1800),
            "queue_name": sbatch_values.get("queue_name", "regular"),
            "account": sbatch_values.get("account", "als"),
            "custom_attributes": {"constraint": constraint},
        }
        if "reservation" in sbatch_values:
            attributes["reservation_id"] = sbatch_values["reservation"]

        job_spec = {
            "executable": "/bin/bash",
            # Reading the script from stdin isn't supported, so the body goes
            # into pre_launch (runs before the executable's main entry point).
            "arguments": ["-s"],
            "pre_launch": script_body,
            "resources": resources,
            "attributes": attributes,
        }
        if "stdout_path" in sbatch_values:
            job_spec["stdout_path"] = sbatch_values["stdout_path"]
        if "stderr_path" in sbatch_values:
            job_spec["stderr_path"] = sbatch_values["stderr_path"]

        response = self.client.post(
            f"/api/v1/compute/job/{self.nersc_resources['perlmutter_job_submit']}",
            json=job_spec,
        )
        if not response.is_success:
            logger.error(f"Job submission failed: {response.status_code} {response.text}")
            logger.error(f"Job spec was: {json.dumps(job_spec, indent=2)}")
        response.raise_for_status()
        return str(response.json()["id"])

    def wait_for_job(self, job_id: str) -> bool:
        """Block until a submitted job reaches a terminal state.

        For SFAPI, this delegates to ``sfapi_client``'s ``job.complete()``,
        which handles polling internally. For IRIAPI, polls the status
        endpoint every 60 seconds.

        Args:
            job_id: The job ID returned by :meth:`submit_job`.

        Returns:
            True if the job completed successfully, False if it failed,
            was canceled, or hit a timeout.

        Raises:
            ValueError: If ``self.login_method`` is unrecognized.
        """
        if self.login_method is NERSCLoginMethod.SFAPI:
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.job(jobid=job_id)
            job.complete()
            return True

        elif self.login_method is NERSCLoginMethod.IRIAPI:
            while True:
                response = self.client.get(
                    f"/api/v1/compute/status/{self.nersc_resources['compute_resource']}/{job_id}"
                )
                response.raise_for_status()
                state = response.json().get("status", {}).get("state")
                logger.info(f"Job {job_id} state: {state}")
                if state == "completed":
                    return True
                if state in ("failed", "canceled", "timeout"):
                    logger.error(f"Job {job_id} ended with state: {state}")
                    return False
                time.sleep(60)

        else:
            raise ValueError(f"Unhandled NERSCLoginMethod: {self.login_method}")

    def mkdir_remote(self, path: str) -> None:
        """Create a directory on Perlmutter.

        Equivalent to ``mkdir -p`` — intermediate directories are created and
        existing directories are not an error.

        Args:
            path: Absolute path to create on Perlmutter.

        Raises:
            ValueError: If ``self.login_method`` is unrecognized.
            httpx.HTTPStatusError: If the IRI filesystem call fails.
        """
        if self.login_method is NERSCLoginMethod.SFAPI:
            perlmutter = self.client.compute(Machine.perlmutter)
            perlmutter.run(f"mkdir -p {path}")
        elif self.login_method is NERSCLoginMethod.IRIAPI:
            response = self.client.post(
                f"/api/v1/filesystem/mkdir/{self.nersc_resources['perlmutter_login']}",
                json={"path": path, "parents": True},
            )
            response.raise_for_status()
        else:
            raise ValueError(f"Unhandled NERSCLoginMethod: {self.login_method}")

    def read_remote_file(self, path: str) -> str:
        """Read a file on Perlmutter and return its contents as a string.

        SFAPI runs ``cat`` synchronously. IRIAPI uses the async filesystem
        view endpoint, which returns a task_id that must be polled.

        Args:
            path: Absolute path to the file on Perlmutter.

        Returns:
            File contents as a string.

        Raises:
            ValueError: If ``self.login_method`` is unrecognized.
            RuntimeError: If the IRI read task ends in a failed state.
            TimeoutError: If the IRI read task does not complete within
                the local polling budget (~120 seconds).
        """
        if self.login_method is NERSCLoginMethod.SFAPI:
            perlmutter = self.client.compute(Machine.perlmutter)
            result = perlmutter.run(f"cat {path}")
            if isinstance(result, str):
                return result
            if hasattr(result, "output"):
                return result.output
            if hasattr(result, "stdout"):
                return result.stdout
            return str(result)

        elif self.login_method is NERSCLoginMethod.IRIAPI:
            return self._read_remote_file_iriapi(path)

        else:
            raise ValueError(f"Unhandled NERSCLoginMethod: {self.login_method}")

    def _read_remote_file_iriapi(self, path: str) -> str:
        """Read a remote file via the IRI async filesystem-view endpoint."""
        response = self.client.get(
            f"/api/v1/filesystem/view/{self.nersc_resources['perlmutter_login']}",
            params={"path": path},
        )
        response.raise_for_status()
        task_id = response.json().get("task_id")
        if not task_id:
            return response.text

        for _ in range(40):
            task_response = self.client.get(f"/api/v1/task/{task_id}")
            task_response.raise_for_status()
            task = task_response.json()
            status = task.get("status")
            if status == "completed":
                result = task.get("result", "")
                if isinstance(result, dict):
                    output = result.get("output", result)
                    if isinstance(output, dict):
                        return output.get("content", str(output))
                    return str(output)
                return str(result)
            elif status == "failed":
                raise RuntimeError(
                    f"File read task {task_id} failed: {task.get('result')}"
                )
            time.sleep(3)

        raise TimeoutError(f"File read task {task_id} did not complete")
