"""Tests for orchestration/jobs/nersc/controller.py — NERSCJobController.

Patch targets:
  orchestration.jobs.nersc.controller.time.sleep  (skip 60-second polling delays)

No Prefect Variable/Secret imports in this module — no Prefect mocking needed.
"""

import pytest

from orchestration.jobs.nersc.controller import NERSCJobController
from orchestration.jobs.nersc.login import NERSCLoginMethod


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sfapi_controller(mocker, mock_config):
    client = mocker.MagicMock()
    user = mocker.MagicMock()
    user.name = "sfapiuser"
    client.user.return_value = user
    return NERSCJobController(mock_config, client=client, login_method=NERSCLoginMethod.SFAPI)


def _iriapi_controller(mocker, mock_config):
    client = mocker.MagicMock()
    # POST (submit_job): json() returns the job ID dict
    post_response = mocker.MagicMock(is_success=True)
    post_response.json.return_value = {"id": "job-99"}
    client.post.return_value = post_response
    # GET (wait_for_job, read_remote_file): default to completed state
    get_response = mocker.MagicMock(is_success=True)
    get_response.json.return_value = {"status": {"state": "completed"}}
    get_response.text = ""
    client.get.return_value = get_response
    return NERSCJobController(mock_config, client=client, login_method=NERSCLoginMethod.IRIAPI)


# ── Initialization ────────────────────────────────────────────────────────────

class TestNERSCJobControllerInit:
    def test_sfapi_stores_sfapi_nersc_resources(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        assert ctrl.nersc_resources == mock_config.nersc_resources["sfapi"]

    def test_iriapi_stores_iri_nersc_resources(self, mocker, mock_config):
        ctrl = _iriapi_controller(mocker, mock_config)
        assert ctrl.nersc_resources == mock_config.nersc_resources["iri"]

    def test_stores_login_method(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        assert ctrl.login_method is NERSCLoginMethod.SFAPI

    def test_unknown_login_method_raises(self, mocker, mock_config):
        bad_method = mocker.MagicMock()
        bad_method.__eq__ = lambda s, o: False
        bad_method.__ne__ = lambda s, o: True
        with pytest.raises(ValueError, match="Unsupported NERSCLoginMethod"):
            NERSCJobController(mock_config, client=None, login_method=bad_method)


# ── get_nersc_username ────────────────────────────────────────────────────────

class TestGetNerscUsername:
    def test_sfapi_reads_name_from_client(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        assert ctrl.get_nersc_username() == "sfapiuser"

    def test_iriapi_reads_from_env(self, mocker, mock_config, monkeypatch):
        monkeypatch.setenv("NERSC_USERNAME", "envuser")
        ctrl = _iriapi_controller(mocker, mock_config)
        assert ctrl.get_nersc_username() == "envuser"

    def test_iriapi_raises_when_env_unset(self, mocker, mock_config, monkeypatch):
        monkeypatch.delenv("NERSC_USERNAME", raising=False)
        ctrl = _iriapi_controller(mocker, mock_config)
        with pytest.raises(ValueError, match="NERSC_USERNAME must be set"):
            ctrl.get_nersc_username()


# ── submit_job ────────────────────────────────────────────────────────────────

class TestSubmitJob:
    def test_sfapi_returns_job_id_string(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        job = mocker.MagicMock()
        job.jobid = 12345
        ctrl.client.compute.return_value.submit_job.return_value = job
        result = ctrl.submit_job("#!/bin/bash\n#SBATCH -q debug\necho hi")
        assert result == "12345"

    def test_sfapi_calls_perlmutter_submit_job(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        job = mocker.MagicMock()
        job.jobid = "abc"
        perlmutter = ctrl.client.compute.return_value
        perlmutter.submit_job.return_value = job
        ctrl.submit_job("script")
        perlmutter.submit_job.assert_called_once_with("script")

    def test_iriapi_returns_job_id_string(self, mocker, mock_config):
        ctrl = _iriapi_controller(mocker, mock_config)
        script = "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A als\n#SBATCH --time=00:10:00\n#SBATCH -N 1\necho hi"
        result = ctrl.submit_job(script)
        assert result == "job-99"

    def test_iriapi_posts_to_job_submit_url(self, mocker, mock_config):
        ctrl = _iriapi_controller(mocker, mock_config)
        script = "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A als\n#SBATCH --time=00:10:00\n#SBATCH -N 1\necho hi"
        ctrl.submit_job(script)
        call_args = ctrl.client.post.call_args
        assert "mock-submit-uuid" in call_args[0][0]


# ── _submit_job_iriapi SBATCH parsing ─────────────────────────────────────────

class TestSubmitJobIRIAPI:
    """Tests the SBATCH header parsing logic in _submit_job_iriapi."""

    def _submit_and_capture_spec(self, mocker, mock_config, script):
        ctrl = _iriapi_controller(mocker, mock_config)
        captured = {}

        def capture_post(url, json=None, **kwargs):
            captured["json"] = json
            resp = mocker.MagicMock(is_success=True)
            resp.json.return_value = {"id": "captured-id"}
            return resp

        ctrl.client.post = capture_post
        ctrl._submit_job_iriapi(script)
        return captured["json"]

    def test_parses_queue_name(self, mocker, mock_config):
        script = (
            "#!/bin/bash\n#SBATCH -q premium\n#SBATCH -A als\n"
            "#SBATCH --time=00:10:00\n#SBATCH -N 1\necho hi"
        )
        spec = self._submit_and_capture_spec(mocker, mock_config, script)
        assert spec["attributes"]["queue_name"] == "premium"

    def test_parses_account(self, mocker, mock_config):
        script = (
            "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A myproject\n"
            "#SBATCH --time=00:10:00\n#SBATCH -N 1\necho hi"
        )
        spec = self._submit_and_capture_spec(mocker, mock_config, script)
        assert spec["attributes"]["account"] == "myproject"

    def test_parses_walltime_to_seconds(self, mocker, mock_config):
        script = (
            "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A als\n"
            "#SBATCH --time=01:30:00\n#SBATCH -N 1\necho hi"
        )
        spec = self._submit_and_capture_spec(mocker, mock_config, script)
        assert spec["attributes"]["duration"] == 5400  # 1h30m in seconds

    def test_parses_node_count(self, mocker, mock_config):
        script = (
            "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A als\n"
            "#SBATCH --time=00:10:00\n#SBATCH -N 4\necho hi"
        )
        spec = self._submit_and_capture_spec(mocker, mock_config, script)
        assert spec["resources"]["node_count"] == 4

    def test_cpu_constraint_adds_cpu_cores(self, mocker, mock_config):
        script = (
            "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A als\n"
            "#SBATCH --time=00:10:00\n#SBATCH -N 1\n#SBATCH -C cpu\necho hi"
        )
        spec = self._submit_and_capture_spec(mocker, mock_config, script)
        assert "cpu_cores_per_process" in spec["resources"]
        assert "gpu_cores_per_process" not in spec["resources"]

    def test_gpu_constraint_adds_gpu_cores(self, mocker, mock_config):
        script = (
            "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A als\n"
            "#SBATCH --time=00:10:00\n#SBATCH -N 1\n#SBATCH -C gpu\necho hi"
        )
        spec = self._submit_and_capture_spec(mocker, mock_config, script)
        assert "gpu_cores_per_process" in spec["resources"]
        assert "cpu_cores_per_process" not in spec["resources"]

    def test_reservation_included_when_present(self, mocker, mock_config):
        script = (
            "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A als\n"
            "#SBATCH --time=00:10:00\n#SBATCH -N 1\n#SBATCH --reservation=myres\necho hi"
        )
        spec = self._submit_and_capture_spec(mocker, mock_config, script)
        assert spec["attributes"]["reservation_id"] == "myres"

    def test_no_reservation_when_absent(self, mocker, mock_config):
        script = "#!/bin/bash\n#SBATCH -q debug\n#SBATCH -A als\n#SBATCH --time=00:10:00\n#SBATCH -N 1\necho hi"
        spec = self._submit_and_capture_spec(mocker, mock_config, script)
        assert "reservation_id" not in spec["attributes"]


# ── wait_for_job ──────────────────────────────────────────────────────────────

class TestWaitForJob:
    def test_sfapi_returns_true_on_complete(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        job = mocker.MagicMock()
        ctrl.client.compute.return_value.job.return_value = job
        result = ctrl.wait_for_job("12345")
        job.complete.assert_called_once()
        assert result is True

    def test_iriapi_returns_true_when_completed(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.nersc.controller.time.sleep")
        ctrl = _iriapi_controller(mocker, mock_config)
        ctrl.client.get.return_value.json.return_value = {"status": {"state": "completed"}}
        result = ctrl.wait_for_job("42")
        assert result is True

    def test_iriapi_returns_false_on_failed(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.nersc.controller.time.sleep")
        ctrl = _iriapi_controller(mocker, mock_config)
        ctrl.client.get.return_value.json.return_value = {"status": {"state": "failed"}}
        result = ctrl.wait_for_job("42")
        assert result is False

    def test_iriapi_returns_false_on_canceled(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.nersc.controller.time.sleep")
        ctrl = _iriapi_controller(mocker, mock_config)
        ctrl.client.get.return_value.json.return_value = {"status": {"state": "canceled"}}
        result = ctrl.wait_for_job("42")
        assert result is False

    def test_iriapi_polls_until_terminal_state(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.nersc.controller.time.sleep")
        ctrl = _iriapi_controller(mocker, mock_config)
        responses = [
            {"status": {"state": "running"}},
            {"status": {"state": "running"}},
            {"status": {"state": "completed"}},
        ]
        ctrl.client.get.return_value.json.side_effect = responses
        result = ctrl.wait_for_job("42")
        assert ctrl.client.get.call_count == 3
        assert result is True


# ── mkdir_remote ──────────────────────────────────────────────────────────────

class TestMkdirRemote:
    def test_sfapi_runs_mkdir(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        ctrl.mkdir_remote("/pscratch/sd/t/testuser/mydir")
        ctrl.client.compute.return_value.run.assert_called_once()
        cmd = ctrl.client.compute.return_value.run.call_args[0][0]
        assert "mkdir -p" in cmd
        assert "/pscratch/sd/t/testuser/mydir" in cmd

    def test_iriapi_posts_to_mkdir_url(self, mocker, mock_config):
        ctrl = _iriapi_controller(mocker, mock_config)
        ctrl.mkdir_remote("/pscratch/sd/t/testuser/mydir")
        ctrl.client.post.assert_called()
        url = ctrl.client.post.call_args[0][0]
        assert "mock-login-uuid" in url

    def test_iriapi_posts_path_in_body(self, mocker, mock_config):
        ctrl = _iriapi_controller(mocker, mock_config)
        ctrl.mkdir_remote("/some/path")
        body = ctrl.client.post.call_args[1]["json"]
        assert body["path"] == "/some/path"
        assert body["parents"] is True


# ── read_remote_file ──────────────────────────────────────────────────────────

class TestReadRemoteFile:
    def test_sfapi_returns_string_result(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        ctrl.client.compute.return_value.run.return_value = "file contents"
        result = ctrl.read_remote_file("/some/file.txt")
        assert result == "file contents"

    def test_sfapi_extracts_output_attribute(self, mocker, mock_config):
        ctrl = _sfapi_controller(mocker, mock_config)
        run_result = mocker.MagicMock(spec=[])  # no __str__ shortcuts
        run_result.output = "from output attr"
        ctrl.client.compute.return_value.run.return_value = run_result
        result = ctrl.read_remote_file("/some/file.txt")
        assert result == "from output attr"

    def test_iriapi_returns_file_contents_on_completed_task(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.nersc.controller.time.sleep")
        ctrl = _iriapi_controller(mocker, mock_config)

        # First call: GET /filesystem/view → task_id
        # Subsequent calls: GET /task/<id> → status=completed + result
        view_response = mocker.MagicMock(is_success=True)
        view_response.json.return_value = {"task_id": "task-abc"}
        view_response.text = ""

        task_response = mocker.MagicMock(is_success=True)
        task_response.json.return_value = {"status": "completed", "result": "file data"}

        ctrl.client.get.side_effect = [view_response, task_response]
        result = ctrl.read_remote_file("/pscratch/data.txt")
        assert result == "file data"

    def test_iriapi_raises_on_failed_task(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.nersc.controller.time.sleep")
        ctrl = _iriapi_controller(mocker, mock_config)

        view_response = mocker.MagicMock(is_success=True)
        view_response.json.return_value = {"task_id": "task-fail"}
        view_response.text = ""

        task_response = mocker.MagicMock(is_success=True)
        task_response.json.return_value = {"status": "failed", "result": "disk error"}

        ctrl.client.get.side_effect = [view_response, task_response]
        with pytest.raises(RuntimeError, match="failed"):
            ctrl.read_remote_file("/pscratch/data.txt")

    def test_iriapi_raises_timeout_after_40_polls(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.nersc.controller.time.sleep")
        ctrl = _iriapi_controller(mocker, mock_config)

        view_response = mocker.MagicMock(is_success=True)
        view_response.json.return_value = {"task_id": "task-slow"}
        view_response.text = ""

        # Always return "pending" — never completes
        pending_response = mocker.MagicMock(is_success=True)
        pending_response.json.return_value = {"status": "pending", "result": None}

        ctrl.client.get.side_effect = [view_response] + [pending_response] * 40
        with pytest.raises(TimeoutError):
            ctrl.read_remote_file("/pscratch/slow.txt")
