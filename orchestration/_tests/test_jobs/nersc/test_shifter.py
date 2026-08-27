"""Tests for orchestration/jobs/nersc/shifter.py.

All tests use mocker.MagicMock(spec=NERSCJobController) — spec= is required so
that drift in the controller interface (renamed/removed methods) breaks these
tests rather than silently passing.

check_shifter_image SFAPI branch deferred import note:
  check_shifter_image does `from sfapi_client.compute import Machine` inside the
  function body (line 204). When setting up the SFAPI mock, use a plain
  mocker.MagicMock() (not spec'd) for the compute() return value — Machine.perlmutter
  is evaluated after the patch, so a spec'd mock keyed on the Machine class would fail.

Patch targets:
  orchestration.jobs.nersc.shifter.time.sleep  (skips 30-second pull delay)
"""

from orchestration.jobs.nersc.controller import NERSCJobController
from orchestration.jobs.nersc.login import NERSCLoginMethod
from orchestration.jobs.nersc.shifter import check_shifter_image, pull_shifter_image


def _make_controller(mocker, login_method=NERSCLoginMethod.SFAPI):
    controller = mocker.MagicMock(spec=NERSCJobController)
    controller.login_method = login_method
    # client is an instance attribute (set in __init__) — not in the class spec,
    # so set it explicitly here so tests can configure controller.client.compute.
    controller.client = mocker.MagicMock()
    controller.get_nersc_username.return_value = "testuser"
    controller.submit_job.return_value = "job-123"
    controller.wait_for_job.return_value = True
    controller.read_remote_file.return_value = "ghcr.io/als-computing/image:latest found"
    return controller


class TestPullShifterImage:
    def test_submits_pull_script_and_returns_success(self, mocker):
        mocker.patch("orchestration.jobs.nersc.shifter.time.sleep")
        controller = _make_controller(mocker)
        result = pull_shifter_image(controller, "docker:ghcr.io/als/image:latest")
        controller.submit_job.assert_called_once()
        controller.wait_for_job.assert_called_once_with("job-123")
        assert result is True

    def test_script_contains_image_name(self, mocker):
        mocker.patch("orchestration.jobs.nersc.shifter.time.sleep")
        controller = _make_controller(mocker)
        pull_shifter_image(controller, "docker:ghcr.io/als/myimage:v2")
        script = controller.submit_job.call_args[0][0]
        assert "docker:ghcr.io/als/myimage:v2" in script

    def test_returns_true_when_wait_false(self, mocker):
        mocker.patch("orchestration.jobs.nersc.shifter.time.sleep")
        controller = _make_controller(mocker)
        result = pull_shifter_image(controller, "docker:image:latest", wait=False)
        controller.submit_job.assert_called_once()
        controller.wait_for_job.assert_not_called()
        assert result is True

    def test_returns_false_on_submit_exception(self, mocker):
        mocker.patch("orchestration.jobs.nersc.shifter.time.sleep")
        controller = _make_controller(mocker)
        controller.submit_job.side_effect = RuntimeError("NERSC down")
        result = pull_shifter_image(controller, "docker:image:latest")
        assert result is False

    def test_mkdir_remote_called_for_log_dir(self, mocker):
        mocker.patch("orchestration.jobs.nersc.shifter.time.sleep")
        controller = _make_controller(mocker)
        pull_shifter_image(controller, "docker:image:latest")
        controller.mkdir_remote.assert_called_once()
        log_dir_arg = controller.mkdir_remote.call_args[0][0]
        assert "testuser" in log_dir_arg


class TestCheckShifterImage:
    def test_sfapi_returns_true_when_grep_matches(self, mocker):
        controller = _make_controller(mocker, login_method=NERSCLoginMethod.SFAPI)
        # Use plain MagicMock (not spec'd) for perlmutter — Machine.perlmutter
        # is evaluated after the patch inside the function body.
        perlmutter = mocker.MagicMock()
        perlmutter.run.return_value = "ghcr.io/als/image:latest found in cache"
        controller.client.compute.return_value = perlmutter

        result = check_shifter_image(controller, "docker:ghcr.io/als/image:latest")
        assert result is True

    def test_sfapi_returns_false_when_no_match(self, mocker):
        controller = _make_controller(mocker, login_method=NERSCLoginMethod.SFAPI)
        perlmutter = mocker.MagicMock()
        perlmutter.run.return_value = ""
        controller.client.compute.return_value = perlmutter

        result = check_shifter_image(controller, "docker:ghcr.io/als/image:latest")
        assert result is False

    def test_iriapi_submits_check_job_and_reads_output(self, mocker):
        mocker.patch("orchestration.jobs.nersc.shifter.time.sleep")
        controller = _make_controller(mocker, login_method=NERSCLoginMethod.IRIAPI)
        controller.read_remote_file.return_value = "ghcr.io/als/image:latest cached"

        result = check_shifter_image(controller, "docker:ghcr.io/als/image:latest")
        controller.submit_job.assert_called_once()
        controller.wait_for_job.assert_called_once_with("job-123")
        controller.read_remote_file.assert_called_once()
        assert result is True

    def test_iriapi_returns_false_when_image_not_in_output(self, mocker):
        mocker.patch("orchestration.jobs.nersc.shifter.time.sleep")
        controller = _make_controller(mocker, login_method=NERSCLoginMethod.IRIAPI)
        controller.read_remote_file.return_value = ""

        result = check_shifter_image(controller, "docker:ghcr.io/als/image:latest")
        assert result is False

    def test_returns_false_on_exception(self, mocker):
        controller = _make_controller(mocker, login_method=NERSCLoginMethod.SFAPI)
        controller.client.compute.side_effect = RuntimeError("network error")

        result = check_shifter_image(controller, "docker:image:latest")
        assert result is False
