"""Tests for orchestration/jobs/alcf/controller.py — ALCFJobController.

Patch targets (confirmed from module-level imports in alcf/controller.py):
  orchestration.jobs.alcf.controller.Variable.get   (prefect Variable)
  orchestration.jobs.alcf.controller.Secret.load    (prefect Secret)
  orchestration.jobs.alcf.controller.get_run_logger (prefect run logger)
  orchestration.jobs.alcf.controller.Client         (globus_compute_sdk.Client)
  orchestration.jobs.alcf.controller.Executor       (globus_compute_sdk.Executor)
  orchestration.jobs.alcf.controller.time.sleep     (skip polling delays)

All TestALCFJobControllerInit and TestSubmit tests request the mock_alcf_prefect
fixture to satisfy Variable.get and Secret.load calls in __init__.

TestWaitForFuture uses ALCFJobController.wait_for_future() as a @staticmethod —
no instance construction needed, so mock_alcf_prefect is not required there.
"""

import pytest

from orchestration.jobs.alcf.controller import (
    ALCFJobController,
    _ALLOCATION_ROOT_VARIABLE,
    _GLOBUS_COMPUTE_ENDPOINT_SECRET,
)


# ── Init ──────────────────────────────────────────────────────────────────────

class TestALCFJobControllerInit:
    def test_reads_allocation_root_from_variable(self, mocker, mock_config, mock_alcf_prefect):
        ctrl = ALCFJobController(mock_config)
        assert ctrl.allocation_root == "/eagle/IRIProd/ALS"

    def test_reads_endpoint_id_from_secret(self, mocker, mock_config, mock_alcf_prefect):
        ctrl = ALCFJobController(mock_config)
        assert ctrl.endpoint_id == "mock-endpoint-uuid"

    def test_variable_get_called_with_correct_name(self, mocker, mock_config, mock_alcf_prefect):
        ALCFJobController(mock_config)
        mock_alcf_prefect.variable.assert_called_once_with(
            _ALLOCATION_ROOT_VARIABLE, _sync=True
        )

    def test_secret_load_called_with_correct_name(self, mocker, mock_config, mock_alcf_prefect):
        ALCFJobController(mock_config)
        # mock_alcf_prefect.secret IS the mock for Secret.load (not Secret itself)
        mock_alcf_prefect.secret.assert_called_once_with(_GLOBUS_COMPUTE_ENDPOINT_SECRET)

    def test_raises_when_allocation_root_missing(self, mocker, mock_config):
        # allocation_data.get(...) returns None → ValueError
        mocker.patch(
            "orchestration.jobs.alcf.controller.Variable.get",
            return_value={},  # key absent → .get() returns None
        )
        mocker.patch("orchestration.jobs.alcf.controller.Secret.load")
        with pytest.raises(ValueError, match="Allocation root not found"):
            ALCFJobController(mock_config)

    def test_stores_config(self, mocker, mock_config, mock_alcf_prefect):
        ctrl = ALCFJobController(mock_config)
        assert ctrl.config is mock_config


# ── submit ────────────────────────────────────────────────────────────────────

class TestSubmit:
    def test_constructs_client_and_submits_via_executor(self, mocker, mock_config, mock_alcf_prefect):
        mock_client_cls = mocker.patch("orchestration.jobs.alcf.controller.Client")
        mock_executor_cls = mocker.patch("orchestration.jobs.alcf.controller.Executor")

        mock_future = mocker.MagicMock()
        mock_executor_instance = mocker.MagicMock()
        mock_executor_instance.submit.return_value = mock_future
        mock_executor_cls.return_value.__enter__ = mocker.MagicMock(return_value=mock_executor_instance)
        mock_executor_cls.return_value.__exit__ = mocker.MagicMock(return_value=False)

        def noop():
            pass

        ctrl = ALCFJobController(mock_config)
        result = ctrl.submit(noop)

        mock_client_cls.assert_called_once()
        mock_executor_cls.assert_called_once_with(
            endpoint_id="mock-endpoint-uuid",
            client=mock_client_cls.return_value,
        )
        mock_executor_instance.submit.assert_called_once()
        assert result is mock_future

    def test_returns_future(self, mocker, mock_config, mock_alcf_prefect):
        mocker.patch("orchestration.jobs.alcf.controller.Client")
        mock_executor_cls = mocker.patch("orchestration.jobs.alcf.controller.Executor")

        mock_future = mocker.MagicMock()
        mock_executor_instance = mocker.MagicMock()
        mock_executor_instance.submit.return_value = mock_future
        mock_executor_cls.return_value.__enter__ = mocker.MagicMock(return_value=mock_executor_instance)
        mock_executor_cls.return_value.__exit__ = mocker.MagicMock(return_value=False)

        def identity(x):
            return x

        ctrl = ALCFJobController(mock_config)
        future = ctrl.submit(identity, 42, key="val")

        mock_executor_instance.submit.assert_called_once_with(identity, 42, key="val")
        assert future is mock_future


# ── wait_for_future ───────────────────────────────────────────────────────────

class TestWaitForFuture:
    """wait_for_future is @staticmethod — call directly without constructing an instance."""

    def _run_logger(self, mocker):
        """Patch get_run_logger and return a mock logger."""
        run_logger = mocker.MagicMock()
        mocker.patch(
            "orchestration.jobs.alcf.controller.get_run_logger",
            return_value=run_logger,
        )
        return run_logger

    def test_returns_true_on_success(self, mocker):
        mocker.patch("orchestration.jobs.alcf.controller.time.sleep")
        self._run_logger(mocker)

        future = mocker.MagicMock()
        future.done.return_value = True
        future.cancelled.return_value = False
        future.exception.return_value = None
        future.result.return_value = "output"

        result = ALCFJobController.wait_for_future(future, "reconstruction")
        assert result is True

    def test_returns_false_when_future_raises(self, mocker):
        mocker.patch("orchestration.jobs.alcf.controller.time.sleep")
        self._run_logger(mocker)

        future = mocker.MagicMock()
        future.done.return_value = True
        future.cancelled.return_value = False
        future.exception.return_value = RuntimeError("job failed")

        result = ALCFJobController.wait_for_future(future, "reconstruction")
        assert result is False

    def test_returns_false_when_cancelled(self, mocker):
        mocker.patch("orchestration.jobs.alcf.controller.time.sleep")
        self._run_logger(mocker)

        future = mocker.MagicMock()
        future.done.return_value = True
        future.cancelled.return_value = True

        result = ALCFJobController.wait_for_future(future, "reconstruction")
        assert result is False

    def test_returns_false_on_timeout(self, mocker):
        mocker.patch("orchestration.jobs.alcf.controller.time.sleep")
        self._run_logger(mocker)

        # Simulate time advancing past walltime by patching time.time
        call_count = [0]
        start = 1000.0

        def mock_time():
            val = start + call_count[0] * 700
            call_count[0] += 1
            return val

        mocker.patch("orchestration.jobs.alcf.controller.time.time", side_effect=mock_time)

        future = mocker.MagicMock()
        future.done.return_value = False  # never completes
        future.cancelled.return_value = False

        result = ALCFJobController.wait_for_future(
            future, "reconstruction", check_interval=1, walltime=600
        )
        future.cancel.assert_called()
        assert result is False

    def test_polls_until_done(self, mocker):
        mocker.patch("orchestration.jobs.alcf.controller.time.sleep")
        self._run_logger(mocker)

        future = mocker.MagicMock()
        future.done.side_effect = [False, False, True]
        future.cancelled.return_value = False
        future.exception.return_value = None
        future.result.return_value = "done"

        result = ALCFJobController.wait_for_future(future, "task", check_interval=1, walltime=3600)
        assert future.done.call_count == 3
        assert result is True
