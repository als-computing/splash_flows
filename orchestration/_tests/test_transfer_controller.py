# test_transfer_controller.py

import pytest
from pytest_mock import MockFixture
import time
from unittest.mock import MagicMock, patch
from uuid import uuid4

import globus_sdk
from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness

from .test_globus import MockTransferClient


<<<<<<< HEAD
=======
@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    """Patch time.sleep to return immediately to speed up tests."""
    monkeypatch.setattr(time, "sleep", lambda x: None)

>>>>>>> 67e515a (Added logic for HPSSToCFSTransferController() copy() method. Now it will launch an SFAPI Slurm job script that handles each case: a single file is requested (non-tar), an entire tar file, or specified files within a tar (partial). Added pytest cases in test_transfer_controller.py for the new HPSS controllers.)

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """
    A pytest fixture that automatically sets up and tears down the Prefect test harness
    for the entire test session. It creates and saves test secrets and configurations
    required for Globus integration.

    Yields:
        None
    """
    with prefect_test_harness():
        # Create ephemeral secrets in the local Prefect test database
        globus_client_id = Secret(value=str(uuid4()))
        globus_client_id.save(name="globus-client-id")

        globus_client_secret = Secret(value=str(uuid4()))
        globus_client_secret.save(name="globus-client-secret")

        yield


@pytest.fixture(scope="session")
def transfer_controller_module():
    """
    Defer importing orchestration.transfer_controller until after
    the prefect_test_fixture is loaded. This prevents Prefect from
    trying to load secrets at import time.
    """
    from orchestration.transfer_controller import (
        FileSystemEndpoint,
        GlobusTransferController,
        SimpleTransferController,
        CFSToHPSSTransferController,
        HPSSToCFSTransferController,
        HPSSEndpoint,
        get_transfer_controller,
        CopyMethod,
    )
    return {
        "FileSystemEndpoint": FileSystemEndpoint,
        "GlobusTransferController": GlobusTransferController,
        "SimpleTransferController": SimpleTransferController,
        "CFSToHPSSTransferController": CFSToHPSSTransferController,
        "HPSSToCFSTransferController": HPSSToCFSTransferController,
        "HPSSEndpoint": HPSSEndpoint,
        "get_transfer_controller": get_transfer_controller,
        "CopyMethod": CopyMethod,
    }


class MockEndpoint:
    def __init__(self, root_path, uuid_value=None):
        self.root_path = root_path
        self.uuid = uuid_value or str(uuid4())
        self.uri = f"mock_endpoint_uri_{self.uuid}"


@pytest.fixture
def mock_config832():
    """
    Mock the Config832 class to provide necessary configurations.
    """
    with patch("orchestration.flows.bl832.nersc.Config832") as MockConfig:
        mock_config = MockConfig.return_value
        mock_config.endpoints = {
            "alcf832_raw": MockEndpoint("/alcf832_raw"),
        }
        mock_config.tc = MockTransferClient()
        yield mock_config


@pytest.fixture
def mock_globus_endpoint():
    """
    A pytest fixture that returns a mocked GlobusEndpoint.
    If your orchestration.globus.transfer also loads secrets at import,
    you may need to similarly defer that import behind another fixture.
    """
    from orchestration.globus.transfer import GlobusEndpoint
    endpoint = GlobusEndpoint(
        name="mock_globus_endpoint",
        root_path="/mock_globus_root/",
        uuid="mock_endpoint_id",
        uri="mock_endpoint_uri"
    )
    return endpoint


@pytest.fixture
def mock_file_system_endpoint(transfer_controller_module):
    """
    A pytest fixture that returns a FileSystemEndpoint instance.
    """
    FileSystemEndpoint = transfer_controller_module["FileSystemEndpoint"]
    endpoint = FileSystemEndpoint(
        name="mock_filesystem_endpoint",
        root_path="/mock_fs_root",
        uri="mock_uri"
    )
    return endpoint


class MockSecret:
    value = str(uuid4())


# --------------------------------------------------------------------------
# Tests for get_transfer_controller
# --------------------------------------------------------------------------

def test_get_transfer_controller_globus(mock_config832, transfer_controller_module):
    CopyMethod = transfer_controller_module["CopyMethod"]
    get_transfer_controller = transfer_controller_module["get_transfer_controller"]
    GlobusTransferController = transfer_controller_module["GlobusTransferController"]

    controller = get_transfer_controller(CopyMethod.GLOBUS, mock_config832)
    assert isinstance(controller, GlobusTransferController), (
        "Expected GlobusTransferController for GLOBUS transfer type."
    )


def test_get_transfer_controller_simple(mock_config832, transfer_controller_module):
    CopyMethod = transfer_controller_module["CopyMethod"]
    get_transfer_controller = transfer_controller_module["get_transfer_controller"]
    SimpleTransferController = transfer_controller_module["SimpleTransferController"]

    controller = get_transfer_controller(CopyMethod.SIMPLE, mock_config832)
    assert isinstance(controller, SimpleTransferController), (
        "Expected SimpleTransferController for SIMPLE transfer type."
    )


def test_get_transfer_controller_invalid(mock_config832, transfer_controller_module):
    get_transfer_controller = transfer_controller_module["get_transfer_controller"]
    with pytest.raises(ValueError, match="Invalid transfer type"):
        get_transfer_controller("invalid_type", mock_config832)


# --------------------------------------------------------------------------
# Tests for GlobusTransferController
# --------------------------------------------------------------------------

def test_globus_transfer_controller_copy_success(
    mock_config832, mock_globus_endpoint, mocker: MockFixture, transfer_controller_module
):
    """
    Test a successful copy() operation using GlobusTransferController.
    We mock start_transfer to return True.
    """
    GlobusTransferController = transfer_controller_module["GlobusTransferController"]
    MockSecretClass = MockSecret

    # Patch any Secret.load calls to avoid real Prefect Cloud calls
    mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecretClass())

    with patch("orchestration.transfer_controller.start_transfer", return_value=(True, "mock-task-id")) as mock_start_transfer:
        controller = GlobusTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_globus_endpoint,
            destination=mock_globus_endpoint,
        )

        assert result is True, "Expected True when transfer completes successfully."
        mock_start_transfer.assert_called_once()

        # Verify arguments passed to start_transfer
        _, called_kwargs = mock_start_transfer.call_args
        assert called_kwargs["source_endpoint"] == mock_globus_endpoint
        assert called_kwargs["dest_endpoint"] == mock_globus_endpoint
        assert "max_wait_seconds" in called_kwargs


def test_globus_transfer_controller_copy_failure(
    mock_config832, mock_globus_endpoint, mocker: MockFixture, transfer_controller_module
):
    """
    Test a failing copy() operation using GlobusTransferController.
    We mock start_transfer to return False, indicating a transfer failure.
    """
    GlobusTransferController = transfer_controller_module["GlobusTransferController"]
    MockSecretClass = MockSecret

    mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecretClass())

    with patch("orchestration.transfer_controller.start_transfer", return_value=(False, "mock-task-id")) as mock_start_transfer:
        controller = GlobusTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_globus_endpoint,
            destination=mock_globus_endpoint,
        )
        assert result is False, "Expected False when transfer fails."
        mock_start_transfer.assert_called_once()


def test_globus_transfer_controller_copy_exception(
    mock_config832, mock_globus_endpoint, transfer_controller_module
):
    """
    Test copy() operation that raises a TransferAPIError exception.
    """
    GlobusTransferController = transfer_controller_module["GlobusTransferController"]
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.reason = "Bad Request"

    with patch(
        "orchestration.transfer_controller.start_transfer",
        side_effect=globus_sdk.services.transfer.errors.TransferAPIError(mock_response, "Mocked Error")
    ) as mock_start_transfer:
        controller = GlobusTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_globus_endpoint,
            destination=mock_globus_endpoint,
        )
        assert result is False, "Expected False when TransferAPIError is raised."
        mock_start_transfer.assert_called_once()

def test_globus_transfer_controller_with_metrics(
    mock_config832, mock_globus_endpoint, transfer_controller_module
):
    """
    Test GlobusTransferController with PrometheusMetrics integration.
    """
    GlobusTransferController = transfer_controller_module["GlobusTransferController"]
    from orchestration.prometheus_utils import PrometheusMetrics
    mock_prometheus = MagicMock(spec=PrometheusMetrics)
    
    with patch("orchestration.transfer_controller.start_transfer", return_value=(True, "mock-task-id")) as mock_start_transfer:
        # Create the controller with mock prometheus metrics
        controller = GlobusTransferController(mock_config832, prometheus_metrics=mock_prometheus)
        
        # Set up mock for get_transfer_file_info
        mock_transfer_info = {"bytes_transferred": 1024 * 1024}  # 1MB
        controller.get_transfer_file_info = MagicMock(return_value=mock_transfer_info)
        
        # Execute the copy operation
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_globus_endpoint,
            destination=mock_globus_endpoint,
        )
        
        # Verify transfer was successful
        assert result is True
        mock_start_transfer.assert_called_once()
        
        # Verify metrics were collected and pushed
        controller.get_transfer_file_info.assert_called_once_with("mock-task-id")
        mock_prometheus.push_metrics_to_prometheus.assert_called_once()
        
        # Verify the metrics data
        metrics_data = mock_prometheus.push_metrics_to_prometheus.call_args[0][0]
        assert metrics_data["bytes_transferred"] == 1024 * 1024
        assert metrics_data["status"] == "success"
        assert "timestamp" in metrics_data
        assert "end_timestamp" in metrics_data
        assert "duration_seconds" in metrics_data
        assert "transfer_speed" in metrics_data
        assert "machine" in metrics_data
        assert "local_path" in metrics_data
        assert "remote_path" in metrics_data

# --------------------------------------------------------------------------
# Tests for SimpleTransferController
# --------------------------------------------------------------------------

def test_simple_transfer_controller_no_file_path(
    mock_config832, mock_file_system_endpoint, transfer_controller_module
):
    SimpleTransferController = transfer_controller_module["SimpleTransferController"]
    controller = SimpleTransferController(mock_config832)
    result = controller.copy(
        file_path="",
        source=mock_file_system_endpoint,
        destination=mock_file_system_endpoint,
    )
    assert result is False, "Expected False when no file_path is provided."


def test_simple_transfer_controller_no_source_or_destination(mock_config832, transfer_controller_module):
    SimpleTransferController = transfer_controller_module["SimpleTransferController"]
    controller = SimpleTransferController(mock_config832)
    result = controller.copy(
        file_path="test.txt",
        source=None,
        destination=None,
    )
    assert result is False, "Expected False when either source or destination is None."


def test_simple_transfer_controller_copy_success(
    mock_config832, mock_file_system_endpoint, transfer_controller_module
):
    SimpleTransferController = transfer_controller_module["SimpleTransferController"]
    with patch("os.system", return_value=0) as mock_os_system:
        controller = SimpleTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_file_system_endpoint,
            destination=mock_file_system_endpoint,
        )

        assert result is True, "Expected True when os.system returns 0."
        mock_os_system.assert_called_once()
        command_called = mock_os_system.call_args[0][0]
        assert "cp -r" in command_called, "Expected cp command in os.system call."


def test_simple_transfer_controller_copy_failure(
    mock_config832, mock_file_system_endpoint, transfer_controller_module
):
    SimpleTransferController = transfer_controller_module["SimpleTransferController"]
    with patch("os.system", return_value=1) as mock_os_system:
        controller = SimpleTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_file_system_endpoint,
            destination=mock_file_system_endpoint,
        )

        assert result is False, "Expected False when os.system returns non-zero."
        mock_os_system.assert_called_once()
        command_called = mock_os_system.call_args[0][0]
        assert "cp -r" in command_called, "Expected cp command in os.system call."


def test_simple_transfer_controller_copy_exception(
    mock_config832, mock_file_system_endpoint, transfer_controller_module
):
    SimpleTransferController = transfer_controller_module["SimpleTransferController"]
    with patch("os.system", side_effect=Exception("Mocked cp error")) as mock_os_system:
        controller = SimpleTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_file_system_endpoint,
            destination=mock_file_system_endpoint,
        )

        assert result is False, "Expected False when an exception is raised during copy."
        mock_os_system.assert_called_once()


# --------------------------------------------------------------------------
# Tests for CFSToHPSSTransferController
# --------------------------------------------------------------------------

def test_cfs_to_hpss_transfer_controller_success(mock_config832, transfer_controller_module, mocker: MockFixture):
    """
    Test a successful copy() operation using CFSToHPSSTransferController.
    We simulate a successful job submission and completion.
    """
    CFSToHPSSTransferController = transfer_controller_module["CFSToHPSSTransferController"]
    HPSSEndpoint = transfer_controller_module["HPSSEndpoint"]
    FileSystemEndpoint = transfer_controller_module["FileSystemEndpoint"]

    # Create mock endpoints for source (CFS) and destination (HPSS)
    source_endpoint = FileSystemEndpoint("mock_cfs_source", "/mock_cfs_source", "mock.uri")
    destination_endpoint = HPSSEndpoint("mock_hpss_dest", "/mock_hpss_dest", "mock.uri")

    # Create a fake job object that simulates successful completion.
    fake_job = MagicMock()
    fake_job.jobid = "12345"
    fake_job.state = "COMPLETED"
    fake_job.complete.return_value = None

    # Create a fake compute object that returns the fake job.
    fake_compute = MagicMock()
    fake_compute.submit_job.return_value = fake_job

    # Create a fake client whose compute() returns our fake_compute.
    fake_client = MagicMock()
    fake_client.compute.return_value = fake_compute

    controller = CFSToHPSSTransferController(fake_client, mock_config832)
    result = controller.copy(
        file_path="test_dir/test_file.txt",
        source=source_endpoint,
        destination=destination_endpoint,
    )
    assert result is True, "Expected True when CFSToHPSSTransferController transfer completes successfully."
    fake_compute.submit_job.assert_called_once()
    fake_job.complete.assert_called_once()


def test_cfs_to_hpss_transfer_controller_failure(mock_config832, transfer_controller_module):
    """
    Test a failing copy() operation using CFSToHPSSTransferController when job submission raises an exception.
    """
    CFSToHPSSTransferController = transfer_controller_module["CFSToHPSSTransferController"]
    HPSSEndpoint = transfer_controller_module["HPSSEndpoint"]
    FileSystemEndpoint = transfer_controller_module["FileSystemEndpoint"]

    source_endpoint = FileSystemEndpoint("mock_cfs_source", "/mock_cfs_source", "mock.uri")
    destination_endpoint = HPSSEndpoint("mock_hpss_dest", "/mock_hpss_dest", "mock.uri")

    # Create a fake client whose compute().submit_job raises an exception.
    fake_client = MagicMock()
    fake_compute = MagicMock()
    fake_compute.submit_job.side_effect = Exception("Job submission failed")
    fake_client.compute.return_value = fake_compute

    controller = CFSToHPSSTransferController(fake_client, mock_config832)
    result = controller.copy(
        file_path="test_dir/test_file.txt",
        source=source_endpoint,
        destination=destination_endpoint,
    )
    assert result is False, "Expected False when CFSToHPSSTransferController transfer fails due to job submission error."
    fake_compute.submit_job.assert_called_once()


# --------------------------------------------------------------------------
# Tests for HPSSToCFSTransferController
# --------------------------------------------------------------------------

def test_hpss_to_cfs_transfer_controller_success(mock_config832, transfer_controller_module, mocker: MockFixture):
    """
    Test a successful copy() operation using HPSSToCFSTransferController.
    We simulate a successful job submission and completion.
    """
    HPSSToCFSTransferController = transfer_controller_module["HPSSToCFSTransferController"]
    HPSSEndpoint = transfer_controller_module["HPSSEndpoint"]
    FileSystemEndpoint = transfer_controller_module["FileSystemEndpoint"]

    source_endpoint = HPSSEndpoint("mock_hpss_source", "/mock_hpss_source", "mock.uri")
    destination_endpoint = FileSystemEndpoint("mock_cfs_dest", "/mock_cfs_dest", "mock.uri")

    # Create a fake job object for a successful transfer.
    fake_job = MagicMock()
    fake_job.jobid = "67890"
    fake_job.state = "COMPLETED"
    fake_job.complete.return_value = None

    fake_compute = MagicMock()
    fake_compute.submit_job.return_value = fake_job

    fake_client = MagicMock()
    fake_client.compute.return_value = fake_compute

    controller = HPSSToCFSTransferController(fake_client, mock_config832)
    result = controller.copy(
        file_path="archive.tar",
        source=source_endpoint,
        destination=destination_endpoint,
        files_to_extract=["file1.txt", "file2.txt"]
    )
    assert result is True, "Expected True when HPSSToCFSTransferController transfer completes successfully."
    fake_compute.submit_job.assert_called_once()
    fake_job.complete.assert_called_once()


def test_hpss_to_cfs_transfer_controller_missing_params(mock_config832, transfer_controller_module):
    """
    Test that HPSSToCFSTransferController.copy() returns False when required parameters are missing.
    """
    HPSSToCFSTransferController = transfer_controller_module["HPSSToCFSTransferController"]
    fake_client = MagicMock()  # Client is not used because the method returns early.
    controller = HPSSToCFSTransferController(fake_client, mock_config832)

    result = controller.copy(file_path=None, source=None, destination=None)
    assert result is False, "Expected False when required parameters are missing."


def test_hpss_to_cfs_transfer_controller_job_failure(mock_config832, transfer_controller_module):
    """
    Test HPSSToCFSTransferController.transfer() returns False when job.complete() raises an exception.
    """
    HPSSToCFSTransferController = transfer_controller_module["HPSSToCFSTransferController"]
    HPSSEndpoint = transfer_controller_module["HPSSEndpoint"]
    FileSystemEndpoint = transfer_controller_module["FileSystemEndpoint"]

    source_endpoint = HPSSEndpoint("mock_hpss_source", "/mock_hpss_source", "mock.uri")
    destination_endpoint = FileSystemEndpoint("mock_cfs_dest", "/mock_cfs_dest", "mock.uri")

    fake_job = MagicMock()
    fake_job.jobid = "67891"
    fake_job.state = "FAILED"
    fake_job.complete.side_effect = Exception("Job completion failed")

    fake_compute = MagicMock()
    fake_compute.submit_job.return_value = fake_job

    fake_client = MagicMock()
    fake_client.compute.return_value = fake_compute

    controller = HPSSToCFSTransferController(fake_client, mock_config832)
    result = controller.copy(
        file_path="archive.tar",
        source=source_endpoint,
        destination=destination_endpoint,
    )
    assert result is False, "Expected False when HPSSToCFSTransferController job fails to complete."
    fake_compute.submit_job.assert_called_once()
    fake_job.complete.assert_called_once()


def test_hpss_to_cfs_transfer_controller_recovery(mock_config832, transfer_controller_module):
    """
    Test HPSSToCFSTransferController recovery scenario when initial job.complete() fails with 'Job not found:'.
    The controller should attempt to recover the job and complete successfully.
    """
    HPSSToCFSTransferController = transfer_controller_module["HPSSToCFSTransferController"]
    HPSSEndpoint = transfer_controller_module["HPSSEndpoint"]
    FileSystemEndpoint = transfer_controller_module["FileSystemEndpoint"]

    source_endpoint = HPSSEndpoint("mock_hpss_source", "/mock_hpss_source", "mock.uri")
    destination_endpoint = FileSystemEndpoint("mock_cfs_dest", "/mock_cfs_dest", "mock.uri")

    # Fake job that fails initially with a "Job not found:" error.
    fake_job_initial = MagicMock()
    fake_job_initial.jobid = "11111"
    fake_job_initial.state = "UNKNOWN"
    fake_job_initial.complete.side_effect = Exception("Job not found: 11111")

    fake_compute = MagicMock()
    fake_compute.submit_job.return_value = fake_job_initial

    # When recovery is attempted, return a job that completes successfully.
    fake_job_recovered = MagicMock()
    fake_job_recovered.jobid = "11111"
    fake_job_recovered.state = "COMPLETED"
    fake_job_recovered.complete.return_value = None

    fake_client = MagicMock()
    fake_client.compute.return_value = fake_compute
    fake_client.perlmutter.job.return_value = fake_job_recovered

    controller = HPSSToCFSTransferController(fake_client, mock_config832)
    result = controller.copy(
        file_path="archive.tar",
        source=source_endpoint,
        destination=destination_endpoint,
    )
    assert result is True, "Expected True after successful job recovery in HPSSToCFSTransferController."
    fake_compute.submit_job.assert_called_once()
    fake_job_initial.complete.assert_called_once()
    fake_client.perlmutter.job.assert_called_once_with(jobid="11111")
    fake_job_recovered.complete.assert_called_once()
