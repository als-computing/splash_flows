'''Pytest unit tests for BL733 move flow. '''

import logging
import pytest
from uuid import uuid4

from prefect.testing.utilities import prefect_test_harness
from prefect.blocks.system import Secret
from prefect.variables import Variable
from pytest_mock import MockFixture

from orchestration._tests.test_transfer_controller import MockSecret
from orchestration.flows.bl733.config import Config733

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        globus_client_id = Secret(value=str(uuid4()))
        globus_client_id.save(name="globus-client-id", overwrite=True)

        globus_client_secret = Secret(value=str(uuid4()))
        globus_client_secret.save(name="globus-client-secret", overwrite=True)

        Variable.set(
            name="globus-settings",
            value={"max_wait_seconds": 600},
            overwrite=True,
            _sync=True
        )

        Variable.set(
            name="bl733-settings",
            value={
                "delete_data733_files_after_days": 180
            },
            overwrite=True,
            _sync=True
        )

        yield


# ----------------------------
# Tests for 733
# ----------------------------

def test_process_new_733_file_task(mocker: MockFixture) -> None:
    """
    Test the process_new_733_file_task flow from orchestration.flows.bl733.move.

    This test verifies that:
      - The get_transfer_controller function is called (patched) with the correct parameters.
      - The returned transfer controller's copy method is called with the expected file path,
        source, and destination endpoints from the provided configuration.

    Parameters:
        mocker (MockFixture): The pytest-mock fixture for patching and mocking objects.
    """
    # Patch the Secret.load and init_transfer_client in the configuration context.
    mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret())
    # Patch ALL three transfer functions BEFORE importing/instantiating Config733
    mocker.patch(
        "orchestration.flows.bl733.config.transfer.build_endpoints",
        return_value={
            "bl733-als-data733": mocker.MagicMock(root_path="/mock/data733"),
            "bl733-als-data733_raw": mocker.MagicMock(root_path="/mock/data733_raw"),
            "bl733-nersc-alsdev_raw": mocker.MagicMock(root_path="/mock/nersc_raw"),
            "bl733-lamarr-beamlines": mocker.MagicMock(root_path="/mock/lamarr"),
        }
    )
    mocker.patch(
        "orchestration.flows.bl733.config.transfer.build_apps",
        return_value={"als_transfer": "mock_app"}
    )

    mocker.patch(
        "orchestration.flows.bl733.config.transfer.init_transfer_client",
        return_value=mocker.MagicMock()  # Return a mock TransferClient
    )

    from orchestration.flows.bl733.move import process_new_733_file_task
    # Instantiate the mock configuration.
    mock_config = Config733()

    # Generate a test file path.
    test_file_path = f"/tmp/test_file_{uuid4()}.txt"

    # Create a mock transfer controller with a mocked 'copy' method.
    mock_transfer_controller = mocker.MagicMock()
    mock_transfer_controller.copy.return_value = True

    mock_prune_controller = mocker.MagicMock()
    mocker.patch(
        "orchestration.flows.bl733.move.get_prune_controller",
        return_value=mock_prune_controller
    )
    # Patch get_transfer_controller where it is used in process_new_733_file_task.
    mocker.patch(
        "orchestration.flows.bl733.move.get_transfer_controller",
        return_value=mock_transfer_controller
    )

    # Execute the move flow with the test file path and mock configuration.
    result = process_new_733_file_task(file_path=test_file_path, config=mock_config)

    # Verify that the transfer controller's copy method was called exactly twice.
    assert mock_transfer_controller.copy.call_count == 2, "Transfer controller copy method should be called exactly twice"
    assert result is None, "The flow should return None"
    assert mock_prune_controller.prune.call_count == 1, "Prune function should be called exactly once"

    # Reset mocks and test with config=None
    mock_transfer_controller.copy.reset_mock()
    mock_prune_controller.prune.reset_mock()
    result = process_new_733_file_task(file_path=test_file_path, config=None)
    assert mock_transfer_controller.copy.call_count == 2, "Transfer controller copy method should be called exactly twice"
    assert result is None, "The flow should return None"
    assert mock_prune_controller.prune.call_count == 1, "Prune function should be called exactly once"


def test_dispatcher_733_flow(mocker: MockFixture) -> None:
    """
    Test the dispatcher flow for BL733.

    This test verifies that:
      - The process_new_733_file_task function is called with the correct parameters
        when the dispatcher flow is executed.
    Parameters:
        mocker (MockFixture): The pytest-mock fixture for patching and mocking objects.
    """
    # Import the dispatcher flow to test.
    from orchestration.flows.bl733.dispatcher import dispatcher

    # Create a mock configuration object.
    class MockConfig:
        pass

    mock_config = MockConfig()

    # Generate a test file path.
    test_file_path = f"/tmp/test_file_{uuid4()}.txt"

    # Patch the schedule_prefect_flow call to avoid real Prefect interaction
    mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret())

    # Patch ALL three transfer functions BEFORE importing Config733
    mocker.patch(
        "orchestration.flows.bl733.config.transfer.build_endpoints",
        return_value={
            "bl733-als-data733": mocker.MagicMock(root_path="/mock/data733"),
            "bl733-als-data733_raw": mocker.MagicMock(root_path="/mock/data733_raw"),
            "bl733-nersc-alsdev_raw": mocker.MagicMock(root_path="/mock/nersc_raw"),
            "bl733-lamarr-beamlines": mocker.MagicMock(root_path="/mock/lamarr"),
        }
    )
    mocker.patch(
        "orchestration.flows.bl733.config.transfer.build_apps",
        return_value={"als_transfer": "mock_app"}
    )

    mocker.patch(
        "orchestration.flows.bl733.config.transfer.init_transfer_client",
        return_value=mocker.MagicMock()  # Return a mock TransferClient
    )

    # Patch the process_new_733_file_task function to monitor its calls.
    mock_process_new_733_file_task = mocker.patch(
        "orchestration.flows.bl733.dispatcher.process_new_733_file_task",
        return_value=None
    )

    # Execute the dispatcher flow with test parameters.
    dispatcher(
        file_path=test_file_path,
        is_export_control=False,
        config=mock_config
    )

    # Verify that process_new_733_file_task was called exactly once with the expected arguments.
    mock_process_new_733_file_task.assert_called_once_with(
        file_path=test_file_path,
        config=mock_config
    )

    # Verify that process_new_733_file_task_task is called even when config is None
    mock_process_new_733_file_task.reset_mock()
    dispatcher(
        file_path=test_file_path,
        is_export_control=False,
        config=None
    )
    mock_process_new_733_file_task.assert_called_once()

    # Test error handling for missing file_path
    mock_process_new_733_file_task.reset_mock()
    with pytest.raises(ValueError):
        dispatcher(
            file_path=None,
            is_export_control=False,
            config=mock_config
        )
    mock_process_new_733_file_task.assert_not_called()

    # Test error handling for export control flag
    mock_process_new_733_file_task.reset_mock()
    with pytest.raises(ValueError):
        dispatcher(
            file_path=test_file_path,
            is_export_control=True,
            config=mock_config
        )
    mock_process_new_733_file_task.assert_not_called()
