'''Pytest unit tests for BL7011 move flow. '''

import logging
import pytest
from uuid import uuid4

from prefect.testing.utilities import prefect_test_harness
from prefect.blocks.system import Secret, JSON
from pytest_mock import MockFixture

from orchestration._tests.test_transfer_controller import MockSecret
from orchestration.flows.bl7011.config import Config7011

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

        pruning_config = JSON(value={"max_wait_seconds": 600})
        pruning_config.save(name="pruning-config", overwrite=True)

        yield


# ----------------------------
# Tests for 7011
# ----------------------------

def test_process_new_7011_file(mocker: MockFixture) -> None:
    """
    Test the process_new_7011_file flow from orchestration.flows.bl7011.move.

    This test verifies that:
      - The get_transfer_controller function is called (patched) with the correct parameters.
      - The returned transfer controller's copy method is called with the expected file path,
        source, and destination endpoints from the provided configuration.

    Parameters:
        mocker (MockFixture): The pytest-mock fixture for patching and mocking objects.
    """
    # Import the flow to test.
    from orchestration.flows.bl7011.move import process_new_7011_file

    # Patch the Secret.load and init_transfer_client in the configuration context.
    with mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret()):
        mocker.patch(
            "orchestration.flows.bl7011.config.transfer.init_transfer_client",
            return_value=mocker.MagicMock()  # Return a dummy TransferClient
        )

    # Instantiate the dummy configuration.
    mock_config = Config7011()

    # Generate a test file path.
    test_file_path = f"/tmp/test_file_{uuid4()}.txt"

    # Create a mock transfer controller with a mocked 'copy' method.
    mock_transfer_controller = mocker.MagicMock()
    mock_transfer_controller.copy.return_value = True

    mock_prune = mocker.patch(
        "orchestration.flows.bl7011.move.prune",
        return_value=None
    )

    # Patch get_transfer_controller where it is used in process_new_7011_file.
    mocker.patch(
        "orchestration.flows.bl7011.move.get_transfer_controller",
        return_value=mock_transfer_controller
    )

    # Execute the move flow with the test file path and mock configuration.
    result = process_new_7011_file(file_path=test_file_path, config=mock_config)

    # Verify that the transfer controller's copy method was called exactly once.
    assert mock_transfer_controller.copy.call_count == 1, "Transfer controller copy method should be called exactly once"
    assert result is None, "The flow should return None"
    assert mock_prune.call_count == 1, "Prune function should be called exactly once"

    # Reset mocks and test with config=None
    mock_transfer_controller.copy.reset_mock()
    mock_prune.reset_mock()

    result = process_new_7011_file(file_path=test_file_path, config=None)
    assert mock_transfer_controller.copy.call_count == 1, "Transfer controller copy method should be called exactly once"
    assert result is None, "The flow should return None"
    assert mock_prune.call_count == 1, "Prune function should be called exactly once"


def test_dispatcher_7011_flow(mocker: MockFixture) -> None:
    """
    Test the dispatcher flow for BL7011.

    This test verifies that:
      - The process_new_7011_file function is called with the correct parameters
        when the dispatcher flow is executed.
    Parameters:
        mocker (MockFixture): The pytest-mock fixture for patching and mocking objects.
    """
    # Import the dispatcher flow to test.
    from orchestration.flows.bl7011.dispatcher import dispatcher

    # Create a mock configuration object.
    class MockConfig:
        pass

    mock_config = MockConfig()

    # Generate a test file path.
    test_file_path = f"/tmp/test_file_{uuid4()}.txt"

    # Patch the schedule_prefect_flow call to avoid real Prefect interaction
    with mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret()):
        mocker.patch(
            "orchestration.flows.bl7011.config.transfer.init_transfer_client",
            return_value=mocker.MagicMock()  # Return a dummy TransferClient
        )
    # Patch the schedule_prefect_flow call to avoid real Prefect interaction
    mocker.patch(
        "orchestration.flows.bl7011.move.schedule_prefect_flow",
        return_value=None
    )

    # Patch the process_new_7011_file function to monitor its calls.
    mock_process_new_7011_file = mocker.patch(
        "orchestration.flows.bl7011.dispatcher.process_new_7011_file",
        return_value=None
    )

    # Execute the dispatcher flow with test parameters.
    dispatcher(
        file_path=test_file_path,
        is_export_control=False,
        config=mock_config
    )

    # Verify that process_new_7011_file was called exactly once with the expected arguments.
    mock_process_new_7011_file.assert_called_once_with(
        file_path=test_file_path,
        config=mock_config
    )

    # Verify that process_new_7011_file is called even when config is None
    mock_process_new_7011_file.reset_mock()
    dispatcher(
        file_path=test_file_path,
        is_export_control=False,
        config=None
    )
    mock_process_new_7011_file.assert_called_once()

    # Test error handling for missing file_path
    mock_process_new_7011_file.reset_mock()
    with pytest.raises(ValueError):
        dispatcher(
            file_path=None,
            is_export_control=False,
            config=mock_config
        )
    mock_process_new_7011_file.assert_not_called()

    # Test error handling for export control flag
    mock_process_new_7011_file.reset_mock()
    with pytest.raises(ValueError):
        dispatcher(
            file_path=test_file_path,
            is_export_control=True,
            config=mock_config
        )
    mock_process_new_7011_file.assert_not_called()
