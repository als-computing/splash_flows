'''Pytest unit tests for Dichroism move flow. '''

import logging
import pytest
from uuid import uuid4

from prefect.testing.utilities import prefect_test_harness
from prefect.blocks.system import Secret
from prefect.variables import Variable
from pytest_mock import MockFixture

from orchestration._tests.test_transfer_controller import MockSecret

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
            name="dichroism-settings",
            value={
                "delete_data402_files_after_days": 180,
                "delete_data631_files_after_days": 180
            },
            overwrite=True,
            _sync=True
        )

        yield


# ----------------------------
# Tests for 402
# ----------------------------

def test_process_new_402_file_task(mocker: MockFixture) -> None:
    """
    Test the process_new_402_file flow from orchestration.flows.dichroism.move.

    This test verifies that:
      - The get_transfer_controller function is called (patched) with the correct parameters.
      - The returned transfer controller's copy method is called with the expected file path,
        source, and destination endpoints from the provided configuration.

    Parameters:
        mocker (MockFixture): The pytest-mock fixture for patching and mocking objects.
    """
    # Import the flow to test.
    from orchestration.flows.dichroism.move import process_new_402_file_task

    # Patch the Secret.load and init_transfer_client in the configuration context.
    with mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret()):
        mocker.patch(
            "orchestration.flows.dichroism.config.transfer.init_transfer_client",
            return_value=mocker.MagicMock()  # Return a dummy TransferClient
        )
    # Patch the schedule_prefect_flow call to avoid real Prefect interaction
    mocker.patch(
        "orchestration.flows.dichroism.move.schedule_prefect_flow",
        return_value=None
    )

    # Instantiate the mock configuration.
    from orchestration.flows.dichroism.config import ConfigDichroism
    mock_config = ConfigDichroism()

    # Generate a test file path.
    test_file_path = f"/tmp/test_file_{uuid4()}.txt"

    # Create a mock transfer controller with a mocked 'copy' method.
    mock_transfer_controller = mocker.MagicMock()
    mock_transfer_controller.copy.return_value = True

    mock_prune = mocker.patch(
        "orchestration.flows.dichroism.move.prune",
        return_value=None
    )

    # Patch get_transfer_controller where it is used in process_new_931_file_task.
    mocker.patch(
        "orchestration.flows.dichroism.move.get_transfer_controller",
        return_value=mock_transfer_controller
    )

    # Execute the move flow with the test file path and mock configuration.
    result = process_new_402_file_task(file_path=test_file_path, config=mock_config)

    # Verify that the transfer controller's copy method was called exactly once.
    assert mock_transfer_controller.copy.call_count == 1, "Transfer controller copy method should be called exactly once"
    assert result is None, "The flow should return None"
    assert mock_prune.call_count == 1, "Prune function should be called exactly once"

    # Reset mocks and test with config=None
    mock_transfer_controller.copy.reset_mock()
    mock_prune.reset_mock()

    result = process_new_402_file_task(file_path=test_file_path, config=None)
    assert mock_transfer_controller.copy.call_count == 1, "Transfer controller copy method should be called exactly once"
    assert result is None, "The flow should return None"
    assert mock_prune.call_count == 1, "Prune function should be called exactly once"


# ----------------------------
# Tests for 631
# ----------------------------

def test_process_new_631_file_task(mocker: MockFixture) -> None:
    """
    Test the process_new_631_file flow from orchestration.flows.dichroism.move.

    This test verifies that:
      - The get_transfer_controller function is called (patched) with the correct parameters.
      - The returned transfer controller's copy method is called with the expected file path,
        source, and destination endpoints from the provided configuration.

    Parameters:
        mocker (MockFixture): The pytest-mock fixture for patching and mocking objects.
    """
    # Import the flow to test.
    from orchestration.flows.dichroism.move import process_new_631_file_task

    # Patch the Secret.load and init_transfer_client in the configuration context.
    with mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret()):
        mocker.patch(
            "orchestration.flows.dichroism.config.transfer.init_transfer_client",
            return_value=mocker.MagicMock()  # Return a dummy TransferClient
        )
    # Patch the schedule_prefect_flow call to avoid real Prefect interaction
    mocker.patch(
        "orchestration.flows.dichroism.move.schedule_prefect_flow",
        return_value=None
    )

    # Instantiate the dummy configuration.
    from orchestration.flows.dichroism.config import ConfigDichroism
    mock_config = ConfigDichroism()

    # Generate a test file path.
    test_file_path = f"/tmp/test_file_{uuid4()}.txt"

    # Create a mock transfer controller with a mocked 'copy' method.
    mock_transfer_controller = mocker.MagicMock()
    mock_transfer_controller.copy.return_value = True

    mock_prune = mocker.patch(
        "orchestration.flows.dichroism.move.prune",
        return_value=None
    )

    # Patch get_transfer_controller where it is used in process_new_931_file_task.
    mocker.patch(
        "orchestration.flows.dichroism.move.get_transfer_controller",
        return_value=mock_transfer_controller
    )

    # Execute the move flow with the test file path and mock configuration.
    result = process_new_631_file_task(file_path=test_file_path, config=mock_config)

    # Verify that the transfer controller's copy method was called exactly once.
    assert mock_transfer_controller.copy.call_count == 1, "Transfer controller copy method should be called exactly once"
    assert result is None, "The flow should return None"
    assert mock_prune.call_count == 1, "Prune function should be called exactly once"

    # Reset mocks and test with config=None
    mock_transfer_controller.copy.reset_mock()
    mock_prune.reset_mock()

    result = process_new_631_file_task(file_path=test_file_path, config=None)
    assert mock_transfer_controller.copy.call_count == 1, "Transfer controller copy method should be called exactly once"
    assert result is None, "The flow should return None"
    assert mock_prune.call_count == 1, "Prune function should be called exactly once"


def test_dispatcher_dichroism_flow(mocker: MockFixture) -> None:
    """
    Test the dispatcher flow for Dichroism.

    This test verifies that:
      - The process_new_402_file_task or process_new_631_file_task functions are called with the correct parameters
        when the dispatcher flow is executed.
    Parameters:
        mocker (MockFixture): The pytest-mock fixture for patching and mocking objects.
    """
    # Import the dispatcher flow to test.
    from orchestration.flows.dichroism.dispatcher import dispatcher

    # Create a mock configuration object.
    class MockConfig:
        pass

    mock_config = MockConfig()

    # Generate a test file path.
    test_file = f"/tmp/test_file_{uuid4()}.txt"

    # -----------------------------
    # Common patches used by dispatcher
    # -----------------------------
    mocker.patch("prefect.blocks.system.Secret.load", return_value=MockSecret())
    mocker.patch(
        "orchestration.flows.dichroism.config.transfer.init_transfer_client",
        return_value=mocker.MagicMock()
    )
    mocker.patch(
        "orchestration.flows.dichroism.move.schedule_prefect_flow",
        return_value=None
    )

    # ---------------------------------
    # Patch BOTH processing tasks
    # ---------------------------------
    mock_402 = mocker.patch(
        "orchestration.flows.dichroism.dispatcher.process_new_402_file_task",
        return_value=None
    )
    mock_631 = mocker.patch(
        "orchestration.flows.dichroism.dispatcher.process_new_631_file_task",
        return_value=None
    )

    # ----------------------------------------------------------------------
    # 402 TEST
    # ----------------------------------------------------------------------
    dispatcher(
        file_path=test_file,
        is_export_control=False,
        config=mock_config,
        beamline="BL402"
    )

    mock_402.assert_called_once_with(file_path=test_file, config=mock_config)
    mock_631.assert_not_called()

    # Reset mocks to reuse
    mock_402.reset_mock()
    mock_631.reset_mock()

    # ----------------------------------------------------------------------
    # 402 TEST – config=None should still call 402
    # ----------------------------------------------------------------------
    dispatcher(
        file_path=test_file,
        is_export_control=False,
        config=None,
        beamline="BL402"
    )

    mock_402.assert_called_once()
    mock_631.assert_not_called()

    mock_402.reset_mock()
    mock_631.reset_mock()

    # ----------------------------------------------------------------------
    # 631 TEST
    # ----------------------------------------------------------------------
    dispatcher(
        file_path=test_file,
        is_export_control=False,
        config=mock_config,
        beamline="BL631"
    )

    mock_631.assert_called_once_with(file_path=test_file, config=mock_config)
    mock_402.assert_not_called()

    mock_402.reset_mock()
    mock_631.reset_mock()

    # ----------------------------------------------------------------------
    # 631 TEST – config=None
    # ----------------------------------------------------------------------
    dispatcher(
        file_path=test_file,
        is_export_control=False,
        config=None,
        beamline="BL631"
    )

    mock_631.assert_called_once()
    mock_402.assert_not_called()

    mock_402.reset_mock()
    mock_631.reset_mock()

    # ----------------------------------------------------------------------
    # Missing file_path → ValueError
    # ----------------------------------------------------------------------
    with pytest.raises(ValueError):
        dispatcher(
            file_path=None,
            is_export_control=False,
            config=mock_config,
            beamline="BL402"
        )

    mock_402.assert_not_called()
    mock_631.assert_not_called()

    # ----------------------------------------------------------------------
    # export control flag blocks execution
    # ----------------------------------------------------------------------
    with pytest.raises(ValueError):
        dispatcher(
            file_path=test_file,
            is_export_control=True,
            config=mock_config,
            beamline="BL402"
        )

    mock_402.assert_not_called()
    mock_631.assert_not_called()

    # ----------------------------------------------------------------------
    # Missing beamline enum → ValueError
    # ----------------------------------------------------------------------
    with pytest.raises(ValueError):
        dispatcher(
            file_path=test_file,
            is_export_control=False,
            config=mock_config,
            beamline=None
        )

    mock_402.assert_not_called()
    mock_631.assert_not_called()
