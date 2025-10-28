import asyncio
from uuid import uuid4
import warnings

from prefect.blocks.system import JSON, Secret
from prefect.testing.utilities import prefect_test_harness
import pytest
from pytest_mock import MockFixture

from .test_globus import MockTransferClient

warnings.filterwarnings("ignore", category=DeprecationWarning)


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
        globus_client_id.save(name="globus-client-id")

        globus_client_secret = Secret(value=str(uuid4()))
        globus_client_secret.save(name="globus-client-secret")

        globus_compute_endpoint = Secret(value=str(uuid4()))
        globus_compute_endpoint.save(name="globus-compute-endpoint")

        pruning_config = JSON(value={"max_wait_seconds": 600})
        pruning_config.save(name="pruning-config")

        decision_settings = JSON(value={
            "alcf_recon_flow/alcf_recon_flow": True,
            "nersc_recon_flow/nersc_recon_flow": False,
            "new_832_file_flow/new_file_832": False
        })
        decision_settings.save(name="decision-settings")

        alcf_allocation_root = JSON(value={"alcf-allocation-root-path": "/eagle/IRIProd/ALS"})
        alcf_allocation_root.save(name="alcf-allocation-root-path")

        yield


class MockEndpoint:
    def __init__(self, root_path, uuid_value=None):
        self.root_path = root_path
        self.uuid = uuid_value or str(uuid4())
        self.uri = f"mock_endpoint_uri_{self.uuid}"


# Mock the Client class to avoid real network calls
class MockGlobusComputeClient:
    def __init__(self, *args, **kwargs):
        # No real initialization, as this is a mock
        pass

    def version_check(self):
        # Mock version check to do nothing
        pass

    def run(self, *args, **kwargs):
        # Return a mock task ID
        return "mock_task_id"

    def get_task(self, task_id):
        # Return a mock task response
        return {
            "pending": False,
            "status": "success",
            "result": "mock_result"
        }

    def get_result(self, task_id):
        # Return a mock result
        return "mock_result"


class MockSecret:
    value = str(uuid4())


# ----------------------------
# Tests for 7011
# ----------------------------

class MockConfig7011:
    def __init__(self) -> None:
        """
        Dummy configuration for 7011 flows.
        """
        # Create mock endpoints
        self.endpoints = {
            "data7011_raw": MockEndpoint(root_path="mock_data7011_raw_path", uuid_value=str(uuid4())),
            "nersc7011_alsdev_raw": MockEndpoint(root_path="mock_nersc7011_alsdev_raw_path", uuid_value=str(uuid4())),
        }

        # Define mock apps
        self.apps = {
            "als_transfer": "mock_als_transfer_app"
        }

        # Use the mock transfer client instead of the real TransferClient
        self.tc = MockTransferClient()

        # Set attributes for easy access
        self.data7011_raw = self.endpoints["data7011_raw"]
        self.nersc7011_alsdev_raw = self.endpoints["nersc7011_alsdev_raw"]


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
        from orchestration.flows.bl7011.config import Config7011

    # Instantiate the dummy configuration.
    mock_config = Config7011()

    # Generate a test file path.
    test_file_path = f"/tmp/test_file_{uuid4()}.txt"

    # Create a mock transfer controller with a mocked 'copy' method.
    mock_transfer_controller = mocker.MagicMock()
    mock_transfer_controller.copy.return_value = True

    # Patch get_transfer_controller where it is used in process_new_733_file.
    mocker.patch(
        "orchestration.flows.bl7011.move.get_transfer_controller",
        return_value=mock_transfer_controller
    )

    # Execute the flow with the test file path and dummy configuration.
    result = process_new_7011_file(file_path=test_file_path, config=mock_config)

    # Verify that the transfer controller's copy method was called exactly once.
    assert mock_transfer_controller.copy.call_count == 1, "Transfer controller copy method should be called exactly once"
    assert result is None, "The flow should return None"


# ----------------------------
# Tests for 832
# ----------------------------


class MockConfig832():
    def __init__(self) -> None:
        # Mock configuration
        config = {
            "scicat": "mock_scicat_value"
        }

        # Mock endpoints with UUIDs
        self.endpoints = {
            "spot832": MockEndpoint(root_path="mock_spot832_path", uuid_value=str(uuid4())),
            "data832": MockEndpoint(root_path="mock_data832_path", uuid_value=str(uuid4())),
            "nersc832": MockEndpoint(root_path="mock_nersc832_path", uuid_value=str(uuid4())),
            "data832_raw": MockEndpoint(root_path="mock_data832_raw_path", uuid_value=str(uuid4())),
            "data832_scratch": MockEndpoint(root_path="mock_data832_scratch_path", uuid_value=str(uuid4())),
            "nersc_alsdev": MockEndpoint(root_path="mock_nersc_alsdev_path", uuid_value=str(uuid4())),
            "nersc832_alsdev_raw": MockEndpoint(root_path="mock_nersc832_alsdev_raw_path",
                                                uuid_value=str(uuid4())),
            "nersc832_alsdev_scratch": MockEndpoint(root_path="mock_nersc832_alsdev_scratch_path",
                                                    uuid_value=str(uuid4())),
            "alcf832_raw": MockEndpoint(root_path="mock_alcf832_raw_path", uuid_value=str(uuid4())),
            "alcf832_scratch": MockEndpoint(root_path="mock_alcf832_scratch_path", uuid_value=str(uuid4())),
        }

        # Mock apps
        self.apps = {
            "als_transfer": "mock_als_transfer_app"
        }

        # Use the MockTransferClient instead of the real TransferClient
        self.tc = MockTransferClient()

        # Set attributes directly on the object
        self.spot832 = self.endpoints["spot832"]
        self.data832 = self.endpoints["data832"]
        self.nersc832 = self.endpoints["nersc832"]
        self.alcf832_raw = self.endpoints["alcf832_raw"]
        self.alcf832_scratch = self.endpoints["alcf832_scratch"]
        self.data832_raw = self.endpoints["data832_raw"]
        self.data832_scratch = self.endpoints["data832_scratch"]
        self.nersc832_alsdev_scratch = self.endpoints["nersc832_alsdev_scratch"]
        self.scicat = config["scicat"]


def test_832_dispatcher(mocker: MockFixture):
    """Test 832 uber decision flow."""

    # Mock the Secret block load using a simple manual mock class

    mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret())

    # Import decision flow after mocking the necessary components
    from orchestration.flows.bl832.dispatcher import dispatcher

    # Mock read_deployment_by_name with a manually defined mock class
    class MockDeployment:
        version = "1.0.0"
        flow_id = str(uuid4())
        name = "test_deployment"

    mocker.patch('prefect.client.orchestration.PrefectClient.read_deployment_by_name',
                 return_value=MockDeployment())

    # Mock run_deployment to avoid executing any Prefect workflows
    async def mock_run_deployment(*args, **kwargs):
        return None

    mocker.patch('prefect.deployments.deployments.run_deployment', new=mock_run_deployment)

    # Mock asyncio.gather to avoid actual async task execution
    async def mock_gather(*args, **kwargs):
        return [None]

    mocker.patch('asyncio.gather', new=mock_gather)

    # Run the decision flow
    result = asyncio.run(dispatcher(
        file_path="/global/raw/transfer_tests/test.txt",
        is_export_control=False,
        config=MockConfig832()
    ))

    # Ensure the flow runs without throwing an error
    assert result is None, "The decision flow did not complete successfully."


def test_alcf_recon_flow(mocker: MockFixture):
    """
    Test the alcf_recon_flow in one function, covering:
      Case 1) All steps succeed => returns True
      Case 2) HPC reconstruction fails => raises ValueError("Reconstruction at ALCF Failed")
      Case 3) Tiff->Zarr fails => raises ValueError("Tiff to Zarr at ALCF Failed")
      Case 4) data832->ALCF transfer fails => raises ValueError("Transfer to ALCF Failed")
    """

    with mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret()):
        # 2) Patch out the calls in Config832 that do real Globus auth:
        #    a) init_transfer_client(...) used in the constructor
        mocker.patch(
            "orchestration.flows.bl832.config.transfer.init_transfer_client",
            return_value=mocker.MagicMock()  # pretend TransferClient
        )
        #    b) flows.get_flows_client(...) used in the constructor
        mocker.patch(
            "orchestration.flows.bl832.config.flows.get_flows_client",
            return_value=mocker.MagicMock()  # pretend FlowsClient
        )

        # 3) Now import the real code AFTER these patches
        from orchestration.flows.bl832.alcf import alcf_recon_flow, ALCFTomographyHPCController
        from orchestration.flows.bl832.config import Config832

    # 4) Create a config => won't do real Globus calls now
    mock_config = Config832()

    # 5) Patch HPC calls on ALCFTomographyHPCController
    mock_hpc_reconstruct = mocker.patch.object(
        ALCFTomographyHPCController, "reconstruct", return_value=True
    )
    mock_hpc_multires = mocker.patch.object(
        ALCFTomographyHPCController, "build_multi_resolution", return_value=True
    )

    # 6) Patch get_transfer_controller(...) => returns a mock
    mock_transfer_controller = mocker.MagicMock()
    mock_transfer_controller.copy.return_value = True
    mocker.patch(
        "orchestration.flows.bl832.alcf.get_transfer_controller",
        return_value=mock_transfer_controller
    )

    # 7) Patch schedule_pruning => skip real scheduling
    mock_schedule_pruning = mocker.patch(
        "orchestration.flows.bl832.alcf.schedule_pruning",
        return_value=True
    )

    file_path = "/global/raw/transfer_tests/test.h5"

    # ---------- CASE 1: SUCCESS PATH ----------
    mock_transfer_controller.copy.return_value = True
    mock_hpc_reconstruct.return_value = True
    mock_hpc_multires.return_value = True

    result = alcf_recon_flow(file_path=file_path, config=mock_config)
    assert result is True, "Flow should return True if HPC + Tiff->Zarr + transfers all succeed"
    assert mock_transfer_controller.copy.call_count == 3, "Should do 3 transfers in success path"
    mock_hpc_reconstruct.assert_called_once()
    mock_hpc_multires.assert_called_once()
    mock_schedule_pruning.assert_called_once()

    # Reset for next scenario
    mock_transfer_controller.copy.reset_mock()
    mock_hpc_reconstruct.reset_mock()
    mock_hpc_multires.reset_mock()
    mock_schedule_pruning.reset_mock()

    #
    # ---------- CASE 2: HPC reconstruction fails ----------
    #
    mock_transfer_controller.copy.return_value = True
    mock_hpc_reconstruct.return_value = False
    mock_hpc_multires.return_value = True

    with pytest.raises(ValueError, match="Reconstruction at ALCF Failed"):
        alcf_recon_flow(file_path=file_path, config=mock_config)

    mock_hpc_reconstruct.assert_called_once()
    mock_hpc_multires.assert_not_called()
    assert mock_transfer_controller.copy.call_count == 1, (
        "Should only do the first data832->alcf copy before HPC fails"
    )
    mock_schedule_pruning.assert_not_called()

    # Reset
    mock_transfer_controller.copy.reset_mock()
    mock_hpc_reconstruct.reset_mock()
    mock_hpc_multires.reset_mock()
    mock_schedule_pruning.reset_mock()

    # ---------- CASE 3: Tiff->Zarr fails ----------
    mock_transfer_controller.copy.return_value = True
    mock_hpc_reconstruct.return_value = True
    mock_hpc_multires.return_value = False

    with pytest.raises(ValueError, match="Tiff to Zarr at ALCF Failed"):
        alcf_recon_flow(file_path=file_path, config=mock_config)

    mock_hpc_reconstruct.assert_called_once()
    mock_hpc_multires.assert_called_once()
    # HPC is done, so there's 1 successful transfer (data832->alcf).
    # We have not transferred tiff or zarr => total 1 copy
    assert mock_transfer_controller.copy.call_count == 1
    mock_schedule_pruning.assert_not_called()

    # Reset
    mock_transfer_controller.copy.reset_mock()
    mock_hpc_reconstruct.reset_mock()
    mock_hpc_multires.reset_mock()
    mock_schedule_pruning.reset_mock()

    # ---------- CASE 4: data832->ALCF fails immediately ----------
    mock_transfer_controller.copy.return_value = False
    mock_hpc_reconstruct.return_value = True
    mock_hpc_multires.return_value = True

    with pytest.raises(ValueError, match="Transfer to ALCF Failed"):
        alcf_recon_flow(file_path=file_path, config=mock_config)

    mock_hpc_reconstruct.assert_not_called()
    mock_hpc_multires.assert_not_called()
    # The only call is the failing copy
    mock_transfer_controller.copy.assert_called_once()
    mock_schedule_pruning.assert_not_called()
