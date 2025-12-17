import asyncio
# import uuid
from uuid import UUID, uuid4, uuid5
import warnings

from prefect.blocks.system import Secret
from prefect.variables import Variable
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
        globus_client_id.save(name="globus-client-id", overwrite=True)

        globus_client_secret = Secret(value=str(uuid4()))
        globus_client_secret.save(name="globus-client-secret", overwrite=True)

        globus_compute_endpoint = Secret(value=str(uuid4()))
        globus_compute_endpoint.save(name="globus-compute-endpoint", overwrite=True)

        Variable.set(
            name="pruning-config",
            value={"max_wait_seconds": 600},
            overwrite=True,
            _sync=True
        )

        Variable.set(
            name="decision-settings",
            value={
                "alcf_recon_flow/alcf_recon_flow": True,
                "nersc_recon_flow/nersc_recon_flow": True,
                "new_832_file_flow/new_file_832": True
            },
            overwrite=True,
            _sync=True
        )

        Variable.set(
            name="alcf-allocation-root-path",
            value={"alcf-allocation-root-path": "/eagle/IRIProd/ALS"},
            overwrite=True,
            _sync=True
        )

        Variable.set(
            name="bl832-settings",
            value={
                "delete_spot832_files_after_days": 1,
                "delete_data832_files_after_days": 35
            },
            overwrite=True,
            _sync=True
        )

        yield


class MockEndpoint:
    def __init__(self, root_path, uuid_value=None, name=None):
        self.root_path = root_path
        self.uuid = uuid_value or str(uuid4())
        self.uri = f"mock_endpoint_uri_{self.uuid}"
        self.name = name or f"mock_endpoint_{self.uuid[:8]}"


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
    """
    Deterministic secret for tests.
    """
    value = "550e8400-e29b-41d4-a716-446655440000"

    @staticmethod
    def for_endpoint(endpoint_name: str) -> str:
        """
        Generate a deterministic UUID string based on endpoint name.
        This ensures each endpoint is unique but stable across test runs.
        """
        namespace = UUID("12345678-1234-5678-1234-123456789012")
        return str(uuid5(namespace, endpoint_name))


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
            "spot832": MockEndpoint("mock_spot832_path", MockSecret.for_endpoint("spot832")),
            "data832": MockEndpoint("mock_data832_path", MockSecret.for_endpoint("data832")),
            "nersc832": MockEndpoint("mock_nersc832_path", MockSecret.for_endpoint("nersc832")),
            "data832_raw": MockEndpoint("mock_data832_raw_path", MockSecret.for_endpoint("data832_raw")),
            "data832_scratch": MockEndpoint("mock_data832_scratch_path", MockSecret.for_endpoint("data832_scratch")),
            "nersc_alsdev": MockEndpoint("mock_nersc_alsdev_path", MockSecret.for_endpoint("nersc_alsdev")),
            "nersc832_alsdev_raw": MockEndpoint("mock_nersc832_alsdev_raw_path",
                                                MockSecret.for_endpoint("nersc832_alsdev_raw")),
            "nersc832_alsdev_scratch": MockEndpoint("mock_nersc832_alsdev_scratch_path",
                                                    MockSecret.for_endpoint("nersc832_alsdev_scratch")),
            "alcf832_raw": MockEndpoint("mock_alcf832_raw_path", MockSecret.for_endpoint("alcf832_raw")),
            "alcf832_scratch": MockEndpoint("mock_alcf832_scratch_path", MockSecret.for_endpoint("alcf832_scratch")),
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

    mocker.patch('orchestration.flows.bl832.move.schedule_prefect_flow', return_value=None)

    # Mock read_deployment_by_name with a manually defined mock class
    class MockDeployment:
        def __init__(self):
            self.id = str(uuid4())  # Add this line
            self.version = "1.0.0"
            self.flow_id = str(uuid4())
            self.name = "test_deployment"

    mocker.patch('prefect.client.orchestration.PrefectClient.read_deployment_by_name',
                 return_value=MockDeployment())

    # Mock run_deployment to avoid executing any Prefect workflows
    async def mock_run_deployment(*args, **kwargs):
        return None

    mocker.patch('prefect.deployments.run_deployment', new=mock_run_deployment)

    # Mock asyncio.gather to avoid actual async task execution
    async def mock_gather(*args, **kwargs):
        return [None]

    mocker.patch('asyncio.gather', new=mock_gather)

    # Import decision flow after mocking the necessary components
    from orchestration.flows.bl832.dispatcher import dispatcher

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

    # 1) Patch Secret.load(...) so HPC calls won't blow up from malformed UUID
    mock_secret = mocker.MagicMock()
    mock_secret.get.return_value = str(uuid4())

    mocker.patch('prefect.blocks.system.Secret.load', return_value=mock_secret)
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
