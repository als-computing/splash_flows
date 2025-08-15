# orchestration/_tests/test_sfapi_flow.py

from pathlib import Path
import pytest
from unittest.mock import MagicMock, patch, mock_open
from uuid import uuid4

from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness


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

        yield


# ----------------------------
# Tests for create_sfapi_client
# ----------------------------


def test_create_sfapi_client_success():
    """
    Test successful creation of the SFAPI client.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    # Mock data for client_id and client_secret files
    mock_client_id = 'value'
    mock_client_secret = '{"key": "value"}'

    # Create separate mock_open instances for each file
    mock_open_client_id = mock_open(read_data=mock_client_id)
    mock_open_client_secret = mock_open(read_data=mock_client_secret)

    with patch("orchestration.flows.bl832.nersc.os.getenv") as mock_getenv, \
         patch("orchestration.flows.bl832.nersc.os.path.isfile") as mock_isfile, \
         patch("builtins.open", side_effect=[
             mock_open_client_id.return_value,
             mock_open_client_secret.return_value
         ]), \
            patch("orchestration.flows.bl832.nersc.JsonWebKey.import_key") as mock_import_key, \
            patch("orchestration.flows.bl832.nersc.Client") as MockClient:

        # Mock environment variables
        mock_getenv.side_effect = lambda x: {
            "PATH_NERSC_CLIENT_ID": "/path/to/client_id",
            "PATH_NERSC_PRI_KEY": "/path/to/client_secret"
        }.get(x, None)

        # Mock file existence
        mock_isfile.return_value = True

        # Mock JsonWebKey.import_key to return a mock secret
        mock_import_key.return_value = "mock_secret"

        # Create the client
        client = NERSCTomographyHPCController.create_sfapi_client()

        # Assert that Client was instantiated with 'value' and 'mock_secret'
        MockClient.assert_called_once_with("value", "mock_secret")

        # Assert that the returned client is the mocked client
        assert client == MockClient.return_value, "Client should be the mocked sfapi_client.Client instance"


def test_create_sfapi_client_missing_paths():
    """
    Test creation of the SFAPI client with missing credential paths.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    with patch("orchestration.flows.bl832.nersc.os.getenv", return_value=None):
        with pytest.raises(ValueError, match="Missing NERSC credentials paths."):
            NERSCTomographyHPCController.create_sfapi_client()


def test_create_sfapi_client_missing_files():
    """
    Test creation of the SFAPI client with missing credential files.
    """
    with (
        # Mock environment variables
        patch(
            "orchestration.flows.bl832.nersc.os.getenv",
            side_effect=lambda x: {
                "PATH_NERSC_CLIENT_ID": "/path/to/client_id",
                "PATH_NERSC_PRI_KEY": "/path/to/client_secret"
            }.get(x, None)
        ),

        # Mock file existence to simulate missing files
        patch("orchestration.flows.bl832.nersc.os.path.isfile", return_value=False)
    ):
        # Import the module after applying patches to ensure mocks are in place
        from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

        # Expect a FileNotFoundError due to missing credential files
        with pytest.raises(FileNotFoundError, match="NERSC credential files are missing."):
            NERSCTomographyHPCController.create_sfapi_client()

# ----------------------------
# Fixture for Mocking SFAPI Client
# ----------------------------


@pytest.fixture
def mock_sfapi_client():
    """
    Mock the sfapi_client.Client class with necessary methods.
    """
    with patch("orchestration.flows.bl832.nersc.Client") as MockClient:
        mock_client_instance = MockClient.return_value

        # Mock the user method
        mock_user = MagicMock()
        mock_user.name = "testuser"
        mock_client_instance.user.return_value = mock_user

        # Mock the compute method to return a mocked compute object
        mock_compute = MagicMock()
        mock_job = MagicMock()
        mock_job.jobid = "12345"
        mock_job.state = "COMPLETED"
        mock_compute.submit_job.return_value = mock_job
        mock_client_instance.compute.return_value = mock_compute

        yield mock_client_instance


# ----------------------------
# Fixture for Mocking Config832
# ----------------------------

@pytest.fixture
def mock_config832():
    """
    Mock the Config832 class to provide necessary configurations.
    """
    with patch("orchestration.flows.bl832.nersc.Config832") as MockConfig:
        mock_config = MockConfig.return_value
        mock_config.harbor_images832 = {
            "recon_image": "mock_recon_image",
            "multires_image": "mock_multires_image",
        }
        mock_config.apps = {"als_transfer": "some_config"}
        yield mock_config


# ----------------------------
# Tests for NERSCTomographyHPCController
# ----------------------------

def test_reconstruct_success(mock_sfapi_client, mock_config832):
    """
    Test successful reconstruction job submission.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
    from sfapi_client.compute import Machine

    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)
    file_path = "path/to/file.h5"

    with patch("orchestration.flows.bl832.nersc.time.sleep", return_value=None):
        result = controller.reconstruct(file_path=file_path)

    # Verify that compute was called with Machine.perlmutter
    mock_sfapi_client.compute.assert_called_once_with(Machine.perlmutter)

    # Verify that submit_job was called once
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()

    # Verify that complete was called on the job
    mock_sfapi_client.compute.return_value.submit_job.return_value.complete.assert_called_once()

    # Assert that the method returns True
    assert result is True, "reconstruct should return True on successful job completion."


def test_reconstruct_submission_failure(mock_sfapi_client, mock_config832):
    """
    Test reconstruction job submission failure.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)
    file_path = "path/to/file.h5"

    # Simulate submission failure
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("Submission failed")

    with patch("orchestration.flows.bl832.nersc.time.sleep", return_value=None):
        result = controller.reconstruct(file_path=file_path)

    # Assert that the method returns False
    assert result is False, "reconstruct should return False on submission failure."


def test_build_multi_resolution_success(mock_sfapi_client, mock_config832):
    """
    Test successful multi-resolution job submission.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
    from sfapi_client.compute import Machine

    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)
    file_path = "path/to/file.h5"

    with patch("orchestration.flows.bl832.nersc.time.sleep", return_value=None):
        result = controller.build_multi_resolution(file_path=file_path)

    # Verify that compute was called with Machine.perlmutter
    mock_sfapi_client.compute.assert_called_once_with(Machine.perlmutter)

    # Verify that submit_job was called once
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()

    # Verify that complete was called on the job
    mock_sfapi_client.compute.return_value.submit_job.return_value.complete.assert_called_once()

    # Assert that the method returns True
    assert result is True, "build_multi_resolution should return True on successful job completion."


def test_build_multi_resolution_submission_failure(mock_sfapi_client, mock_config832):
    """
    Test multi-resolution job submission failure.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)
    file_path = "path/to/file.h5"

    # Simulate submission failure
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("Submission failed")

    with patch("orchestration.flows.bl832.nersc.time.sleep", return_value=None):
        result = controller.build_multi_resolution(file_path=file_path)

    # Assert that the method returns False
    assert result is False, "build_multi_resolution should return False on submission failure."


def test_job_submission(mock_sfapi_client):
    """
    Test job submission and status updates.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
    from sfapi_client.compute import Machine

    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=MagicMock())
    file_path = "path/to/file.h5"

    # Mock Path to extract file and folder names
    with patch.object(Path, 'parent', new_callable=MagicMock) as mock_parent, \
         patch.object(Path, 'stem', new_callable=MagicMock) as mock_stem:
        mock_parent.name = "to"
        mock_stem.return_value = "file"

        with patch("orchestration.flows.bl832.nersc.time.sleep", return_value=None):
            controller.reconstruct(file_path=file_path)

    # Verify that compute was called with Machine.perlmutter
    mock_sfapi_client.compute.assert_called_once_with(Machine.perlmutter)

    # Verify that submit_job was called once
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()

    # Verify the returned job has the expected attributes
    submitted_job = mock_sfapi_client.compute.return_value.submit_job.return_value
    assert submitted_job.jobid == "12345", "Job ID should match the mock job ID."
    assert submitted_job.state == "COMPLETED", "Job state should be COMPLETED."
