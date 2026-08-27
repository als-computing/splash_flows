# orchestration/_tests/test_bl832/test_nersc.py
import pytest
from uuid import uuid4

from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness


# ──────────────────────────────────────────────────────────────────────────────
# Session fixture
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """Set up Prefect test harness and required secrets for the full session."""
    with prefect_test_harness():
        Secret(value=str(uuid4())).save(name="globus-client-id", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-client-secret", overwrite=True)
        yield


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_sfapi_client(mocker):
    """sfapi_client.Client mock with user, compute, submit_job, and job chained."""
    client = mocker.MagicMock()

    mock_user = mocker.MagicMock()
    mock_user.name = "testuser"
    client.user.return_value = mock_user

    mock_job = mocker.MagicMock()
    mock_job.jobid = "12345"
    mock_job.state = "COMPLETED"

    mock_compute = mocker.MagicMock()
    mock_compute.submit_job.return_value = mock_job
    client.compute.return_value = mock_compute
    mocker.patch("orchestration.flows.bl832.nersc.Client", return_value=client)
    return client


@pytest.fixture
def mock_config832(mocker):
    """
    Mock Config832 constructor so any call to Config832() returns our mock.

    Tests that call flows must pass config=None so Prefect's type validation
    is never given a MagicMock — the flow will call Config832() internally and
    get our mock back.

    All settings dicts must be fully populated to match the config YAML schema,
    because _load_job_options() passes config_settings directly as the defaults
    dict and then accesses keys by name.
    """
    mock_config = mocker.MagicMock()

    mock_config.ghcr_images832 = {
        "recon_image": "mock_recon_image",
        "multires_image": "mock_multires_image",
    }

    for attr in [
        "nersc832_alsdev_raw",
        "nersc832_alsdev_scratch",
        "nersc832_alsdev_recon_scripts",
        "nersc832_alsdev_pscratch_scratch",
        "nersc832_alsdev_pscratch_raw",
        "data832_scratch",
    ]:
        ep = mocker.MagicMock()
        ep.root_path = f"/mock/{attr}"
        setattr(mock_config, attr, ep)

    mock_config.nersc_resources = {
        "iri": {
            "api_base_url": "https://mock-iri.nersc.gov",
            "perlmutter_compute": "mock-perlmutter-compute-uuid",
            "perlmutter_login": "mock-perlmutter-login-uuid",
            "perlmutter_realtime": "mock-perlmutter-realtime-uuid",
            "perlmutter_job_submit": "mock-perlmutter-job-submit-uuid",
            "compute_resource": "compute",
            "scratch": "mock-scratch-uuid",
            "homes": "mock-homes-uuid",
            "common": "mock-common-uuid",
            "cfs": "mock-cfs-uuid",
            "archive": "mock-archive-uuid",
            "globus": "mock-globus-uuid",
            "dtns": "mock-dtns-uuid",
        },
        "sfapi": {
            "api_base_url": "https://mock-sfapi.nersc.gov",
        },
    }

    mock_config.nersc_recon_settings = {
        "qos": "realtime",
        "account": "mock_account",
        "reservation": "",
        "num_nodes": 4,
        "cpus-per-task": 128,
        "walltime": "0:30:00",
    }
    mock_config.nersc_multiresolution_settings = {
        "qos": "realtime",
        "account": "mock_account",
        "reservation": "",
        "cpus-per-task": 128,
        "walltime": "0:15:00",
    }
    mock_config.nersc_segment_sam3_settings = {
        "qos": "regular",
        "account": "mock_account",
        "constraint": "gpu",
        "reservation": "",
        "num_nodes": 4,
        "ntasks-per-node": 1,
        "gpus-per-node": 4,
        "cpus-per-task": 32,
        "walltime": "00:59:00",
        "batch_size": 1,
        "patch_size": 400,
        "confidence": [0.5],
        "overlap": 0.25,
        "prompts": ["cell wall", "lumen"],
        "cfs_path": "/mock/cfs",
        "conda_env_path": "/mock/conda/sam3",
        "seg_scripts_dir": "/mock/seg_scripts/sam3",
        "checkpoints_dir": "/mock/checkpoints",
        "bpe_path": "/mock/bpe.model",
        "original_checkpoint_path": "/mock/original.pt",
        "finetuned_checkpoint_path": "/mock/checkpoints/finetuned.pt",
    }
    mock_config.nersc_segment_dinov3_settings = {
        "qos": "regular",
        "account": "mock_account",
        "constraint": "gpu",
        "reservation": "",
        "num_nodes": 4,
        "ntasks-per-node": 1,
        "nproc_per_node": 4,
        "gpus-per-node": 4,
        "cpus-per-task": 32,
        "walltime": "00:59:00",
        "batch_size": 4,
        "cfs_path": "/mock/cfs",
        "conda_env_path": "/mock/conda/dino",
        "seg_scripts_dir": "/mock/seg_scripts/dino",
        "dino_checkpoint_path": "/mock/dino.pt",
    }
    mock_config.nersc_combine_segmentation_settings = {
        "qos": "regular",
        "account": "mock_account",
        "constraint": "cpu",
        "reservation": "",
        "num_nodes": 1,
        "ntasks": 128,
        "cpus-per-task": 1,
        "walltime": "01:00:00",
        "dilate_px": 5,
        "conda_env_path": "/mock/conda/combine",
        "seg_scripts_dir": "/mock/seg_scripts/combine",
    }

    mocker.patch("orchestration.flows.bl832.nersc.Config832", return_value=mock_config)
    return mock_config


@pytest.fixture
def mock_recon_success():
    return {"success": True, "job_id": "11111", "timing": None}


@pytest.fixture
def mock_seg_sam3_success():
    return {"success": True, "job_id": "22222", "timing": None, "output_dir": "/out/sam3"}


def _make_future(mocker, value):
    """Return a mock Prefect future whose .result() yields the given value."""
    f = mocker.MagicMock()
    f.result.return_value = value
    return f


@pytest.fixture
def mock_iriapi_client(mocker):
    """httpx.Client mock for IRI API responses."""
    client = mocker.MagicMock()

    submit_response = mocker.MagicMock()
    submit_response.json.return_value = {"id": "99999"}
    client.post.return_value = submit_response

    status_response = mocker.MagicMock()
    status_response.json.return_value = {"status": {"state": "completed"}}
    client.get.return_value = status_response

    return client

# ──────────────────────────────────────────────────────────────────────────────
# build_multi_resolution
# ──────────────────────────────────────────────────────────────────────────────

def test_build_multi_resolution_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.build_multi_resolution(file_path="folder/file.h5")

    assert mock_sfapi_client.compute.call_count == 2
    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.job.return_value.complete.assert_called_once()
    assert result is True


def test_build_multi_resolution_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("Submission failed")
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.build_multi_resolution(file_path="folder/file.h5")

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# segmentation_sam3
# ──────────────────────────────────────────────────────────────────────────────

def test_segmentation_sam3_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )
    mocker.patch.object(controller, "_fetch_seg_timing_from_output", return_value=None)

    result = controller.segmentation_sam3(recon_folder_path="folder/recfile")

    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.job.return_value.complete.assert_called_once()
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["job_id"] == "12345"


def test_segmentation_sam3_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("GPU queue full")
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.segmentation_sam3(recon_folder_path="folder/recfile")

    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["job_id"] is None


def test_segmentation_sam3_uses_variable_options(mocker, mock_sfapi_client, mock_config832):
    """Custom Prefect variable options should be forwarded into the job script."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={
        "defaults": False,
        "batch_size": 8,
        "patch_size": 512,
        "confidence": [0.6, 0.7],
        "overlap": 0.5,
        "qos": "debug",
        "account": "als_test",
        "constraint": "gpu",
        "checkpoint": "checkpoint_v7.pt",
    })

    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )
    mocker.patch.object(controller, "_fetch_seg_timing_from_output", return_value=None)

    captured_scripts = []
    original_return = mock_sfapi_client.compute.return_value.submit_job.return_value

    def capture_script(script):
        captured_scripts.append(script)
        return original_return

    mock_sfapi_client.compute.return_value.submit_job.side_effect = capture_script

    controller.segmentation_sam3(recon_folder_path="folder/recfile")

    assert captured_scripts, "submit_job was never called"
    script = captured_scripts[0]
    assert "checkpoint_v7.pt" in script
    assert "--patch-size 512" in script
    assert "0.6 0.7" in script
    assert "--overlap-ratio 0.5" in script
    assert "#SBATCH -q debug" in script
    assert "#SBATCH -A als_test" in script


# ──────────────────────────────────────────────────────────────────────────────
# segmentation_dinov3
# ──────────────────────────────────────────────────────────────────────────────

def test_segmentation_dinov3_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.segmentation_dinov3(recon_folder_path="folder/recfile")

    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.job.return_value.complete.assert_called_once()
    assert result is True


def test_segmentation_dinov3_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("No GPU nodes")
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.segmentation_dinov3(recon_folder_path="folder/recfile")
    assert result is False

# ---------------------------------------------------------------------------
# reconstruct — SFAPI
# ---------------------------------------------------------------------------


def test_reconstruct_sfapi_success(mocker, mock_sfapi_client, mock_config832):
    """SFAPI reconstruct submits a job and waits for completion."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")

    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.reconstruct(file_path="folder/scan.h5")

    assert result["success"] is True
    assert result["job_id"] == "12345"
    assert mock_sfapi_client.compute.call_count == 3  # 1 _submit_job() + 1 _wait_for_job() + 1 _fetch_timing_data()
    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.job.assert_called_once_with(jobid="12345")
    mock_sfapi_client.compute.return_value.job.return_value.complete.assert_called_once()


def test_reconstruct_sfapi_submission_failure(mocker, mock_sfapi_client, mock_config832):
    """SFAPI reconstruct returns False when submission raises."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("SFAPI error")

    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.reconstruct(file_path="folder/scan.h5")

    assert result["success"] is False


# ---------------------------------------------------------------------------
# reconstruct — IRIAPI
# ---------------------------------------------------------------------------

def test_reconstruct_iriapi_success(mocker, mock_iriapi_client, mock_config832, monkeypatch):
    """IRIAPI reconstruct POSTs a job and polls for COMPLETED state."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    monkeypatch.setenv("NERSC_USERNAME", "alsdev")
    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")

    controller = NERSCTomographyHPCController(
        client=mock_iriapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.IRIAPI,
    )

    result = controller.reconstruct(file_path="folder/scan.h5")

    # Resource lookups now live on the config, not module-level constants
    iri = mock_config832.nersc_resources["iri"]

    assert result["success"] is True
    assert result["job_id"] == "99999"
    mock_iriapi_client.post.assert_called_once()
    assert (
        mock_iriapi_client.post.call_args.args[0]
        == f"/api/v1/compute/job/{iri['perlmutter_job_submit']}"
    )
    posted_json = mock_iriapi_client.post.call_args.kwargs["json"]
    assert posted_json["executable"] == "/bin/bash"
    assert posted_json["arguments"] == ["-s"]
    assert "pre_launch" in posted_json
    assert "tomo_recon" in posted_json["pre_launch"]

    mock_iriapi_client.get.assert_any_call(
        f"/api/v1/compute/status/{iri['compute_resource']}/99999"
    )
    mock_iriapi_client.get.assert_any_call(
        f"/api/v1/filesystem/view/{iri['perlmutter_login']}",
        params={"path": mocker.ANY},
    )


def test_reconstruct_iriapi_job_failed(mocker, mock_iriapi_client, mock_config832, monkeypatch):
    """IRIAPI reconstruct returns False when job state is FAILED."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    monkeypatch.setenv("NERSC_USERNAME", "alsdev")
    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mock_iriapi_client.get.return_value.json.return_value = {"status": {"state": "failed"}}

    controller = NERSCTomographyHPCController(
        client=mock_iriapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.IRIAPI,
    )

    result = controller.reconstruct(file_path="folder/scan.h5")

    assert result["success"] is False


def test_reconstruct_iriapi_missing_username(mocker, mock_iriapi_client, mock_config832, monkeypatch):
    """IRIAPI reconstruct raises ValueError when NERSC_USERNAME is unset."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    monkeypatch.delenv("NERSC_USERNAME", raising=False)

    controller = NERSCTomographyHPCController(
        client=mock_iriapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.IRIAPI,
    )

    with pytest.raises(ValueError, match="NERSC_USERNAME"):
        controller.reconstruct(file_path="folder/scan.h5")


# ---------------------------------------------------------------------------
# build_multi_resolution — SFAPI
# ---------------------------------------------------------------------------

def test_build_multi_resolution_sfapi_success(mocker, mock_sfapi_client, mock_config832):
    """SFAPI build_multi_resolution submits and waits successfully."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")

    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.build_multi_resolution(file_path="folder/scan.h5")

    assert result is True
    assert mock_sfapi_client.compute.call_count == 2
    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)


def test_build_multi_resolution_sfapi_failure(mocker, mock_sfapi_client, mock_config832):
    """SFAPI build_multi_resolution returns False when submission raises."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("error")

    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.build_multi_resolution(file_path="folder/scan.h5")

    assert result is False


def test_segmentation_dinov3_output_paths(mocker, mock_sfapi_client, mock_config832):
    """
    Output dir should swap /rec for /seg in the folder name and route to /dino.

    Given recon_folder_path="folder/recfile", the code does:
        seg_folder = "folder/recfile".replace("/rec", "/seg")  →  "folder/segfile"
        output_dir = ".../scratch/folder/segfile/dino"
    So the script contains "segfile" and "/dino", not a literal "/seg/" segment.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})

    captured_scripts = []
    original_return = mock_sfapi_client.compute.return_value.submit_job.return_value

    def capture(script):
        captured_scripts.append(script)
        return original_return

    mock_sfapi_client.compute.return_value.submit_job.side_effect = capture
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )
    controller.segmentation_dinov3(recon_folder_path="folder/recfile")

    script = captured_scripts[0]
    assert "segfile" in script
    assert "/dino" in script


# ──────────────────────────────────────────────────────────────────────────────
# combine_segmentations
# ──────────────────────────────────────────────────────────────────────────────

def test_combine_segmentations_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.combine_segmentations(recon_folder_path="folder/recfile")

    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.job.return_value.complete.assert_called_once()
    assert result is True


def test_combine_segmentations_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("Cluster down")
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )

    result = controller.combine_segmentations(recon_folder_path="folder/recfile")

    assert result is False


def test_combine_segmentations_script_references_sam3_and_dino(mocker, mock_sfapi_client, mock_config832):
    """The combination job script should reference both model output directories."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})

    captured_scripts = []
    original_return = mock_sfapi_client.compute.return_value.submit_job.return_value

    def capture(script):
        captured_scripts.append(script)
        return original_return

    mock_sfapi_client.compute.return_value.submit_job.side_effect = capture
    controller = NERSCTomographyHPCController(
        client=mock_sfapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.SFAPI,
    )
    controller.combine_segmentations(recon_folder_path="folder/recfile")

    script = captured_scripts[0]
    assert "/sam3" in script
    assert "/dino" in script
    assert "combine_sam_dino_v3" in script
    assert "/combined" in script


# ──────────────────────────────────────────────────────────────────────────────
# Prefect tasks
#
# We call task.fn() to bypass Prefect's task runner machinery, but the task
# bodies call get_run_logger() which requires an active flow/task run context.
# Patching it at the module level avoids MissingContextError without needing
# a full prefect_test_harness flow run.
# ──────────────────────────────────────────────────────────────────────────────

def test_nersc_segmentation_sam3_task_success(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_segmentation_sam3_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_sam3.return_value = {
        "success": True, "job_id": "99", "timing": None, "output_dir": "/out"
    }
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_segmentation_sam3_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    mock_controller.segmentation_sam3.assert_called_once_with(recon_folder_path="folder/recfile")
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["job_id"] == "99"


def test_nersc_segmentation_sam3_task_failure(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_segmentation_sam3_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_sam3.return_value = {
        "success": False, "job_id": None, "timing": None, "output_dir": None
    }
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_segmentation_sam3_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    assert isinstance(result, dict)
    assert result["success"] is False


def test_nersc_segmentation_dinov3_task_success(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_segmentation_dinov3_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_dinov3.return_value = True
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_segmentation_dinov3_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    mock_controller.segmentation_dinov3.assert_called_once_with(recon_folder_path="folder/recfile", project="petiole")
    assert result is True


def test_nersc_segmentation_dinov3_task_failure(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_segmentation_dinov3_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_dinov3.return_value = False
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_segmentation_dinov3_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    assert result is False


def test_nersc_combine_segmentations_task_success(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_combine_segmentations_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.combine_segmentations.return_value = True
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_combine_segmentations_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    mock_controller.combine_segmentations.assert_called_once_with(recon_folder_path="folder/recfile")
    assert result is True


def test_nersc_combine_segmentations_task_failure(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_combine_segmentations_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.combine_segmentations.return_value = False
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_combine_segmentations_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# nersc_petiole_segment_flow  (recon + SAM3 + DINOv3 + combine)
#
# Replaces the former nersc_forge_recon_multisegment_flow tests.
# The cleaned nersc.py exposes nersc_petiole_segment_flow as the canonical
# multi-segmentation flow; controller.reconstruct() is the correct method name
# (reconstruct_multinode no longer exists).
#
# Prefect validates the `config` parameter against Optional[Config832] at
# runtime, so we pass config=None and let the flow call Config832() internally,
# which returns mock_config832 via the fixture patch.
# ──────────────────────────────────────────────────────────────────────────────

def test_petiole_segment_flow_both_succeed(mocker, mock_config832, mock_recon_success):
    """Recon + SAM3 + DINOv3 all succeed → combine is called → flow returns True."""
    from orchestration.flows.bl832.nersc import nersc_petiole_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_globus_transfer = mocker.patch("orchestration.flows.bl832.nersc.globus_transfer_task")
    mock_globus_transfer.submit.return_value = _make_future(mocker, True)

    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task")
    mock_dinov3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dinov3_task")
    mock_combine_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_combine_segmentations_task")

    mock_sam3_task.submit.return_value = _make_future(
        mocker, {"success": True, "job_id": "1", "timing": None, "output_dir": "/out/sam3"}
    )
    mock_dinov3_task.submit.return_value = _make_future(mocker, True)
    mock_combine_task.submit.return_value = _make_future(mocker, True)

    result = nersc_petiole_segment_flow(file_path="folder/file.h5", num_nodes=4, config=None)

    assert result is True
    mock_controller.reconstruct.assert_called_once()
    mock_sam3_task.submit.assert_called_once()
    mock_dinov3_task.submit.assert_called_once()
    mock_combine_task.submit.assert_called_once()


def test_petiole_segment_flow_only_sam3_succeeds(mocker, mock_config832, mock_recon_success):
    """When only SAM3 succeeds, combine should be skipped but flow returns True."""
    from orchestration.flows.bl832.nersc import nersc_petiole_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_globus_transfer = mocker.patch("orchestration.flows.bl832.nersc.globus_transfer_task")
    mock_globus_transfer.submit.return_value = _make_future(mocker, True)

    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task")
    mock_dinov3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dinov3_task")
    mock_combine_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_combine_segmentations_task")

    mock_sam3_task.submit.return_value = _make_future(
        mocker, {"success": True, "job_id": "1", "timing": None, "output_dir": "/out/sam3"}
    )
    mock_dinov3_task.submit.return_value = _make_future(mocker, False)

    result = nersc_petiole_segment_flow(file_path="folder/file.h5", num_nodes=4, config=None)

    assert result is True
    mock_combine_task.submit.assert_not_called()


def test_petiole_segment_flow_both_seg_fail(mocker, mock_config832, mock_recon_success):
    """Recon succeeds but both segmentations fail → flow returns False."""
    from orchestration.flows.bl832.nersc import nersc_petiole_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_globus_transfer = mocker.patch("orchestration.flows.bl832.nersc.globus_transfer_task")
    mock_globus_transfer.submit.return_value = _make_future(mocker, False)

    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task")
    mock_dinov3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dinov3_task")
    mock_combine_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_combine_segmentations_task")

    mock_sam3_task.submit.return_value = _make_future(
        mocker, {"success": False, "job_id": None, "timing": None, "output_dir": None}
    )
    mock_dinov3_task.submit.return_value = _make_future(mocker, False)

    result = nersc_petiole_segment_flow(file_path="folder/file.h5", num_nodes=4, config=None)

    assert result is False
    mock_combine_task.submit.assert_not_called()


def test_petiole_segment_flow_recon_failure(mocker, mock_config832):
    """Recon failure should raise ValueError immediately."""
    from orchestration.flows.bl832.nersc import nersc_petiole_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct.return_value = {"success": False, "job_id": None, "timing": None}
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)
    mocker.patch("orchestration.flows.bl832.nersc.globus_transfer_task")
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    with pytest.raises(ValueError, match="Reconstruction at NERSC Failed"):
        nersc_petiole_segment_flow(file_path="folder/file.h5", num_nodes=4, config=None)

# ──────────────────────────────────────────────────────────────────────────────
# nersc_moon_segment_flow  (recon + DINOv3-moon only, no SAM3, no combine)
# ──────────────────────────────────────────────────────────────────────────────


def test_moon_segment_flow_succeeds(mocker, mock_config832, mock_recon_success):
    """Recon + DINOv3-moon both succeed → flow returns True."""
    from orchestration.flows.bl832.nersc import nersc_moon_segment_flow, NERSCLoginMethod

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_globus_transfer = mocker.patch("orchestration.flows.bl832.nersc.globus_transfer_task")
    mock_globus_transfer.submit.return_value = _make_future(mocker, True)

    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_dinov3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dinov3_task")
    mock_dinov3_task.submit.return_value = _make_future(mocker, True)

    result = nersc_moon_segment_flow(file_path="folder/file.h5", num_nodes=4, config=None)

    assert result is True
    mock_controller.reconstruct.assert_called_once()
    mock_dinov3_task.submit.assert_called_once_with(
        recon_folder_path="folder/recfile",
        config=mock_config832,
        project="moon",
        login_method=NERSCLoginMethod.SFAPI,
    )


def test_moon_segment_flow_seg_failure(mocker, mock_config832, mock_recon_success):
    """Recon succeeds but DINOv3-moon fails → flow returns False."""
    from orchestration.flows.bl832.nersc import nersc_moon_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_globus_transfer = mocker.patch("orchestration.flows.bl832.nersc.globus_transfer_task")
    mock_globus_transfer.submit.return_value = _make_future(mocker, False)

    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_dinov3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dinov3_task")
    mock_dinov3_task.submit.return_value = _make_future(mocker, False)

    result = nersc_moon_segment_flow(file_path="folder/file.h5", num_nodes=4, config=None)

    assert result is False


def test_moon_segment_flow_recon_failure(mocker, mock_config832):
    """Recon failure should raise ValueError immediately."""
    from orchestration.flows.bl832.nersc import nersc_moon_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct.return_value = {"success": False, "job_id": None, "timing": None}
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)
    mocker.patch("orchestration.flows.bl832.nersc.globus_transfer_task")
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    with pytest.raises(ValueError, match="Reconstruction at NERSC failed"):
        nersc_moon_segment_flow(file_path="folder/file.h5", num_nodes=4, config=None)


def test_moon_segment_flow_no_sam3_no_combine(mocker, mock_config832, mock_recon_success):
    """SAM3 and combine tasks should never be called in the moon flow."""
    from orchestration.flows.bl832.nersc import nersc_moon_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_globus_transfer = mocker.patch("orchestration.flows.bl832.nersc.globus_transfer_task")
    mock_globus_transfer.submit.return_value = _make_future(mocker, True)

    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task")
    mock_combine_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_combine_segmentations_task")
    mock_dinov3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dinov3_task")
    mock_dinov3_task.submit.return_value = _make_future(mocker, True)

    nersc_moon_segment_flow(file_path="folder/file.h5", num_nodes=4, config=None)

    mock_sam3_task.submit.assert_not_called()
    mock_combine_task.submit.assert_not_called()
# ---------------------------------------------------------------------------
# build_multi_resolution — IRIAPI
# ---------------------------------------------------------------------------


def test_build_multi_resolution_iriapi_success(mocker, mock_iriapi_client, mock_config832, monkeypatch):
    """IRIAPI build_multi_resolution POSTs and polls successfully."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    monkeypatch.setenv("NERSC_USERNAME", "alsdev")
    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")

    controller = NERSCTomographyHPCController(
        client=mock_iriapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.IRIAPI,
    )

    result = controller.build_multi_resolution(file_path="folder/scan.h5")

    assert result is True
    mock_iriapi_client.post.assert_called_once()
    mock_iriapi_client.get.assert_called_once_with(
        "/api/v1/compute/status/compute/99999"
    )


def test_build_multi_resolution_iriapi_failure(mocker, mock_iriapi_client, mock_config832, monkeypatch):
    """IRIAPI build_multi_resolution returns False when job state is failed."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

    monkeypatch.setenv("NERSC_USERNAME", "alsdev")
    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mock_iriapi_client.get.return_value.json.return_value = {"status": {"state": "failed"}}

    controller = NERSCTomographyHPCController(
        client=mock_iriapi_client,
        config=mock_config832,
        login_method=NERSCLoginMethod.IRIAPI,
    )

    result = controller.build_multi_resolution(file_path="folder/scan.h5")

    assert result is False
