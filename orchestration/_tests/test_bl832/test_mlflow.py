# orchestration/_tests/test_bl832/test_mlflow.py
#
# Tests for the MLflow integration in the NERSC segmentation workflow.
# Covers:
#   - get_checkpoint_info (orchestration/mlflow.py)
#   - _load_job_options MLflow layer (orchestration/flows/bl832/nersc.py)
#   - segmentation_sam3 checkpoint resolution via MLflow

import json
import pytest
from uuid import uuid4

from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness


# ──────────────────────────────────────────────────────────────────────────────
# Session fixture
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        Secret(value=str(uuid4())).save(name="globus-client-id", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-client-secret", overwrite=True)
        yield


# ──────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_beamline_config(mocker):
    """Minimal BeamlineConfig mock with mlflow tracking_uri."""
    config = mocker.MagicMock()
    config.mlflow = {"tracking_uri": "http://mock-mlflow:5000"}
    return config


@pytest.fixture
def mock_config832(mocker):
    """
    Mock Config832 with fully-populated nersc_segment_sam3_settings.
    Matches the schema expected by _load_job_options / segmentation_sam3.
    """
    mock_config = mocker.MagicMock()
    mock_config.mlflow = {"tracking_uri": "http://mock-mlflow:5000"}
    mock_config.nersc_segment_sam3_settings = {
        "qos": "regular",
        "account": "als",
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
        "finetuned_checkpoint_path": "/mock/checkpoints/finetuned_v6.pt",
    }
    mocker.patch("orchestration.flows.bl832.nersc.Config832", return_value=mock_config)
    return mock_config


@pytest.fixture
def mock_sfapi_client(mocker):
    """Mock sfapi_client.Client with a completed SAM3 segmentation job."""
    mock_client = mocker.MagicMock()
    mock_user = mocker.MagicMock()
    mock_user.name = "testuser"
    mock_client.user.return_value = mock_user

    mock_job = mocker.MagicMock()
    mock_job.jobid = "55555"
    mock_job.state = "COMPLETED"
    mock_compute = mocker.MagicMock()
    mock_compute.submit_job.return_value = mock_job
    mock_client.compute.return_value = mock_compute

    mocker.patch("orchestration.flows.bl832.nersc.Client", return_value=mock_client)
    return mock_client


def _make_model_version(mocker, *, version="1", tags=None):
    """Helper: build a mock MlflowClient model version object."""
    mv = mocker.MagicMock()
    mv.version = version
    mv.tags = tags or {}
    return mv


# ──────────────────────────────────────────────────────────────────────────────
# get_checkpoint_info
# ──────────────────────────────────────────────────────────────────────────────

class TestGetCheckpointInfo:

    def test_returns_checkpoint_info_when_mlflow_reachable(self, mocker, mock_beamline_config):
        """Happy path: reachable server + valid production alias → ModelCheckpointInfo."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)

        mv = _make_model_version(mocker, version="3", tags={
            "nersc_path": "/cfs/checkpoints/sam3_v3.pt",
            "alcf_path": "/eagle/checkpoints/sam3_v3.pt",
            "batch_size": "2",
            "prompts": json.dumps(["cell wall", "lumen"]),
        })
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.return_value = mv
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config, alias="production")

        assert info is not None
        assert info.model_name == "sam3-petiole"
        assert info.version == "3"
        assert info.alias == "production"
        assert info.nersc_path == "/cfs/checkpoints/sam3_v3.pt"
        assert info.alcf_path == "/eagle/checkpoints/sam3_v3.pt"

    def test_deserializes_json_inference_params(self, mocker, mock_beamline_config):
        """JSON-encoded tag values (lists, dicts) are decoded into Python objects."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)
        mv = _make_model_version(mocker, tags={
            "nersc_path": "/cfs/sam3.pt",
            "prompts": json.dumps(["cell wall", "lumen"]),
            "confidence": json.dumps([0.6, 0.7]),
            "batch_size": "4",
        })
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.return_value = mv
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert info.inference_params["prompts"] == ["cell wall", "lumen"]
        assert info.inference_params["confidence"] == [0.6, 0.7]
        assert info.inference_params["batch_size"] == 4  # "4" is valid JSON → int

    def test_returns_none_when_mlflow_unreachable(self, mocker, mock_beamline_config):
        """Unreachable tracking server → None (caller falls back to config defaults)."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=False)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert info is None

    def test_returns_none_when_alias_not_found(self, mocker, mock_beamline_config):
        """Missing production alias → MlflowException → None."""
        from orchestration.mlflow import get_checkpoint_info
        import mlflow.exceptions

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.side_effect = (
            mlflow.exceptions.MlflowException("Alias not found")
        )
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert info is None

    def test_returns_none_when_nersc_path_tag_missing(self, mocker, mock_beamline_config):
        """A model version without 'nersc_path' tag → None."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)
        mv = _make_model_version(mocker, tags={"alcf_path": "/eagle/sam3.pt"})
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.return_value = mv
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert info is None

    def test_nersc_and_alcf_paths_excluded_from_inference_params(self, mocker, mock_beamline_config):
        """nersc_path and alcf_path must NOT appear in inference_params."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)
        mv = _make_model_version(mocker, tags={
            "nersc_path": "/cfs/sam3.pt",
            "alcf_path": "/eagle/sam3.pt",
            "batch_size": "2",
        })
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.return_value = mv
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert "nersc_path" not in info.inference_params
        assert "alcf_path" not in info.inference_params
        assert "batch_size" in info.inference_params


# ──────────────────────────────────────────────────────────────────────────────
# _load_job_options — MLflow layer
# ──────────────────────────────────────────────────────────────────────────────

class TestLoadJobOptionsMLflowLayer:
    """
    _load_job_options has three layers: config → MLflow → Prefect Variable.
    These tests isolate the MLflow layer by stubbing get_checkpoint_info and
    keeping the Prefect Variable at defaults.
    """

    def _patch_variable_defaults(self, mocker):
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            return_value={"defaults": True},
        )

    def test_mlflow_nersc_path_mapped_to_checkpoint_key(self, mocker, mock_config832):
        """When MLflow returns a checkpoint, nersc_path is written to mlflow_checkpoint_key."""
        from orchestration.jobs.options import load_job_options
        from orchestration.mlflow import ModelCheckpointInfo

        self._patch_variable_defaults(mocker)

        checkpoint_info = ModelCheckpointInfo(
            model_name="sam3-petiole",
            version="5",
            alias="production",
            nersc_path="/cfs/checkpoints/sam3_v5.pt",
            alcf_path="",
            inference_params={},
        )
        mocker.patch(
            "orchestration.jobs.options.get_checkpoint_info",
            return_value=checkpoint_info,
        )

        base_settings = dict(mock_config832.nersc_segment_sam3_settings)
        opts = load_job_options(
            "nersc-segmentation-options",
            base_settings,
            config=mock_config832,
            mlflow_model_name="sam3-petiole",
            mlflow_checkpoint_key="finetuned_checkpoint_path",
        )

        assert opts["finetuned_checkpoint_path"] == "/cfs/checkpoints/sam3_v5.pt"

    def test_mlflow_inference_params_overlay_config_defaults(self, mocker, mock_config832):
        """inference_params from MLflow overwrite matching config keys."""
        from orchestration.jobs.options import load_job_options
        from orchestration.mlflow import ModelCheckpointInfo

        self._patch_variable_defaults(mocker)

        checkpoint_info = ModelCheckpointInfo(
            model_name="sam3-petiole",
            version="2",
            alias="production",
            nersc_path="/cfs/sam3.pt",
            alcf_path="",
            inference_params={
                "batch_size": 8,
                "confidence": [0.6, 0.7],
                "prompts": ["lumen", "cell wall", "vessel"],
            },
        )
        mocker.patch(
            "orchestration.jobs.options.get_checkpoint_info",
            return_value=checkpoint_info,
        )

        base_settings = dict(mock_config832.nersc_segment_sam3_settings)
        opts = load_job_options(
            "nersc-segmentation-options",
            base_settings,
            config=mock_config832,
            mlflow_model_name="sam3-petiole",
            mlflow_checkpoint_key="finetuned_checkpoint_path",
        )

        assert opts["batch_size"] == 8
        assert opts["confidence"] == [0.6, 0.7]
        assert opts["prompts"] == ["lumen", "cell wall", "vessel"]

    def test_mlflow_layer_skipped_when_config_is_none(self, mocker, mock_config832):
        """Passing config=None skips the MLflow layer entirely."""
        from orchestration.jobs.options import load_job_options

        self._patch_variable_defaults(mocker)
        spy = mocker.patch("orchestration.jobs.options.get_checkpoint_info")

        base_settings = dict(mock_config832.nersc_segment_sam3_settings)
        opts = load_job_options(
            "nersc-segmentation-options",
            base_settings,
            config=None,
            mlflow_model_name="sam3-petiole",
            mlflow_checkpoint_key="finetuned_checkpoint_path",
        )

        spy.assert_not_called()
        # Config default should be unchanged
        assert opts["finetuned_checkpoint_path"] == base_settings["finetuned_checkpoint_path"]

    def test_mlflow_layer_skipped_when_model_name_is_none(self, mocker, mock_config832):
        """Passing mlflow_model_name=None skips the MLflow layer."""
        from orchestration.jobs.options import load_job_options

        self._patch_variable_defaults(mocker)
        spy = mocker.patch("orchestration.jobs.options.get_checkpoint_info")

        base_settings = dict(mock_config832.nersc_segment_sam3_settings)
        load_job_options(
            "nersc-segmentation-options",
            base_settings,
            config=mock_config832,
            mlflow_model_name=None,
        )

        spy.assert_not_called()

    def test_config_defaults_used_when_mlflow_returns_none(self, mocker, mock_config832):
        """get_checkpoint_info returning None → config defaults unchanged."""
        from orchestration.jobs.options import load_job_options

        self._patch_variable_defaults(mocker)
        mocker.patch(
            "orchestration.jobs.options.get_checkpoint_info",
            return_value=None,
        )

        base_settings = dict(mock_config832.nersc_segment_sam3_settings)
        opts = load_job_options(
            "nersc-segmentation-options",
            base_settings,
            config=mock_config832,
            mlflow_model_name="sam3-petiole",
            mlflow_checkpoint_key="finetuned_checkpoint_path",
        )

        assert opts["finetuned_checkpoint_path"] == base_settings["finetuned_checkpoint_path"]

    def test_config_defaults_used_when_mlflow_raises(self, mocker, mock_config832):
        """An exception from get_checkpoint_info is caught; config defaults are used."""
        from orchestration.jobs.options import load_job_options

        self._patch_variable_defaults(mocker)
        mocker.patch(
            "orchestration.jobs.options.get_checkpoint_info",
            side_effect=RuntimeError("Network timeout"),
        )

        base_settings = dict(mock_config832.nersc_segment_sam3_settings)
        opts = load_job_options(
            "nersc-segmentation-options",
            base_settings,
            config=mock_config832,
            mlflow_model_name="sam3-petiole",
            mlflow_checkpoint_key="finetuned_checkpoint_path",
        )

        assert opts["finetuned_checkpoint_path"] == base_settings["finetuned_checkpoint_path"]

    def test_prefect_variable_wins_over_mlflow(self, mocker, mock_config832):
        """Prefect Variable overrides take priority over MLflow inference params (layer 3 > layer 2)."""
        from orchestration.jobs.options import load_job_options
        from orchestration.mlflow import ModelCheckpointInfo

        # MLflow says batch_size=8; Prefect Variable says batch_size=16 → 16 wins
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            return_value={"defaults": False, "batch_size": 16},
        )

        checkpoint_info = ModelCheckpointInfo(
            model_name="sam3-petiole",
            version="2",
            alias="production",
            nersc_path="/cfs/sam3.pt",
            alcf_path="",
            inference_params={"batch_size": 8},
        )
        mocker.patch(
            "orchestration.jobs.options.get_checkpoint_info",
            return_value=checkpoint_info,
        )

        base_settings = dict(mock_config832.nersc_segment_sam3_settings)
        opts = load_job_options(
            "nersc-segmentation-options",
            base_settings,
            config=mock_config832,
            mlflow_model_name="sam3-petiole",
            mlflow_checkpoint_key="finetuned_checkpoint_path",
        )

        assert opts["batch_size"] == 16


# ──────────────────────────────────────────────────────────────────────────────
# segmentation_sam3 — checkpoint path from MLflow in the job script
# ──────────────────────────────────────────────────────────────────────────────

class TestSegmentationSam3MLflowCheckpoint:
    """
    Verify that when _load_job_options resolves a checkpoint path from MLflow,
    segmentation_sam3 uses it in the submitted SLURM job script.
    """

    def test_mlflow_checkpoint_appears_in_job_script(self, mocker, mock_config832):
        """
        When _load_job_options returns an MLflow-sourced finetuned_checkpoint_path,
        that path must appear in the SLURM script submitted to Perlmutter.
        """
        from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

        mocker.patch("orchestration.flows.bl832.nersc.time.sleep")

        mlflow_checkpoint = "/cfs/checkpoints/sam3_mlflow_v7.pt"
        resolved_settings = dict(mock_config832.nersc_segment_sam3_settings)
        resolved_settings["finetuned_checkpoint_path"] = mlflow_checkpoint

        mocker.patch(
            "orchestration.flows.bl832.nersc.load_job_options",
            return_value=resolved_settings,
        )

        # Capture the script passed to _submit_job — bypasses the SFAPI/IRIAPI
        # dispatch entirely and avoids the real client.post() / job polling.
        captured = []

        def capture_script(script, *args, **kwargs):
            captured.append(script)
            return "12345"  # fake job id

        # Use a real client mock just so __init__ doesn't fail; we'll never call it.
        controller = NERSCTomographyHPCController(
            client=mocker.MagicMock(),
            config=mock_config832,
            login_method=NERSCLoginMethod.IRIAPI,
        )
        mocker.patch.object(controller, "submit_job", side_effect=capture_script)
        mocker.patch.object(controller, "wait_for_job", return_value=True)
        mocker.patch.object(controller, "mkdir_remote", return_value=None)
        mocker.patch.object(controller, "_fetch_seg_timing_from_output", return_value=None)
        # _get_nersc_username reads NERSC_USERNAME for IRIAPI; stub it
        mocker.patch.object(controller, "get_nersc_username", return_value="testuser")

        result = controller.segmentation_sam3(recon_folder_path="folder/recfile")

        assert captured, "submit_job was never called"
        assert mlflow_checkpoint in captured[0], (
            "The MLflow checkpoint path must appear in the SLURM job script"
        )
        assert result["success"] is True

    def test_config_default_checkpoint_used_when_mlflow_unavailable(self, mocker, mock_config832):
        from orchestration.flows.bl832.nersc import NERSCTomographyHPCController, NERSCLoginMethod

        mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            return_value={"defaults": True},
        )
        mocker.patch(
            "orchestration.jobs.options.get_checkpoint_info",
            return_value=None,
        )

        captured = []

        def capture_script(script, *args, **kwargs):
            captured.append(script)
            return "12345"

        controller = NERSCTomographyHPCController(
            client=mocker.MagicMock(),
            config=mock_config832,
            login_method=NERSCLoginMethod.IRIAPI,
        )
        mocker.patch.object(controller, "submit_job", side_effect=capture_script)
        mocker.patch.object(controller, "wait_for_job", return_value=True)
        mocker.patch.object(controller, "mkdir_remote", return_value=None)
        mocker.patch.object(controller, "_fetch_seg_timing_from_output", return_value=None)
        mocker.patch.object(controller, "get_nersc_username", return_value="testuser")

        controller.segmentation_sam3(recon_folder_path="folder/recfile")

        config_default = mock_config832.nersc_segment_sam3_settings["finetuned_checkpoint_path"]
        assert captured, "submit_job was never called"
        assert config_default in captured[0], (
            "Config default checkpoint path must be used when MLflow is unavailable"
        )
