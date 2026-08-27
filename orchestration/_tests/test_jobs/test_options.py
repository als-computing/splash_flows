"""Tests for orchestration/jobs/options.py — load_job_options three-layer resolution.

Patch targets verified from source:
  orchestration.jobs.options.Variable.get       (prefect.variables.Variable imported at top)
  orchestration.jobs.options.get_checkpoint_info (orchestration.mlflow imported at top)
"""

import json

from orchestration.jobs.options import load_job_options


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_checkpoint(mocker, *, nersc_path="/pscratch/checkpoint.pt", inference_params=None):
    cp = mocker.MagicMock()
    cp.nersc_path = nersc_path
    cp.inference_params = inference_params or {}
    return cp


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestLoadJobOptions:

    # ── Layer 1: config defaults ──────────────────────────────────────────────

    def test_returns_config_defaults_when_variable_says_defaults(self, mocker):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        opts = load_job_options("some-var", {"key": "value"})
        assert opts == {"key": "value"}

    def test_returns_copy_not_original(self, mocker):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        base = {"key": "value"}
        opts = load_job_options("some-var", base)
        opts["key"] = "mutated"
        assert base["key"] == "value"

    # ── Layer 2: MLflow ───────────────────────────────────────────────────────

    def test_mlflow_nersc_path_maps_to_checkpoint_key(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        cp = _make_checkpoint(mocker, nersc_path="/pscratch/model.pt")
        mocker.patch("orchestration.jobs.options.get_checkpoint_info", return_value=cp)

        opts = load_job_options(
            "var",
            {"finetuned_checkpoint_path": "/old/path"},
            config=mock_config,
            mlflow_model_name="my-model",
            mlflow_checkpoint_key="finetuned_checkpoint_path",
        )
        assert opts["finetuned_checkpoint_path"] == "/pscratch/model.pt"

    def test_mlflow_inference_params_overlay_config_defaults(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        cp = _make_checkpoint(mocker, inference_params={"batch_size": 16, "threshold": 0.5})
        mocker.patch("orchestration.jobs.options.get_checkpoint_info", return_value=cp)

        opts = load_job_options(
            "var",
            {"batch_size": 8, "threshold": 0.3, "other": "kept"},
            config=mock_config,
            mlflow_model_name="my-model",
        )
        assert opts["batch_size"] == 16
        assert opts["threshold"] == 0.5
        assert opts["other"] == "kept"

    def test_mlflow_layer_skipped_when_config_is_none(self, mocker):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        spy = mocker.patch("orchestration.jobs.options.get_checkpoint_info")

        opts = load_job_options("var", {"key": "value"}, config=None, mlflow_model_name="model")
        spy.assert_not_called()
        assert opts == {"key": "value"}

    def test_mlflow_layer_skipped_when_model_name_is_none(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        spy = mocker.patch("orchestration.jobs.options.get_checkpoint_info")

        opts = load_job_options("var", {"key": "value"}, config=mock_config, mlflow_model_name=None)
        spy.assert_not_called()
        assert opts == {"key": "value"}

    def test_mlflow_fallback_to_config_when_checkpoint_is_none(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        mocker.patch("orchestration.jobs.options.get_checkpoint_info", return_value=None)

        opts = load_job_options("var", {"key": "value"}, config=mock_config, mlflow_model_name="model")
        assert opts == {"key": "value"}

    def test_mlflow_fallback_to_config_when_get_checkpoint_raises(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        mocker.patch(
            "orchestration.jobs.options.get_checkpoint_info",
            side_effect=RuntimeError("mlflow unreachable"),
        )

        opts = load_job_options("var", {"key": "value"}, config=mock_config, mlflow_model_name="model")
        assert opts == {"key": "value"}

    def test_mlflow_injects_new_keys_not_in_config(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.options.Variable.get", return_value={"defaults": True})
        cp = _make_checkpoint(mocker, inference_params={"new_param": "injected"})
        mocker.patch("orchestration.jobs.options.get_checkpoint_info", return_value=cp)

        opts = load_job_options("var", {"existing": "kept"}, config=mock_config, mlflow_model_name="model")
        assert opts["new_param"] == "injected"
        assert opts["existing"] == "kept"

    # ── Layer 3: Prefect Variable overrides ───────────────────────────────────

    def test_prefect_variable_overrides_win_over_config(self, mocker):
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            return_value={"defaults": False, "key": "override"},
        )
        opts = load_job_options("var", {"key": "config-default"})
        assert opts["key"] == "override"

    def test_defaults_true_suppresses_variable_overrides(self, mocker):
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            return_value={"defaults": True, "key": "would-override"},
        )
        opts = load_job_options("var", {"key": "config-default"})
        assert opts["key"] == "config-default"

    def test_json_string_variable_is_parsed(self, mocker):
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            return_value=json.dumps({"defaults": False, "key": "from-json"}),
        )
        opts = load_job_options("var", {"key": "config-default"})
        assert opts["key"] == "from-json"

    def test_variable_get_failure_falls_back_to_opts(self, mocker):
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            side_effect=Exception("Prefect unavailable"),
        )
        opts = load_job_options("var", {"key": "config-default"})
        assert opts == {"key": "config-default"}

    def test_prefect_variable_wins_over_mlflow(self, mocker, mock_config):
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            return_value={"defaults": False, "batch_size": 99},
        )
        cp = _make_checkpoint(mocker, inference_params={"batch_size": 16})
        mocker.patch("orchestration.jobs.options.get_checkpoint_info", return_value=cp)

        opts = load_job_options(
            "var",
            {"batch_size": 8},
            config=mock_config,
            mlflow_model_name="model",
        )
        assert opts["batch_size"] == 99

    def test_defaults_key_not_present_in_output(self, mocker):
        mocker.patch(
            "orchestration.jobs.options.Variable.get",
            return_value={"defaults": False, "key": "override"},
        )
        opts = load_job_options("var", {})
        assert "defaults" not in opts
