# orchestration/_tests/bl832/test_alcf.py

import pytest
from uuid import uuid4

from prefect.blocks.system import Secret
from prefect.variables import Variable
from prefect.testing.utilities import prefect_test_harness


# ──────────────────────────────────────────────────────────────────────────────
# Session fixture
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """Set up Prefect test harness with all required secrets and variables."""
    with prefect_test_harness():
        Secret(value=str(uuid4())).save(name="globus-client-id", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-client-secret", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-compute-endpoint", overwrite=True)

        Variable.set(
            name="alcf-allocation-root-path",
            value={"alcf-allocation-root-path": "/eagle/test"},
            overwrite=True, _sync=True
        )
        Variable.set(name="pruning-config",
                     value={"max_wait_seconds": 600}, overwrite=True, _sync=True)
        yield


# ──────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_future(mocker, value):
    """Return a mock Prefect future whose .result() yields the given value."""
    f = mocker.MagicMock()
    f.result.return_value = value
    return f


def _patch_config832(mocker):
    """
    Patch all Config832 network dependencies and return a real Config832 instance.

    Flow tests receive a real Config832 so Prefect's parameter type validation
    passes, but all underlying Globus clients and endpoints are mocked.
    """
    mock_secret = mocker.MagicMock()
    mock_secret.get.return_value = str(uuid4())
    mocker.patch("prefect.blocks.system.Secret.load", return_value=mock_secret)

    endpoint_mock = mocker.MagicMock()
    mocker.patch(
        "orchestration.flows.bl832.config.transfer.build_endpoints",
        return_value={k: endpoint_mock for k in [
            "spot832", "data832", "data832_raw", "data832_scratch",
            "nersc832", "nersc_alsdev",
            "nersc832_alsdev_raw", "nersc832_alsdev_scratch",
            "nersc832_alsdev_pscratch_raw", "nersc832_alsdev_pscratch_scratch",
            "nersc832_alsdev_recon_scripts",
            "alcf832_iri_raw", "alcf832_iri_scratch",
            "alcf832_synaps_raw", "alcf832_synaps_recon", "alcf832_synaps_segment",
        ]}
    )
    mocker.patch("orchestration.flows.bl832.config.transfer.build_apps",
                 return_value={"als_transfer": "mock_app"})
    mocker.patch("orchestration.flows.bl832.config.transfer.init_transfer_client",
                 return_value=mocker.MagicMock())
    mocker.patch("orchestration.flows.bl832.config.flows.get_flows_client",
                 return_value=mocker.MagicMock())
    mocker.patch("orchestration.config.settings", mocker.MagicMock())

    from orchestration.flows.bl832.config import Config832
    return Config832()


def _mock_executor(mocker):
    """Return a context-manager-compatible mock Executor."""
    mock_exec = mocker.MagicMock()
    mock_exec.__enter__ = mocker.MagicMock(return_value=mock_exec)
    mock_exec.__exit__ = mocker.MagicMock(return_value=False)
    mocker.patch("orchestration.flows.bl832.alcf.Executor", return_value=mock_exec)
    return mock_exec


# ──────────────────────────────────────────────────────────────────────────────
# Controller fixture
#
# Controller tests instantiate ALCFTomographyHPCController directly, outside
# any flow context. We patch get_run_logger (used in __init__ and every method)
# and Variable.get (used in __init__ for allocation root and in methods for
# endpoint UUIDs).
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_controller(mocker):
    """ALCFTomographyHPCController with all external dependencies mocked."""
    mocker.patch("orchestration.flows.bl832.alcf.get_run_logger",
                 return_value=mocker.MagicMock())

    def variable_get_side_effect(name, default=None, _sync=False):
        if name == "alcf-allocation-root-path":
            result = mocker.MagicMock()
            result.get.return_value = "/eagle/test"
            return result
        # endpoint UUIDs and other variables: return a stable UUID string
        return default if default is not None else str(uuid4())

    mocker.patch("orchestration.flows.bl832.alcf.Variable.get",
                 side_effect=variable_get_side_effect)

    mocker.patch("orchestration.flows.bl832.alcf.Client")
    mocker.patch("orchestration.flows.bl832.alcf.Secret")

    mock_config = mocker.MagicMock()
    from orchestration.flows.bl832.alcf import ALCFTomographyHPCController
    return ALCFTomographyHPCController(config=mock_config)


# ──────────────────────────────────────────────────────────────────────────────
# ALCFTomographyHPCController — reconstruct
# ──────────────────────────────────────────────────────────────────────────────

def test_reconstruct_success(mocker, mock_controller):
    mock_exec = _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=True)

    result = mock_controller.reconstruct(file_path="folder/file.h5")

    assert result is True
    mock_exec.submit.assert_called_once()


def test_reconstruct_failure(mocker, mock_controller):
    _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=False)

    result = mock_controller.reconstruct(file_path="folder/file.h5")

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# ALCFTomographyHPCController — build_multi_resolution
# ──────────────────────────────────────────────────────────────────────────────

def test_build_multi_resolution_success(mocker, mock_controller):
    mock_exec = _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=True)

    result = mock_controller.build_multi_resolution(file_path="folder/file.h5")

    assert result is True
    mock_exec.submit.assert_called_once()


def test_build_multi_resolution_failure(mocker, mock_controller):
    _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=False)

    result = mock_controller.build_multi_resolution(file_path="folder/file.h5")

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# ALCFTomographyHPCController — segmentation_sam3
# ──────────────────────────────────────────────────────────────────────────────

def test_segmentation_sam3_success(mocker, mock_controller):
    mock_exec = _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=True)

    result = mock_controller.segmentation_sam3(recon_folder_path="folder/recfile")

    assert result is True
    mock_exec.submit.assert_called_once()


def test_segmentation_sam3_failure(mocker, mock_controller):
    _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=False)

    result = mock_controller.segmentation_sam3(recon_folder_path="folder/recfile")

    assert result is False


def test_segmentation_sam3_output_paths(mocker, mock_controller):
    """
    SAM3 output dir should be under /sam3 and use the rec→seg path substitution.
    Given 'folder/recfile', the code does replace('/rec', '/seg') → 'folder/segfile',
    so output_dir should contain 'segfile' and '/sam3'.
    """
    mock_exec = _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=True)

    mock_controller.segmentation_sam3(recon_folder_path="folder/recfile")

    call_kwargs = mock_exec.submit.call_args.kwargs
    assert "segfile" in call_kwargs.get("output_dir", "")
    assert "/sam3" in call_kwargs.get("output_dir", "")


# ──────────────────────────────────────────────────────────────────────────────
# ALCFTomographyHPCController — segmentation_dino
# ──────────────────────────────────────────────────────────────────────────────

def test_segmentation_dino_success(mocker, mock_controller):
    mock_exec = _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=True)

    result = mock_controller.segmentation_dino(recon_folder_path="folder/recfile")

    assert result is True
    mock_exec.submit.assert_called_once()


def test_segmentation_dino_failure(mocker, mock_controller):
    _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=False)

    result = mock_controller.segmentation_dino(recon_folder_path="folder/recfile")

    assert result is False


def test_segmentation_dino_output_paths(mocker, mock_controller):
    """DINO output dir should be under /dino."""
    mock_exec = _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=True)

    mock_controller.segmentation_dino(recon_folder_path="folder/recfile")

    call_kwargs = mock_exec.submit.call_args.kwargs
    assert "/dino" in call_kwargs.get("output_dir", "")


# ──────────────────────────────────────────────────────────────────────────────
# ALCFTomographyHPCController — combine_segmentations
# ──────────────────────────────────────────────────────────────────────────────

def test_combine_segmentations_success(mocker, mock_controller):
    mock_exec = _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=True)

    result = mock_controller.combine_segmentations(recon_folder_path="folder/recfile")

    assert result is True
    mock_exec.submit.assert_called_once()


def test_combine_segmentations_failure(mocker, mock_controller):
    _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=False)

    result = mock_controller.combine_segmentations(recon_folder_path="folder/recfile")

    assert result is False


def test_combine_segmentations_passes_sam3_and_dino_paths(mocker, mock_controller):
    """Wrapper should be called with both sam3 and dino result paths."""
    mock_exec = _mock_executor(mocker)
    mocker.patch.object(mock_controller, "_wait_for_globus_compute_future", return_value=True)

    mock_controller.combine_segmentations(recon_folder_path="folder/recfile")

    call_kwargs = mock_exec.submit.call_args.kwargs
    assert "/sam3" in call_kwargs.get("sam3_results", "")
    assert "/dino" in call_kwargs.get("dino_results", "")
    assert "/combined" in call_kwargs.get("combined_output", "")


# ──────────────────────────────────────────────────────────────────────────────
# Prefect tasks
# ──────────────────────────────────────────────────────────────────────────────

def test_alcf_segmentation_sam3_task_success(mocker):
    from orchestration.flows.bl832.alcf import alcf_segmentation_sam3_task

    mocker.patch("orchestration.flows.bl832.alcf.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_sam3.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_controller", return_value=mock_controller)

    result = alcf_segmentation_sam3_task.fn(
        recon_folder_path="folder/recfile", config=mocker.MagicMock()
    )

    mock_controller.segmentation_sam3.assert_called_once_with(recon_folder_path="folder/recfile")
    assert result is True


def test_alcf_segmentation_sam3_task_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_segmentation_sam3_task

    mocker.patch("orchestration.flows.bl832.alcf.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_sam3.return_value = False
    mocker.patch("orchestration.flows.bl832.alcf.get_controller", return_value=mock_controller)

    result = alcf_segmentation_sam3_task.fn(
        recon_folder_path="folder/recfile", config=mocker.MagicMock()
    )

    assert result is False


def test_alcf_segmentation_dino_task_success(mocker):
    from orchestration.flows.bl832.alcf import alcf_segmentation_dino_task

    mocker.patch("orchestration.flows.bl832.alcf.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_dino.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_controller", return_value=mock_controller)

    result = alcf_segmentation_dino_task.fn(
        recon_folder_path="folder/recfile", config=mocker.MagicMock()
    )

    mock_controller.segmentation_dino.assert_called_once_with(recon_folder_path="folder/recfile")
    assert result is True


def test_alcf_segmentation_dino_task_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_segmentation_dino_task

    mocker.patch("orchestration.flows.bl832.alcf.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_dino.return_value = False
    mocker.patch("orchestration.flows.bl832.alcf.get_controller", return_value=mock_controller)

    result = alcf_segmentation_dino_task.fn(
        recon_folder_path="folder/recfile", config=mocker.MagicMock()
    )

    assert result is False


def test_alcf_combine_segmentations_task_success(mocker):
    from orchestration.flows.bl832.alcf import alcf_combine_segmentations_task

    mocker.patch("orchestration.flows.bl832.alcf.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.combine_segmentations.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_controller", return_value=mock_controller)

    result = alcf_combine_segmentations_task.fn(
        recon_folder_path="folder/recfile", config=mocker.MagicMock()
    )

    mock_controller.combine_segmentations.assert_called_once_with(recon_folder_path="folder/recfile")
    assert result is True


def test_alcf_combine_segmentations_task_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_combine_segmentations_task

    mocker.patch("orchestration.flows.bl832.alcf.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.combine_segmentations.return_value = False
    mocker.patch("orchestration.flows.bl832.alcf.get_controller", return_value=mock_controller)

    result = alcf_combine_segmentations_task.fn(
        recon_folder_path="folder/recfile", config=mocker.MagicMock()
    )

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# alcf_recon_flow
#
# Flow tests pass a real Config832 instance (not a MagicMock) so Prefect's
# parameter type validation passes. Config832's network dependencies are all
# mocked via _patch_config832. Controller methods are patched on the class so
# the real __init__ runs (in the flow context) while no HPC calls are made.
# ──────────────────────────────────────────────────────────────────────────────

def test_alcf_recon_flow_success(mocker):
    from orchestration.flows.bl832.alcf import alcf_recon_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)

    mock_prune = mocker.MagicMock()
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mock_prune)

    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)
    mocker.patch.object(ALCFTomographyHPCController, "build_multi_resolution", return_value=True)

    result = alcf_recon_flow(file_path="folder/file.h5", config=mock_config)

    assert result is True
    assert mock_transfer.copy.call_count == 3   # raw→ALCF, tiff→data832, zarr→data832
    assert mock_prune.prune.call_count == 5     # raw, tiff(ALCF), zarr(ALCF), tiff(data832), zarr(data832)


def test_alcf_recon_flow_transfer_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_recon_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = False
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)
    mocker.patch.object(ALCFTomographyHPCController, "build_multi_resolution", return_value=True)

    with pytest.raises(ValueError, match="Transfer to ALCF Failed"):
        alcf_recon_flow(file_path="folder/file.h5", config=mock_config)


def test_alcf_recon_flow_recon_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_recon_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=False)
    mocker.patch.object(ALCFTomographyHPCController, "build_multi_resolution", return_value=True)

    with pytest.raises(ValueError, match="Reconstruction at ALCF Failed"):
        alcf_recon_flow(file_path="folder/file.h5", config=mock_config)


def test_alcf_recon_flow_multires_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_recon_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)
    mocker.patch.object(ALCFTomographyHPCController, "build_multi_resolution", return_value=False)

    with pytest.raises(ValueError, match="Tiff to Zarr at ALCF Failed"):
        alcf_recon_flow(file_path="folder/file.h5", config=mock_config)


# ──────────────────────────────────────────────────────────────────────────────
# alcf_forge_recon_segment_flow
# ──────────────────────────────────────────────────────────────────────────────

def test_alcf_forge_recon_segment_flow_success(mocker):
    from orchestration.flows.bl832.alcf import alcf_forge_recon_segment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)

    mock_seg_task = mocker.patch(
        "orchestration.flows.bl832.alcf.alcf_segmentation_sam3_task", return_value=True
    )

    result = alcf_forge_recon_segment_flow(file_path="folder/file.h5", config=mock_config)

    assert result is True
    mock_seg_task.assert_called_once()
    assert mock_transfer.copy.call_count == 3   # raw→ALCF, tiff→data832, seg→data832


def test_alcf_forge_recon_segment_flow_transfer_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_forge_recon_segment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = False
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)

    with pytest.raises(ValueError, match="Transfer to ALCF Failed"):
        alcf_forge_recon_segment_flow(file_path="folder/file.h5", config=mock_config)


def test_alcf_forge_recon_segment_flow_recon_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_forge_recon_segment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=False)

    with pytest.raises(ValueError, match="Reconstruction at ALCF Failed"):
        alcf_forge_recon_segment_flow(file_path="folder/file.h5", config=mock_config)


def test_alcf_forge_recon_segment_flow_seg_failure(mocker):
    """Flow should return False (not raise) when segmentation fails."""
    from orchestration.flows.bl832.alcf import alcf_forge_recon_segment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)
    mocker.patch("orchestration.flows.bl832.alcf.alcf_segmentation_sam3_task", return_value=False)

    result = alcf_forge_recon_segment_flow(file_path="folder/file.h5", config=mock_config)

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# alcf_forge_recon_multisegment_flow
# ──────────────────────────────────────────────────────────────────────────────

def test_alcf_forge_recon_multisegment_flow_both_succeed(mocker):
    from orchestration.flows.bl832.alcf import alcf_forge_recon_multisegment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)
    mock_combine = mocker.patch.object(
        ALCFTomographyHPCController, "combine_segmentations", return_value=True
    )

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.alcf.alcf_segmentation_sam3_task")
    mock_dino_task = mocker.patch("orchestration.flows.bl832.alcf.alcf_segmentation_dino_task")
    mock_sam3_task.submit.return_value = _make_future(mocker, True)
    mock_dino_task.submit.return_value = _make_future(mocker, True)

    result = alcf_forge_recon_multisegment_flow(file_path="folder/file.h5", config=mock_config)

    assert result is True
    mock_sam3_task.submit.assert_called_once()
    mock_dino_task.submit.assert_called_once()
    mock_combine.assert_called_once()


def test_alcf_forge_recon_multisegment_flow_only_sam3_succeeds(mocker):
    """Combine is skipped when DINO fails; flow still returns True."""
    from orchestration.flows.bl832.alcf import alcf_forge_recon_multisegment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)
    mock_combine = mocker.patch.object(
        ALCFTomographyHPCController, "combine_segmentations", return_value=True
    )

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.alcf.alcf_segmentation_sam3_task")
    mock_dino_task = mocker.patch("orchestration.flows.bl832.alcf.alcf_segmentation_dino_task")
    mock_sam3_task.submit.return_value = _make_future(mocker, True)
    mock_dino_task.submit.return_value = _make_future(mocker, False)

    result = alcf_forge_recon_multisegment_flow(file_path="folder/file.h5", config=mock_config)

    assert result is True
    mock_combine.assert_not_called()


def test_alcf_forge_recon_multisegment_flow_both_seg_fail(mocker):
    from orchestration.flows.bl832.alcf import alcf_forge_recon_multisegment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)
    mock_combine = mocker.patch.object(
        ALCFTomographyHPCController, "combine_segmentations", return_value=True
    )

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.alcf.alcf_segmentation_sam3_task")
    mock_dino_task = mocker.patch("orchestration.flows.bl832.alcf.alcf_segmentation_dino_task")
    mock_sam3_task.submit.return_value = _make_future(mocker, False)
    mock_dino_task.submit.return_value = _make_future(mocker, False)

    result = alcf_forge_recon_multisegment_flow(file_path="folder/file.h5", config=mock_config)

    assert result is False
    mock_combine.assert_not_called()


def test_alcf_forge_recon_multisegment_flow_recon_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_forge_recon_multisegment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=False)

    with pytest.raises(ValueError, match="Reconstruction at ALCF Failed"):
        alcf_forge_recon_multisegment_flow(file_path="folder/file.h5", config=mock_config)


def test_alcf_forge_recon_multisegment_flow_transfer_failure(mocker):
    from orchestration.flows.bl832.alcf import alcf_forge_recon_multisegment_flow, ALCFTomographyHPCController

    mock_config = _patch_config832(mocker)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = False
    mocker.patch("orchestration.flows.bl832.alcf.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.alcf.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch.object(ALCFTomographyHPCController, "reconstruct", return_value=True)

    with pytest.raises(ValueError, match="Transfer to ALCF Failed"):
        alcf_forge_recon_multisegment_flow(file_path="folder/file.h5", config=mock_config)
