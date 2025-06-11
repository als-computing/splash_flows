# tests/orchestration/_tests/test_prune_controllers.py

from pathlib import Path
from typing import Any, Dict, Optional

import pytest
from prefect.blocks.system import JSON
from prefect.testing.utilities import prefect_test_harness

from orchestration.prune_controller import (
    PruneController,
    FileSystemPruneController,
    GlobusPruneController,
    get_prune_controller,
    PruneMethod,
)
from orchestration.transfer_endpoints import FileSystemEndpoint
from orchestration.globus.transfer import GlobusEndpoint


###############################################################################
# Shared Fixtures & Helpers
###############################################################################

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """Set up the Prefect test harness and register our JSON block."""
    with prefect_test_harness():
        JSON(value={"max_wait_seconds": 600}).save(name="globus-settings")
        yield


class MockConfig:
    """Minimal config stub for controllers (only beamline_id and tc)."""
    def __init__(self, beamline_id: str = "test_beamline") -> None:
        self.beamline_id = beamline_id
        self.tc: Any = None  # transfer client stub


@pytest.fixture
def mock_config() -> MockConfig:
    """Provides a fresh MockConfig per test."""
    return MockConfig(beamline_id="unittest_beamline")


@pytest.fixture
def tmp_fs_path(tmp_path: Path) -> Path:
    """Temporary directory for filesystem tests."""
    return tmp_path


@pytest.fixture
def fs_endpoint(tmp_fs_path: Path) -> FileSystemEndpoint:
    """A FileSystemEndpoint rooted at our tmp directory."""
    return FileSystemEndpoint(
        name="fs_endpoint",
        root_path=str(tmp_fs_path),
        uri=str(tmp_fs_path),
    )


@pytest.fixture
def globus_endpoint(tmp_fs_path: Path) -> GlobusEndpoint:
    """A real GlobusEndpoint with a mock UUID."""
    return GlobusEndpoint(
        uuid="mock-uuid",
        uri=str(tmp_fs_path),
        root_path=str(tmp_fs_path),
        name="globus_endpoint",
    )


@pytest.fixture
def fs_controller(mock_config: MockConfig) -> FileSystemPruneController:
    """FileSystemPruneController using mock_config."""
    return FileSystemPruneController(config=mock_config)


@pytest.fixture
def globus_controller(mock_config: MockConfig) -> GlobusPruneController:
    """GlobusPruneController using mock_config."""
    return GlobusPruneController(config=mock_config)


def create_file_or_dir(root: Path, rel: str, mkdir: bool = False) -> Path:
    """
    Create either a file or directory under root/rel.
    If mkdir is True, makes a directory; otherwise creates an empty file.
    """
    p = root / rel
    if mkdir:
        p.mkdir(parents=True, exist_ok=True)
    else:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
    return p


###############################################################################
# Mock Fixtures for External Calls
###############################################################################

@pytest.fixture
def mock_scheduler(monkeypatch):
    """
    Monkeypatches schedule_prefect_flow → a mock that records its args and returns True.
    Returns the dict where call args are recorded.
    """
    recorded: Dict[str, Any] = {}

    def _scheduler(deployment_name, flow_run_name, parameters, duration_from_now):
        recorded.update(
            deployment_name=deployment_name,
            flow_run_name=flow_run_name,
            parameters=parameters,
            duration=duration_from_now,
        )
        return True

    monkeypatch.setattr(
        "orchestration.prune_controller.schedule_prefect_flow",
        _scheduler,
    )
    return recorded


@pytest.fixture
def mock_scheduler_raises(monkeypatch):
    """
    Monkeypatches schedule_prefect_flow → a mock that always raises.
    """
    def _scheduler_raises(*args, **kwargs):
        raise RuntimeError("scheduler failure")

    monkeypatch.setattr(
        "orchestration.prune_controller.schedule_prefect_flow",
        _scheduler_raises,
    )


@pytest.fixture
def mock_prune_one_safe(monkeypatch):
    """
    Monkeypatches prune_one_safe → a mock that records its kwargs and returns True.
    Returns the dict where call args are recorded.
    """
    recorded: Dict[str, Any] = {}

    def _prune_one_safe(
        file: str,
        if_older_than_days: int,
        transfer_client: Any,
        source_endpoint: GlobusEndpoint,
        check_endpoint: Optional[GlobusEndpoint],
        logger: Any,
        max_wait_seconds: int,
    ) -> bool:
        recorded.update(
            file=file,
            if_older_than_days=if_older_than_days,
            transfer_client=transfer_client,
            source_endpoint=source_endpoint,
            check_endpoint=check_endpoint,
            max_wait_seconds=max_wait_seconds,
        )
        return True

    monkeypatch.setattr(
        "orchestration.prune_controller.prune_one_safe",
        _prune_one_safe,
    )
    return recorded


###############################################################################
# Tests
###############################################################################

def test_prunecontroller_is_abstract():
    """PruneController must be abstract (cannot be instantiated directly)."""
    with pytest.raises(TypeError):
        PruneController(config=MockConfig())


def test_get_prune_controller_factory_correct_types(mock_config):
    """get_prune_controller returns the right subclass or raises on invalid."""
    assert isinstance(get_prune_controller(PruneMethod.SIMPLE, mock_config), FileSystemPruneController)
    assert isinstance(get_prune_controller(PruneMethod.GLOBUS, mock_config), GlobusPruneController)
    with pytest.raises((AttributeError, ValueError)):
        get_prune_controller("invalid", mock_config)  # type: ignore


def test_fs_prune_immediate_deletes_file_directly(fs_controller, fs_endpoint, tmp_fs_path):
    """Immediate FileSystem prune should delete an existing file."""
    rel = "subdir/foo.txt"
    p = create_file_or_dir(tmp_fs_path, rel)
    assert p.exists()

    fn = fs_controller._prune_filesystem_endpoint.fn  # type: ignore
    assert fn(relative_path=rel, source_endpoint=fs_endpoint, check_endpoint=None, config=fs_controller.config)
    assert not p.exists()


def test_fs_prune_immediate_returns_false_if_missing(fs_controller, fs_endpoint, tmp_fs_path):
    """Immediate FileSystem prune should return False for missing path."""
    rel = "no/such/file.txt"
    assert not (tmp_fs_path / rel).exists()

    fn = fs_controller._prune_filesystem_endpoint.fn  # type: ignore
    assert fn(relative_path=rel, source_endpoint=fs_endpoint, check_endpoint=None, config=fs_controller.config) is False


def test_fs_prune_schedules_when_days_from_now_positive(fs_controller, fs_endpoint, tmp_fs_path, mock_scheduler):
    """Calling prune with days_from_now>0 should schedule a Prefect flow."""
    rel = "to_schedule.txt"
    create_file_or_dir(tmp_fs_path, rel)

    result = fs_controller.prune(
        file_path=rel,
        source_endpoint=fs_endpoint,
        check_endpoint=None,
        days_from_now=1.5,
    )
    assert result is True

    assert mock_scheduler["flow_run_name"] == f"prune_from_{fs_endpoint.name}"
    assert mock_scheduler["parameters"]["relative_path"] == rel
    assert pytest.approx(mock_scheduler["duration"].total_seconds()) == 1.5 * 86400


def test_fs_prune_returns_false_if_schedule_raises(fs_controller, fs_endpoint, tmp_fs_path, mock_scheduler_raises):
    """If scheduling fails, fs_controller.prune should return False."""
    rel = "error.txt"
    create_file_or_dir(tmp_fs_path, rel)

    assert fs_controller.prune(
        file_path=rel,
        source_endpoint=fs_endpoint,
        check_endpoint=None,
        days_from_now=2.0,
    ) is False


def test_globus_prune_immediate_calls_prune_one_safe_directly(
    globus_controller,
    globus_endpoint,
    tmp_fs_path,
    mock_prune_one_safe
):
    """Immediate Globus prune should invoke prune_one_safe with correct arguments."""
    rel = "data.bin"
    create_file_or_dir(tmp_fs_path, rel)
    assert (tmp_fs_path / rel).exists()

    fn = globus_controller._prune_globus_endpoint.fn  # type: ignore
    _ = fn(
        relative_path=rel,
        source_endpoint=globus_endpoint,
        check_endpoint=None,
        config=globus_controller.config,
    )

    assert mock_prune_one_safe["file"] == rel
    assert mock_prune_one_safe["if_older_than_days"] == 0
    assert mock_prune_one_safe["transfer_client"] is None
    assert mock_prune_one_safe["source_endpoint"] is globus_endpoint
    assert mock_prune_one_safe["max_wait_seconds"] == 600


@pytest.mark.parametrize("invalid_fp", [None, ""])
def test_globus_prune_rejects_missing_file_path_directly(globus_controller, globus_endpoint, invalid_fp):
    """Globus prune should return False when file_path is None or empty."""
    assert globus_controller.prune(
        file_path=invalid_fp,
        source_endpoint=globus_endpoint,
        check_endpoint=None,
        days_from_now=0.0,
    ) is False


def test_globus_prune_rejects_missing_endpoint_directly(globus_controller, tmp_fs_path):
    """Globus prune should return False when source_endpoint is None."""
    (tmp_fs_path / "whatever").touch()

    assert globus_controller.prune(
        file_path="whatever",
        source_endpoint=None,  # type: ignore
        check_endpoint=None,
        days_from_now=0.0,
    ) is False


def test_globus_prune_schedules_when_days_from_now_positive(globus_controller, globus_endpoint, tmp_fs_path, mock_scheduler):
    """Calling Globus prune with days_from_now>0 should schedule a Prefect flow."""
    rel = "sched.txt"
    create_file_or_dir(tmp_fs_path, rel)

    result = globus_controller.prune(
        file_path=rel,
        source_endpoint=globus_endpoint,
        check_endpoint=None,
        days_from_now=3.0,
    )
    assert result is True

    assert mock_scheduler["flow_run_name"] == f"prune_from_{globus_endpoint.name}"
    assert pytest.approx(mock_scheduler["duration"].total_seconds()) == 3.0 * 86400


def test_globus_prune_returns_false_if_schedule_raises(globus_controller, globus_endpoint, tmp_fs_path, mock_scheduler_raises):
    """If scheduling fails, globus_controller.prune should return False."""
    rel = "err.txt"
    create_file_or_dir(tmp_fs_path, rel)

    assert globus_controller.prune(
        file_path=rel,
        source_endpoint=globus_endpoint,
        check_endpoint=None,
        days_from_now=4.0,
    ) is False
