import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# Import the abstract PruneController base
from orchestration.prune_controller import PruneController


###############################################################################
# Dummy Implementation for Testing
###############################################################################
class DummyPruneController(PruneController):
    """
    A concrete dummy implementation of PruneController for testing purposes.

    This class uses a local directory (self.base_dir) to simulate file pruning.
    It provides additional helper methods:
      - get_files_to_delete(retention): returns files older than the given retention period.
      - prune_files(retention): deletes files older than the given retention period.
    """
    def __init__(self, base_dir: Path):
        # Create a dummy configuration object (the real config isn’t used in these tests)
        dummy_config = type("DummyConfig", (), {})()
        dummy_config.tc = None
        super().__init__(dummy_config)
        self.base_dir = base_dir

    def get_files_to_delete(self, retention: int):
        """
        Return a list of files in self.base_dir whose modification time is older than
        'retention' days.

        Args:
            retention (int): Retention period in days. Must be > 0.

        Returns:
            List[Path]: Files older than the retention period.

        Raises:
            ValueError: If retention is not positive.
        """
        if retention <= 0:
            raise ValueError("Retention must be a positive number of days")
        now_ts = datetime.now().timestamp()
        retention_seconds = retention * 24 * 3600
        files_to_delete = []
        for f in self.base_dir.glob("*"):
            if f.is_file():
                mod_time = f.stat().st_mtime
                if now_ts - mod_time > retention_seconds:
                    files_to_delete.append(f)
        return files_to_delete

    def prune_files(self, retention: int):
        """
        Delete files older than 'retention' days and return the list of deleted files.

        Args:
            retention (int): Retention period in days. Must be > 0.

        Returns:
            List[Path]: List of files that were deleted.

        Raises:
            ValueError: If retention is not positive.
        """
        if retention <= 0:
            raise ValueError("Retention must be a positive number of days")
        files_to_delete = self.get_files_to_delete(retention)
        for f in files_to_delete:
            f.unlink()  # Delete the file
        return files_to_delete

    def prune(
        self,
        file_path: str = None,
        source_endpoint=None,
        check_endpoint=None,
        days_from_now: timedelta = timedelta(0)
    ) -> bool:
        """
        Dummy implementation of the abstract method.
        (Not used in these tests.)
        """
        return True


###############################################################################
# Pytest Fixtures
###############################################################################
@pytest.fixture
def test_dir():
    """
    Fixture that creates (and later cleans up) a temporary directory for tests.
    """
    test_path = Path("test_prune_data")
    test_path.mkdir(exist_ok=True)
    yield test_path
    if test_path.exists():
        shutil.rmtree(test_path)


@pytest.fixture
def prune_controller(test_dir):
    """
    Fixture that returns an instance of DummyPruneController using the temporary directory.
    """
    return DummyPruneController(base_dir=test_dir)


###############################################################################
# Helper Function for Creating Test Files
###############################################################################
def create_test_files(directory: Path, dates):
    """
    Create test files in the specified directory with modification times given by `dates`.

    Args:
        directory (Path): The directory in which to create files.
        dates (List[datetime]): List of datetimes to set as the file's modification time.

    Returns:
        List[Path]: List of created file paths.
    """
    files = []
    for date in dates:
        filepath = directory / f"test_file_{date.strftime('%Y%m%d')}.txt"
        filepath.touch()  # Create the empty file
        os.utime(filepath, (date.timestamp(), date.timestamp()))
        files.append(filepath)
    return files


###############################################################################
# Tests
###############################################################################
def test_prune_controller_initialization(prune_controller):
    from orchestration.prune_controller import PruneController
    # Verify that our dummy controller is an instance of the abstract base class
    assert isinstance(prune_controller, PruneController)
    # And that the base directory exists
    assert prune_controller.base_dir.exists()


def test_get_files_to_delete(prune_controller, test_dir):
    # Create test files with various modification times.
    now = datetime.now()
    dates = [
        now - timedelta(days=31),  # Old enough to be pruned
        now - timedelta(days=20),  # Recent
        now - timedelta(days=40),  # Old enough to be pruned
        now - timedelta(days=10),  # Recent
    ]
    test_files = create_test_files(test_dir, dates)

    # When using a 30-day retention, the two older files should be flagged.
    files_to_delete = prune_controller.get_files_to_delete(30)
    assert len(files_to_delete) == 2

    # Check that the names of the older files are in the returned list.
    file_names_to_delete = {f.name for f in files_to_delete}
    assert test_files[0].name in file_names_to_delete
    assert test_files[2].name in file_names_to_delete


def test_prune_files(prune_controller, test_dir):
    # Create two files: one older than 30 days and one newer.
    now = datetime.now()
    dates = [
        now - timedelta(days=31),  # Should be deleted
        now - timedelta(days=20),  # Should remain
    ]
    test_files = create_test_files(test_dir, dates)

    # Prune files older than 30 days.
    deleted_files = prune_controller.prune_files(30)
    # One file should have been deleted.
    assert len(deleted_files) == 1
    # The older file should no longer exist.
    assert not test_files[0].exists()
    # The newer file should still exist.
    assert test_files[1].exists()


def test_empty_directory(prune_controller):
    # Ensure the test directory is empty.
    for f in list(prune_controller.base_dir.glob("*")):
        f.unlink()
    deleted_files = prune_controller.prune_files(30)
    # There should be no files to delete.
    assert len(deleted_files) == 0


def test_invalid_retention_period(prune_controller):
    # Using retention periods <= 0 should raise a ValueError.
    with pytest.raises(ValueError):
        prune_controller.prune_files(-1)
    with pytest.raises(ValueError):
        prune_controller.prune_files(0)
