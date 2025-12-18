import json
import numpy as np
import os
import sys
import types
from typing import List
from pathlib import Path
from pytest_mock import MockFixture

from orchestration.flows.scicat.utils import Issue
from orchestration.flows.scicat.ingest import scicat_ingest_flow

# This code is in an indeterminate state because it is unclear whether
# the SciCat ingest flow should be tested here, when it is already tested
# with Prefect in the scicat_beamline package tests.

# There may be some need for end-to-end testing where the destination SciCat
# server is mocked or switched to the staging server, and in that case this code
# may be useful.

# Dummy functions and modules for external dependencies.
def dummy_requests_post(*args, **kwargs):
    class DummyResponse:
        def json(self):
            return {"access_token": "dummy_token"}
    return DummyResponse()


def dummy_from_credentials(url, user, password):
    return "dummy_client"


class DummyLogger:
    def info(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass


def dummy_ingest(
    scicat_client,
    owner_username: str,
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
):
    issues.clear()
    return "dummy_dataset_id"


def test_ingest_dataset_task(mocker: MockFixture):

    return  # Temporarily disable the test.

    # Set environment variables.
    mocker.patch.dict(os.environ, {
        "SCICAT_INGEST_URL": "http://localhost:3000/",
        "SCICAT_INGEST_USER": "test_user",
        "SCICAT_INGEST_PASSWORD": "test_password"
    })

    # Patch external HTTP calls.
    mocker.patch("requests.post", side_effect=dummy_requests_post)

    # Patch pyscicat's from_credentials.
    mocker.patch("orchestration.flows.scicat.ingest.from_credentials", side_effect=dummy_from_credentials)

    # Patch the logger.
    mocker.patch("orchestration.flows.scicat.ingest.get_run_logger", return_value=DummyLogger())

    # Inject dummy ingestor module.
    dummy_ingestor = types.ModuleType("dummy_ingestor")
    dummy_ingestor.ingest = dummy_ingest
    mocker.patch.dict(sys.modules, {"scicat_beamline.ingesters.als_test_ingest": dummy_ingestor})

    # Call the underlying function (.fn) of the task to bypass Prefect orchestration.
    result = scicat_ingest_flow.fn(dataset_path=Path("dummy_file.h5"), ingester_spec="bltest")
    assert result == "dummy_dataset_id"
