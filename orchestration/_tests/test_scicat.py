import json
import numpy as np
import os
import sys
import types
from typing import List
from pytest_mock import MockFixture

from orchestration.flows.scicat.utils import NPArrayEncoder, build_search_terms, calculate_access_controls, Issue
from orchestration.flows.scicat.ingest import ingest_dataset_task

from orchestration.flows.bl832.ingest_tomo832 import clean_email, UNKNOWN_EMAIL


def test_clean_email_valid():
    # Remove surrounding whitespace.
    assert clean_email("  user@example.com  ") == "user@example.com"


def test_clean_email_none():
    # Non-string input returns the default unknown email.
    assert clean_email(None) == UNKNOWN_EMAIL


def test_clean_email_empty():
    # An empty string (or only spaces) should return the default.
    assert clean_email("   ") == UNKNOWN_EMAIL


def test_clean_email_no_at_symbol():
    # A string without an "@" should return the default.
    assert clean_email("invalid-email") == UNKNOWN_EMAIL


def test_clean_email_literal_none():
    # The string "NONE" (case-insensitive) should return the default.
    assert clean_email("NONE") == UNKNOWN_EMAIL
    assert clean_email("none") == UNKNOWN_EMAIL


def test_clean_email_internal_spaces():
    # Spaces inside the email should be removed.
    # For example, " user @ example.com " should be cleaned to "user@example.com"
    assert clean_email(" user @ example.com ") == "user@example.com"


def add_mock_requests(mock_request):
    mock_request.post("http://localhost:3000/api/v3/Users/login", json={"id": "foobar"})
    mock_request.post("http://localhost:3000/api/v3/Samples", json={"sampleId": "dataset_id"})
    mock_request.post("http://localhost:3000/api/v3/RawDatasets/replaceOrCreate", json={"pid": "42"})
    mock_request.post("http://localhost:3000/api/v3/RawDatasets/42/origdatablocks", json={"response": "random"})


def test_np_encoder():
    test_dict = {"dont_panic": np.array([1, 2, 3], dtype=np.int8)}
    assert json.dumps(test_dict, cls=NPArrayEncoder)

    test_dict = {"dont_panic": np.array([1, 2, 3], dtype=np.float32)}
    assert json.dumps(test_dict, cls=NPArrayEncoder)

    test_dict = {"dont_panic": np.full((1, 1), np.inf)}
    encoded_np = json.loads(json.dumps(test_dict, cls=NPArrayEncoder))
    # requests doesn't allow strings  that have np.inf or np.nan
    # so the NPArrayEncoder needs to return both as None
    assert json.dumps(encoded_np, allow_nan=False)


def test_build_search_terms():
    terms = build_search_terms("Time-is_an illusion. Lunchtime/2x\\so.")
    assert "time" in terms
    assert "is" in terms
    assert "an" in terms
    assert "illusion" in terms
    assert "lunchtime" in terms
    assert "2x" in terms
    assert "so" in terms


def test_access_controls():
    username = "slartibartfast"
    # no proposal, no beamline
    access_controls = calculate_access_controls(username, None, None)
    assert access_controls["owner_group"] == "slartibartfast"
    assert access_controls["access_groups"] == []

    # proposal and no beamline
    access_controls = calculate_access_controls(username, None, "42")
    assert access_controls["owner_group"] == "42"

    # no proposal and beamline
    access_controls = calculate_access_controls(username, "10.3.1", None)
    assert access_controls["owner_group"] == "slartibartfast"
    assert "10.3.1" in access_controls["access_groups"]
    assert "slartibartfast" in access_controls["access_groups"]

    # proposal and beamline
    access_controls = calculate_access_controls(username, "10.3.1", "42")
    assert access_controls["owner_group"] == "42"
    assert "10.3.1" in access_controls["access_groups"]

    # special 8.3.2 mapping
    access_controls = calculate_access_controls(username, "bl832", "42")
    assert access_controls["owner_group"] == "42"
    assert "8.3.2" in access_controls["access_groups"]
    assert "bl832" in access_controls["access_groups"]


def test_clean_email():
    pass


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


def dummy_ingest(scicat_client, file_path, issues: List[Issue]):
    issues.clear()
    return "dummy_dataset_id"


def test_ingest_dataset_task(mocker: MockFixture):
    # Set environment variables.
    mocker.patch.dict(os.environ, {
        "SCICAT_API_URL": "http://localhost:3000/",
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
    mocker.patch.dict(sys.modules, {"orchestration.flows.bl832.ingest_tomo832": dummy_ingestor})

    # Call the underlying function (.fn) of the task to bypass Prefect orchestration.
    result = ingest_dataset_task.fn("dummy_file.h5", "orchestration.flows.bl832.ingest_tomo832")
    assert result == "dummy_dataset_id"
