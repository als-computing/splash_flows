import types

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def prefect_test_fixture():
    """Wrap the entire test_jobs/ session in a Prefect test harness.

    Required because ALCFJobController.__init__ calls Variable.get(_sync=True),
    which needs a live Prefect API — even when Variable.get is patched, the
    import-time Prefect setup must succeed.
    """
    with prefect_test_harness():
        yield


@pytest.fixture
def mock_config():
    """Minimal BeamlineConfig-like namespace for tests that need a config object."""
    return types.SimpleNamespace(
        nersc_resources={
            "iri": {
                "api_base_url": "https://mock-iri.nersc.gov",
                "perlmutter_login": "mock-login-uuid",
                "perlmutter_job_submit": "mock-submit-uuid",
                "compute_resource": "mock-compute-uuid",
            },
            "sfapi": {"api_base_url": "https://mock-sfapi.nersc.gov"},
        },
        mlflow={"tracking_uri": "http://mock-mlflow:5000"},
    )


@pytest.fixture
def mock_sfapi_client(mocker):
    """MagicMock shaped like an sfapi_client.Client."""
    client = mocker.MagicMock()
    user = mocker.MagicMock()
    user.name = "testuser"
    client.user.return_value = user
    return client


@pytest.fixture
def mock_iriapi_client(mocker):
    """MagicMock shaped like an httpx.Client targeting the IRI API."""
    client = mocker.MagicMock()
    client.post.return_value = mocker.MagicMock(
        is_success=True, json=lambda: {"id": "job-42"}
    )
    client.get.return_value = mocker.MagicMock(
        is_success=True,
        json=lambda: {"status": {"state": "completed"}},
    )
    return client


@pytest.fixture
def mock_alcf_prefect(mocker):
    """Patch Variable.get and Secret.load in the ALCF controller module.

    allocation_data is a real dict, not a MagicMock — the constructor calls
    allocation_data.get("alcf-allocation-root-path") and checks truthiness of
    the result. A MagicMock would pass the check with an arbitrary truthy value,
    masking bugs.

    Tests that need Prefect mocked request this fixture explicitly.
    """
    allocation_data = {"alcf-allocation-root-path": "/eagle/IRIProd/ALS"}
    var_mock = mocker.patch(
        "orchestration.jobs.alcf.controller.Variable.get",
        return_value=allocation_data,
    )
    secret_mock = mocker.patch("orchestration.jobs.alcf.controller.Secret.load")
    secret_mock.return_value.get.return_value = "mock-endpoint-uuid"
    return types.SimpleNamespace(variable=var_mock, secret=secret_mock)


@pytest.fixture
def mock_options_prefect(mocker):
    """Patch Variable.get in the options module (used by load_job_options tests)."""
    return mocker.patch(
        "orchestration.jobs.options.Variable.get",
        return_value={"defaults": True},
    )
