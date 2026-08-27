"""Tests for orchestration/jobs/nersc/login.py.

Patch targets (nersc/login.py has NO Variable or Secret imports):
  orchestration.jobs.nersc.login._create_sfapi_client   (private builder)
  orchestration.jobs.nersc.login._create_iriapi_client  (private builder)

The private builders are patched, not tested directly — they are thin wrappers
around third-party constructors with extensive credential I/O (sfapi_client.Client
__init__, env vars, file reads). Patching them is sufficient to exercise
create_nersc_client's dispatch logic, which is the only thing this module adds.
That choice is intentional.
"""

from orchestration.jobs.nersc.login import NERSCLoginMethod, create_nersc_client


# ── Structural identity check ─────────────────────────────────────────────────

def test_login_method_is_canonical():
    """NERSCLoginMethod is defined exactly once; bl832 re-export is the same object.

    Uses `is`, not `==` — equality passes even for two separate enum classes
    whose members have matching names and values.
    """
    from orchestration.jobs.nersc.login import NERSCLoginMethod as A
    from orchestration.flows.bl832.job_controller import NERSCLoginMethod as B
    assert A is B


# ── NERSCLoginMethod enum ─────────────────────────────────────────────────────

class TestNERSCLoginMethod:
    def test_sfapi_value(self):
        assert NERSCLoginMethod.SFAPI.value == "sfapi"

    def test_iriapi_value(self):
        assert NERSCLoginMethod.IRIAPI.value == "iriapi"

    def test_membership_sfapi(self):
        assert NERSCLoginMethod("sfapi") is NERSCLoginMethod.SFAPI

    def test_membership_iriapi(self):
        assert NERSCLoginMethod("iriapi") is NERSCLoginMethod.IRIAPI


# ── create_nersc_client dispatch ──────────────────────────────────────────────

class TestCreateNerscClient:
    def test_sfapi_dispatches_to_sfapi_builder(self, mocker, mock_config):
        mock_client = mocker.MagicMock()
        builder = mocker.patch(
            "orchestration.jobs.nersc.login._create_sfapi_client",
            return_value=mock_client,
        )
        result = create_nersc_client(mock_config, NERSCLoginMethod.SFAPI)
        builder.assert_called_once_with()
        assert result is mock_client

    def test_iriapi_dispatches_to_iriapi_builder(self, mocker, mock_config):
        mock_client = mocker.MagicMock()
        builder = mocker.patch(
            "orchestration.jobs.nersc.login._create_iriapi_client",
            return_value=mock_client,
        )
        result = create_nersc_client(mock_config, NERSCLoginMethod.IRIAPI)
        builder.assert_called_once_with(mock_config.nersc_resources["iri"]["api_base_url"])
        assert result is mock_client

    def test_sfapi_passes_api_base_url_from_config(self, mocker, mock_config):
        mocker.patch("orchestration.jobs.nersc.login._create_sfapi_client")
        # No assertion on URL for SFAPI (the builder doesn't take a URL arg),
        # but create_nersc_client must read the sfapi sub-dict without raising.
        create_nersc_client(mock_config, NERSCLoginMethod.SFAPI)

    def test_iriapi_passes_correct_api_base_url(self, mocker, mock_config):
        builder = mocker.patch("orchestration.jobs.nersc.login._create_iriapi_client")
        create_nersc_client(mock_config, NERSCLoginMethod.IRIAPI)
        builder.assert_called_once_with("https://mock-iri.nersc.gov")

    def test_default_login_method_is_iriapi(self, mocker, mock_config):
        builder = mocker.patch("orchestration.jobs.nersc.login._create_iriapi_client")
        create_nersc_client(mock_config)
        builder.assert_called_once()
