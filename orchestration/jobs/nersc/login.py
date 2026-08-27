# orchestration/jobs/nersc/login.py

"""NERSC API authentication and client construction.

Two login methods are supported, each backed by a different credential model
and pointed at a different API base URL:

- :attr:`NERSCLoginMethod.SFAPI`: Iris-registered OAuth2 client ID + private
  key (NERSC OIDC flow). Reads ``PATH_NERSC_CLIENT_ID`` and
  ``PATH_NERSC_PRI_KEY`` from the environment.
- :attr:`NERSCLoginMethod.IRIAPI`: Globus bearer token written by
  ``orchestration/globus/get_globus_token.py``. Reads ``PATH_GLOBUS_TOKEN_FILE``
  from the environment, falling back to ``~/.globus/auth_tokens.json``.

The base URLs for each method live in the beamline config under
``nersc_resources.{iri,sfapi}.api_base_url``.
"""

from enum import Enum
import json
import logging
import os
from pathlib import Path

from authlib.jose import JsonWebKey
from dotenv import load_dotenv
import httpx
from sfapi_client import Client

from orchestration.config import BeamlineConfig
from orchestration.globus.get_globus_token import (
    DEFAULT_TOKEN_FILE,
    get_iri_access_token,
)

logger = logging.getLogger(__name__)
load_dotenv()

# Env var pointing at the cached Globus token file used by IRIAPI auth.
_IRIAPI_TOKEN_FILE_ENV: str = "PATH_GLOBUS_TOKEN_FILE"


class NERSCLoginMethod(Enum):
    """Selects which NERSC API to authenticate against.

    Each method has its own credentials and API base URL — see module docstring.
    """

    SFAPI = "sfapi"
    """Standard Superfacility API via Iris-registered OAuth2 credentials."""

    IRIAPI = "iriapi"
    """Integrated Research Infrastructure API via Globus bearer token."""


def create_nersc_client(
    config: BeamlineConfig,
    login_method: NERSCLoginMethod = NERSCLoginMethod.IRIAPI,
) -> Client | httpx.Client:
    """Create and return a NERSC client for the requested login method.

    Reads the API base URL from ``config.nersc_resources[login_method.value]``,
    then delegates to the appropriate underscored builder.

    Args:
        config: Beamline config instance. Must expose ``nersc_resources`` with
            ``"iri"`` and ``"sfapi"`` sub-dicts each containing ``api_base_url``.
        login_method: Which NERSC API to authenticate against. Defaults to
            :attr:`NERSCLoginMethod.IRIAPI`.

    Returns:
        An authenticated client — :class:`sfapi_client.Client` for SFAPI,
        :class:`httpx.Client` for IRIAPI.

    Raises:
        ValueError: If SFAPI credential env vars are unset, or if
            ``login_method`` is not a recognized member.
        FileNotFoundError: If SFAPI credential files are absent.
        RuntimeError: If the Globus token is expired or missing required scopes.
    """
    logger.info(f"Creating NERSC client using login method: {login_method.value}")

    if login_method is NERSCLoginMethod.SFAPI:
        api_base_url = config.nersc_resources["sfapi"]["api_base_url"]
        client = _create_sfapi_client()
    elif login_method is NERSCLoginMethod.IRIAPI:
        api_base_url = config.nersc_resources["iri"]["api_base_url"]
        client = _create_iriapi_client(api_base_url)
    else:
        raise ValueError(f"Unhandled NERSCLoginMethod: {login_method}")

    logger.info(
        f"NERSC client created successfully "
        f"(method={login_method.value}, api_url={api_base_url})."
    )
    return client


def _create_iriapi_client(api_base_url: str) -> httpx.Client:
    """Create a NERSC IRI API client using a Globus bearer token.

    Reuses a cached token if valid; otherwise mints a new one via the client
    credentials grant. No browser or user interaction.

    Args:
        api_base_url: Base URL for the NERSC IRI API.

    Returns:
        An authenticated :class:`httpx.Client` targeting the IRI API.

    Raises:
        ValueError: If ``GLOBUS_CLIENT_ID`` or ``GLOBUS_CLIENT_SECRET`` are unset.
        RuntimeError: If the acquired token is missing required scopes.
    """
    token_file_env = os.getenv(_IRIAPI_TOKEN_FILE_ENV)
    token_file = Path(token_file_env) if token_file_env else DEFAULT_TOKEN_FILE

    access_token = get_iri_access_token(
        token_file=token_file,
        force_login=False,
        prompt_login=False,
    )

    return httpx.Client(
        base_url=api_base_url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=10.0),
    )


def _create_sfapi_client() -> Client:
    """Create a NERSC SFAPI client from Iris-registered OAuth2 credentials.

    Reads the client ID and private key paths from ``PATH_NERSC_CLIENT_ID``
    and ``PATH_NERSC_PRI_KEY``. When generating the SFAPI key in Iris, the
    "asldev" user must be selected so the key has the necessary data-access
    permissions.

    Returns:
        An authenticated :class:`sfapi_client.Client`.

    Raises:
        ValueError: If the credential env vars are unset.
        FileNotFoundError: If the credential files are absent.
    """
    client_id_path = os.getenv("PATH_NERSC_CLIENT_ID")
    client_secret_path = os.getenv("PATH_NERSC_PRI_KEY")

    if not client_id_path or not client_secret_path:
        logger.error("NERSC credentials paths are missing.")
        raise ValueError("Missing NERSC credentials paths.")
    if not os.path.isfile(client_id_path) or not os.path.isfile(client_secret_path):
        logger.error("NERSC credential files are missing.")
        raise FileNotFoundError("NERSC credential files are missing.")

    with open(client_id_path, "r") as f:
        client_id = f.read()
    with open(client_secret_path, "r") as f:
        client_secret = JsonWebKey.import_key(json.loads(f.read()))

    try:
        client = Client(client_id, client_secret)
        logger.info("NERSC client created successfully.")
        return client
    except Exception as e:
        logger.error(f"Failed to create NERSC client: {e}")
        raise
