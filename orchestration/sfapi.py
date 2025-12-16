from dotenv import load_dotenv
import json
import logging
import os
from pathlib import Path

from authlib.jose import JsonWebKey
from sfapi_client import Client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


# TODO: we need a better way to store the client_id and client_secret
def create_sfapi_client(
    client_id_path: str = os.getenv("PATH_NERSC_CLIENT_ID"),
    client_secret_path: str = os.getenv("PATH_NERSC_PRI_KEY"),
) -> Client:
    """Create and return an NERSC client instance"""

    # When generating the SFAPI Key in Iris, make sure to select "asldev" as the user!
    # Otherwise, the key will not have the necessary permissions to access the data.

    if not client_id_path or not client_secret_path:
        logger.error("NERSC credentials paths are missing.")
        raise ValueError("NERSC credentials paths are missing.")
    if not Path(client_id_path).is_file() or not Path(client_secret_path).is_file():
        raise FileNotFoundError("NERSC credential files are missing.")

    client_id = Path(client_id_path).read_text(encoding="utf-8").strip()
    secret_text = Path(client_secret_path).read_text(encoding="utf-8")
    client_secret = JsonWebKey.import_key(json.loads(secret_text))

    try:
        client = Client(client_id, client_secret)
        logger.info("NERSC client created successfully.")
        return client
    except Exception as e:
        logger.error(f"Failed to create NERSC client: {e}")
        raise e
