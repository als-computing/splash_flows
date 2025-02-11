from dotenv import load_dotenv
import json
import logging
import os

from authlib.jose import JsonWebKey
from sfapi_client import Client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


def create_sfapi_client(
    client_id_path: str,
    client_secret_path: str,
) -> Client:
    """Create and return an NERSC client instance"""

    # When generating the SFAPI Key in Iris, make sure to select "asldev" as the user!
    # Otherwise, the key will not have the necessary permissions to access the data.
    # client_id_path = os.getenv("PATH_NERSC_CLIENT_ID")
    # client_secret_path = os.getenv("PATH_NERSC_PRI_KEY")

    if not client_id_path or not client_secret_path:
        logger.error("NERSC credentials paths are missing.")
        raise ValueError("Missing NERSC credentials paths.")
    if not os.path.isfile(client_id_path) or not os.path.isfile(client_secret_path):
        logger.error("NERSC credential files are missing.")
        raise FileNotFoundError("NERSC credential files are missing.")

    client_id = None
    client_secret = None
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
        raise e
