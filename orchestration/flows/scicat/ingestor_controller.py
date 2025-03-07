from abc import ABC, abstractmethod
import logging
from logging import getLogger
import os
import requests
from typing import Optional

from pyscicat.client import ScicatClient, from_credentials

from orchestration.config import BeamlineConfig


logging.basicConfig(level=logging.INFO)
logger = getLogger(__name__)


class BeamlineIngestorController(ABC):
    """
    Abstract class for beamline SciCat ingestors.
    Provides interface methods for ingesting data.
    """

    def __init__(
        self,
        config: BeamlineConfig,
        scicat_client: Optional[ScicatClient] = None
    ) -> None:
        self.config = config
        self.scicat_client = scicat_client

    def login_to_scicat(
        self,
        scicat_base_url: Optional[str] = None,
        scicat_user: Optional[str] = None,
        scicat_password: Optional[str] = None
    ) -> ScicatClient:
        """
        Log in to SciCat using the provided credentials.

        :param scicat_base_url: Base URL of the SciCat instance. Defaults to the environment variable 'SCICAT_API_URL'.
        :param scicat_user: Username for the SciCat instance. Defaults to the environment variable 'SCICAT_INGEST_USER'.
        :param scicat_password: Password for the SciCat instance. Defaults to the environment variable 'SCICAT_INGEST_PASSWORD'
        :return: An instance of ScicatClient with an authenticated session.
        :raises ValueError: If any required credentials are missing.
        """
        # Use environment variables as defaults if parameters are not provided.
        scicat_base_url = scicat_base_url or os.getenv("SCICAT_API_URL")
        scicat_user = scicat_user or os.getenv("SCICAT_INGEST_USER")
        scicat_password = scicat_password or os.getenv("SCICAT_INGEST_PASSWORD")

        # Ensure that all required credentials are provided.
        if not (scicat_base_url and scicat_user and scicat_password):
            raise ValueError(
                "Missing required SciCat credentials. Provide scicat_base_url, scicat_user, "
                "and scicat_password as parameters or set them in the environment variables: "
                "SCICAT_API_URL, SCICAT_INGEST_USER, SCICAT_INGEST_PASSWORD."
            )

        # Try to log in using the pyscicat client first.
        # This method seems deprecated, but leaving it here for backwards compatability
        # https://github.com/SciCatProject/pyscicat/issues/61
        try:
            self.scicat_client = from_credentials(
                base_url=scicat_base_url,
                username=scicat_user,
                password=scicat_password
            )
            logger.info("Logged in to SciCat.")
            return self.scicat_client
        except Exception as e:
            logger.error(f"Failed to log in to SciCat: {e}, trying alternative method.")

        # This method works for scicatlive 3.2.5
        try:
            response = requests.post(
                url=scicat_base_url,
                json={"username": scicat_user, "password": scicat_password},
                stream=False,
                verify=True,
            )
            self.scicat_client = ScicatClient(scicat_base_url, response.json()["access_token"])
            logger.info("Logged in to SciCat.")
            # logger.info(f"SciCat token: {response.json()['access_token']}")
            return self.scicat_client

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to log in to SciCat: {e}")
            raise e
        except Exception as e:
            logger.error(f"Failed to log in to SciCat: {e}")
            raise e

    @abstractmethod
    def ingest_new_raw_dataset(
        self,
        file_path: str = "",
    ) -> str:
        """Ingest data from the beamline.

        :param file_path: Path to the file to ingest.
        :return: SciCat ID of the dataset.
        """
        pass

    @abstractmethod
    def ingest_new_derived_dataset(
        self,
        file_path: str = "",
        raw_dataset_id: str = "",
    ) -> str:
        """Ingest data from the beamline.

        :param file_path: Path to the file to ingest.
        :return: SciCat ID of the dataset.
        """
        pass

    def add_new_dataset_location(
        self,
        dataset_id: Optional[str] = None,
        proposal_id: Optional[str] = None,
        file_name: Optional[str] = None,
        source_folder: str = None,
        source_folder_host: str = None
    ) -> str:
        """
        Add a new location to an existing dataset in SciCat.

        :param dataset_id:          SciCat ID of the dataset.
        :param source_folder:       "Absolute file path on file server containing the files of this dataset,
                                    e.g. /some/path/to/sourcefolder. In case of a single file dataset, e.g. HDF5 data,
                                    it contains the path up to, but excluding the filename. Trailing slashes are removed.",

        :param source_folder_host: "DNS host name of file server hosting sourceFolder,
                                    optionally including a protocol e.g. [protocol://]fileserver1.example.com",

        """
        # If dataset_id is not provided, we need to find it using proposal_id and file_name.
        # Otherwise, we use the provided dataset_id directly.
        if dataset_id is None and proposal_id and file_name:
            dataset_id = self._find_dataset(proposal_id=proposal_id, file_name=file_name)

        dataset = self.scicat_client.datasets_get_one(dataset_id)

        # sourceFolder sourceFolderHost are each a string
        dataset["sourceFolder"] = source_folder
        dataset["sourceFolderHost"] = source_folder_host
        self.scicat_client.datasets_update(dataset, dataset_id)
        logger.info(f"Added location {source_folder} to dataset {dataset_id}")
        return dataset_id

    def remove_dataset_location(
        self,
        dataset_id: str = "",
        source: str = "",
    ) -> bool:
        """

        """
        pass

    def _find_dataset(
        self,
        proposal_id: Optional[str] = None,  # The ALS proposal ID, not the SciCat ID
        file_name: Optional[str] = None
    ) -> str:
        """
        Find a dataset in SciCat and return the ID based on proposal ID and file name.
        This method is used when a dataset ID is not provided.
        If more than one dataset is found, an error is raised, and the user is advised to check the logs.
        If no dataset is found, an error is raised.
        If exactly one dataset is found, its ID is returned.
        This method is intended to be used internally within the class.

        Parameters:
            self,
            proposal_id (Optional[str]): The proposal identifier used in ingestion.
            file_name (Optional[str]): The dataset name (derived from file name).

        Raises:
            ValueError: If insufficient search parameters are provided,
                        no dataset is found, or multiple datasets match.
        """
        # Require both search terms if no dataset_id is given.
        if not (proposal_id and file_name):
            raise ValueError("Either a dataset ID must be provided or both proposal_id and file_name must be given.")

        query_fields = {
            "proposalId": proposal_id,
            "datasetName": file_name
        }
        results = self.scicat_client.datasets_find(query_fields=query_fields)
        count = results.get("count", 0)

        if count == 0:
            raise ValueError(f"No dataset found for proposal '{proposal_id}' with name '{file_name}'.")
        elif count > 1:
            # Log all found dataset IDs for human review.
            dataset_ids = [d.get("pid", "N/A") for d in results["data"]]
            logger.error(
                f"Multiple datasets found for proposal '{proposal_id}' with name '{file_name}': {dataset_ids}. Please verify."
            )
            raise ValueError(
                f"Multiple datasets found for proposal '{proposal_id}' with name '{file_name}'. See log for details."
            )
        dataset = results["data"][0]
        dataset_id = dataset.get("pid")
        if not dataset_id:
            raise ValueError("The dataset returned does not have a valid 'pid' field.")

        return dataset_id


# Concrete implementation for testing and instantiation.
class ConcreteBeamlineIngestorController(BeamlineIngestorController):
    def ingest_new_raw_dataset(self, file_path: str = "") -> str:
        # Dummy implementation for testing.
        return "raw_dataset_id_dummy"

    def ingest_new_derived_dataset(self, file_path: str = "", raw_dataset_id: str = "") -> str:
        # Dummy implementation for testing.
        return "derived_dataset_id_dummy"


if __name__ == "__main__":
    logger.info("Testing SciCat ingestor controller")
    test_ingestor = ConcreteBeamlineIngestorController(BeamlineConfig)
    test_ingestor.login_to_scicat(
        scicat_base_url="http://localhost:3000/api/v3/auth/login",
        scicat_user="ingestor",
        scicat_password="aman"
    )
