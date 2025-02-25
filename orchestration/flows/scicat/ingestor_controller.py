from abc import ABC, abstractmethod
from logging import getLogger

from orchestration.config import BeamlineConfig
from pyscicat.client import ScicatClient, from_credentials


logger = getLogger(__name__)


class BeamlineIngestorController(ABC):
    """
    Abstract class for beamline SciCat ingestors.
    Provides interface methods for ingesting data.
    """

    def __init__(
        self,
        config: BeamlineConfig,
        scicat_client: ScicatClient
    ) -> None:
        self.config = config
        self.scicat_client = scicat_client

    def _login_to_scicat(
        self,
        scicat_base_url: str,
        scicat_user: str,
        scicat_password: str
    ) -> ScicatClient:
        scicat_client = from_credentials(
            base_url=scicat_base_url,
            username=scicat_user,
            password=scicat_password
        )
        return scicat_client

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
        dataset_id: str,
        source_folder: str,
        source_folder_host: str,
    ) -> bool:
        """
        Add a new location to an existing dataset in SciCat.

        :param dataset_id:          SciCat ID of the dataset.
        :param source_folder:       "Absolute file path on file server containing the files of this dataset,
                                    e.g. /some/path/to/sourcefolder. In case of a single file dataset, e.g. HDF5 data,
                                    it contains the path up to, but excluding the filename. Trailing slashes are removed.",

        :param source_folder_host: "DNS host name of file server hosting sourceFolder,
                                    optionally including a protocol e.g. [protocol://]fileserver1.example.com",

        """
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
