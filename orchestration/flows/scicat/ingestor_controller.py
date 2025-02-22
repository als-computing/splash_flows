from abc import ABC, abstractmethod

from orchestration.config import BeamlineConfig
from pyscicat.client import ScicatClient, from_credentials


class BeamlineIngestorController(ABC):
    """
    Abstract class for beamline ingestors.
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

    @abstractmethod
    def add_new_dataset_location(
        self,
        dataset_id: str = "",
        destination: str = "",
    ) -> bool:
        """

        """
        pass

    @abstractmethod
    def remove_dataset_location(
        self,
        dataset_id: str = "",
        source: str = "",
    ) -> bool:
        """

        """
        pass
