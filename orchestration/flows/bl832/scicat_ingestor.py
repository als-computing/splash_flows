import importlib
from logging import getLogger
from typing import List

from pyscicat.client import ScicatClient

from orchestration.flows.bl832.config import Config832
from orchestration.flows.scicat.ingestor_controller import BeamlineIngestorController
from orchestration.flows.scicat.utils import Issue


logger = getLogger(__name__)


class TomographyIngestorController(BeamlineIngestorController):
    """
    Ingestor for Tomo832 beamline.
    """

    def __init__(
        self,
        config: Config832,
        scicat_client: ScicatClient
    ) -> None:
        super().__init__(config, scicat_client)

    def ingest_new_raw_dataset(
        self,
        file_path: str = "",
    ) -> str:
        """
        Ingest a new raw dataset from the Tomo832 beamline.

        :param file_path: Path to the file to ingest.
        :return: SciCat ID of the dataset.
        """

        # Ingest the dataset
        ingestor_module = "orchestration.flows.bl832.ingest_tomo832"
        ingestor_module = importlib.import_module(ingestor_module)
        issues: List[Issue] = []
        new_dataset_id = ingestor_module.ingest(
            self.scicat_client,
            file_path,
            issues,
        )
        if len(issues) > 0:
            logger.error(f"SciCat ingest failed with {len(issues)} issues")
            for issue in issues:
                logger.error(issue)
            raise Exception("SciCat ingest failed")
        return new_dataset_id

    def ingest_new_derived_dataset(
        self,
        file_path: str = "",
        raw_dataset_id: str = "",
    ) -> str:
        """
        Ingest a new derived dataset from the Tomo832 beamline.

        :param file_path: Path to the file to ingest.
        :return: SciCat ID of the dataset.
        """
        pass

    def add_new_dataset_location(
        self,
        dataset_id: str,
        location: str,
    ) -> None:
        """
        Add a new location to an existing dataset in SciCat.

        :param dataset_id: SciCat ID of the dataset.
        :param location: Path to the location to add.
        """

        dataset = self.scicat_client.datasets_get_one(dataset_id)
        dataset["locations"].append(location)
        self.scicat_client.update_dataset(dataset, dataset_id)
        logger.info(f"Added location {location} to dataset {dataset_id}")
        return
