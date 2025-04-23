"""
End-to-end tests for transfer, prune, and ingest controllers.
These tests are designed to be as generic as possible and should work with any beamline configuration.

"""

from datetime import timedelta, datetime
import logging
import os
import shutil
from typing import Optional

from pyscicat.client import ScicatClient

from orchestration.config import BeamlineConfig
from orchestration.flows.scicat.ingestor_controller import BeamlineIngestorController
from orchestration.globus import flows, transfer
from orchestration.globus.transfer import GlobusEndpoint
from orchestration.hpss import cfs_to_hpss_flow, hpss_to_cfs_flow
from orchestration.prune_controller import get_prune_controller, PruneMethod
from orchestration.transfer_controller import get_transfer_controller, CopyMethod
from orchestration.transfer_endpoints import FileSystemEndpoint, HPSSEndpoint
from globus_sdk import TransferClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ----------------------------------------------------------------------------------------------------------------------
# Setup Environment Configuration and Test Classes
# ----------------------------------------------------------------------------------------------------------------------


def check_required_envvars() -> bool:
    """
    Check for required environment variables before running the end-to-end tests.
    """
    missing_vars = []

    # Check Globus environment variables
    globus_vars = ['GLOBUS_CLIENT_ID', 'GLOBUS_CLIENT_SECRET']
    for var in globus_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    # Check Prefect environment variables
    prefect_vars = ['PREFECT_API_URL', 'PREFECT_API_KEY']
    for var in prefect_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    # Check SciCat environment variables
    scicat_vars = ['SCICAT_API_URL', 'SCICAT_INGEST_USER', 'SCICAT_INGEST_PASSWORD']
    for var in scicat_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    # TODO: Add SFAPI Keys check

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables before running the end-to-end tests.")
        return False

    logger.info("All required environment variables are set.")
    return True


class TestConfig(BeamlineConfig):
    """
    Test configuration class for a beamline
    """
    def __init__(self) -> None:
        super().__init__(beamline_id="0.0.0")

    def _beam_specific_config(self) -> None:
        self.endpoints = transfer.build_endpoints(self.config)
        self.apps = transfer.build_apps(self.config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.flow_client = flows.get_flows_client()
        self.nersc_alsdev = self.endpoints["nersc_alsdev"]  # root: /global/homes/a/alsdev/test_directory/
        self.hpss_alsdev = self.config["hpss_alsdev"]
        self.scicat = self.config["scicat"]


class TestIngestorController(BeamlineIngestorController):
    """
    Test ingestor controller class for SciCat that does very basic ingest operations.

    Works with scicatlive v3.2.5
    https://github.com/SciCatProject/scicatlive
    """
    def __init__(
        self,
        config: TestConfig,
        scicat_client: Optional[ScicatClient] = None
    ) -> None:
        super().__init__(config, scicat_client)

    def ingest_new_raw_dataset(
        self,
        file_path: str = "",
    ) -> str:
        """
        Create a minimal raw dataset in SciCat for the given file.

        Args:
            file_path: Path to the file to ingest.

        Returns:
            str: The SciCat ID of the created dataset.
        """
        if not self.scicat_client:
            logger.error("SciCat client not initialized. Call login_to_scicat first.")
            raise ValueError("SciCat client not initialized. Call login_to_scicat first.")

        # Create minimal metadata for the dataset
        from pyscicat.model import CreateDatasetOrigDatablockDto, DataFile, RawDataset

        filename = os.path.basename(file_path)
        basename = os.path.splitext(filename)[0]

        logger.info(f"Creating raw dataset for {filename}")

        try:
            # Create a RawDataset object directly with parameters
            # Making sure to include principalInvestigator as a string
            dataset = RawDataset(
                owner="ingestor",
                contactEmail="test@example.com",
                creationLocation=f"/test/location/{basename}",
                sourceFolder="/test/source/folder",
                datasetName=basename,
                type="raw",
                proposalId="test-proposal",
                description=f"Test dataset for {filename}",
                ownerGroup="admin",
                accessGroups=["admin"],
                creationTime=datetime.now().isoformat(),
                principalInvestigator="Test Investigator"  # Add this required field
            )

            # Upload dataset to SciCat
            dataset_id = self.scicat_client.upload_new_dataset(dataset)

            logger.info(f"Created raw dataset with ID: {dataset_id}")

            # Add a dummy file to the datablock
            dummy_file = DataFile(
                path=file_path,
                size=1024,  # Dummy size
                time=datetime.now().isoformat()
            )

            datablock = CreateDatasetOrigDatablockDto(
                size=1024,  # Dummy size
                dataFileList=[dummy_file]
            )

            # Attach the datablock to the dataset
            self.scicat_client.upload_dataset_origdatablock(dataset_id, datablock)
            logger.info(f"Added datablock to dataset {dataset_id}")

            return dataset_id

        except Exception as e:
            logger.error(f"Error creating raw dataset: {e}")
            raise e

    def ingest_new_derived_dataset(
        self,
        file_path: str = "",
        raw_dataset_id: str = "",
    ) -> str:
        """
        Create a minimal derived dataset in SciCat for the given file,
        linked to the provided raw dataset.

        Args:
            file_path: Path to the file to ingest.
            raw_dataset_id: ID of the parent raw dataset.

        Returns:
            str: The SciCat ID of the created dataset.
        """
        if not self.scicat_client:
            logger.error("SciCat client not initialized. Call login_to_scicat first.")
            raise ValueError("SciCat client not initialized. Call login_to_scicat first.")

        # Create minimal metadata for the dataset
        from pyscicat.model import CreateDatasetOrigDatablockDto, DataFile, DerivedDataset

        filename = os.path.basename(file_path)
        basename = os.path.splitext(filename)[0]

        logger.info(f"Creating derived dataset for {filename} from {raw_dataset_id}")

        try:
            # Create a DerivedDataset object
            derived_dataset = DerivedDataset(
                owner="ingestor",
                contactEmail="test@example.com",
                creationLocation=f"/test/location/{basename}_derived",
                sourceFolder="/test/source/folder",
                datasetName=f"{basename}_derived",
                type="derived",
                proposalId="test-proposal",
                description=f"Derived dataset from {raw_dataset_id}",
                ownerGroup="admin",
                accessGroups=["admin"],
                creationTime=datetime.now().isoformat(),
                investigator="test-investigator",
                inputDatasets=[raw_dataset_id],
                principalInvestigator="Test Investigator",
                usedSoftware=["TestSoftware"]
            )

            # Upload the dataset to SciCat
            dataset_id = self.scicat_client.upload_new_dataset(derived_dataset)

            logger.info(f"Created derived dataset with ID: {dataset_id}")

            # Add a dummy file to the datablock
            dummy_file = DataFile(
                path=file_path,
                size=1024,  # Dummy size
                time=datetime.now().isoformat()
            )

            datablock = CreateDatasetOrigDatablockDto(
                size=1024,  # Dummy size
                dataFileList=[dummy_file]
            )

            # Attach the datablock to the dataset
            self.scicat_client.upload_dataset_origdatablock(dataset_id, datablock)
            logger.info(f"Added datablock to dataset {dataset_id}")

            return dataset_id

        except Exception as e:
            logger.error(f"Error creating derived dataset: {e}")
            raise e

    # TODO: Add methods to add and remove dataset locations


# ----------------------------------------------------------------------------------------------------------------------
# End-to-end Tests
# ----------------------------------------------------------------------------------------------------------------------


def test_transfer_controllers(
    file_path: str,
    test_globus: bool,
    test_filesystem: bool,
    test_hpss: bool,
    config: BeamlineConfig,
) -> None:
    """
    Test the transfer controller by transferring a file to each endpoint.

    Args:
        file_path (str): The path to the file to transfer.
        test_globus (bool): Whether to test the Globus transfer controller.
        test_filesystem (bool): Whether to test the FileSystem transfer controller.
        test_hpss (bool): Whether to test the HPSS transfer controller.
        config (BeamlineConfig): The beamline configuration.

    Returns:
        None
    """
    logger.info("Testing transfer controllers...")
    logger.info(f"File path: {file_path}")
    logger.info(f"Test Globus: {test_globus}")
    logger.info(f"Test Filesystem: {test_filesystem}")
    logger.info(f"Test HPSS: {test_hpss}")

    if test_globus:
        # Create a transfer controller for Globus transfers
        globus_transfer_controller = get_transfer_controller(
            transfer_type=CopyMethod.GLOBUS,
            config=config
        )

        # Configure the source and destination endpoints
        # Use the NERSC alsdev endpoint with root_path: /global/homes/a/alsdev/test_directory/source/ as the source
        source_endpoint = GlobusEndpoint(
            uuid=config.nersc_alsdev.uuid,
            uri=config.nersc_alsdev.uri,
            root_path=config.nersc_alsdev.root_path + "source/",
            name="source_endpoint"
        )

        # Use the NERSC alsdev endpoint with root_path: /global/homes/a/alsdev/test_directory/destination/ as the destination
        destination_endpoint = GlobusEndpoint(
            uuid=config.nersc_alsdev.uuid,
            uri=config.nersc_alsdev.uri,
            root_path=config.nersc_alsdev.root_path + "destination/",
            name="destination_endpoint"
        )

        globus_transfer_controller.copy(
            file_path=file_path,
            source=source_endpoint,
            destination=destination_endpoint,
        )

    if test_filesystem:
        # Create a transfer controller for filesystem transfers
        filesystem_transfer_controller = get_transfer_controller(
            transfer_type=CopyMethod.SIMPLE,
            config=config
        )

        # Configure the source and destination endpoints

        # Create temporary directories for testing in current working directory
        base_test_dir = os.path.join(os.getcwd(), "orchestration_test_dir")
        source_dir = os.path.join(base_test_dir, "source")
        dest_dir = os.path.join(base_test_dir, "destination")

        # Create directories
        os.makedirs(source_dir, exist_ok=True)
        os.makedirs(dest_dir, exist_ok=True)

        # Create a test file in the source directory
        test_file_path = os.path.join(source_dir, file_path)
        with open(test_file_path, "w") as f:
            f.write("This is a test file for SimpleTransferController")

        logger.info(f"Created test file at {test_file_path}")

        # Use the defined FileSystemEndpoint
        source_endpoint = FileSystemEndpoint(
            name="source_endpoint",
            root_path=source_dir,
            uri="source.test"
        )
        destination_endpoint = FileSystemEndpoint(
            name="destination_endpoint",
            root_path=dest_dir,
            uri="destination.test"
        )

        result = filesystem_transfer_controller.copy(
            file_path=file_path,
            source=source_endpoint,
            destination=destination_endpoint,
        )

        # Verify the transfer
        dest_file_path = os.path.join(dest_dir, file_path)
        if os.path.exists(dest_file_path):
            logger.info(f"File successfully transferred to {dest_file_path}")
        else:
            logger.error(f"Transfer failed: file not found at {dest_file_path}")

        assert result is True, "Transfer operation returned False"
        assert os.path.exists(dest_file_path), "File wasn't copied to destination"

    if test_hpss:
        from orchestration.flows.bl832.config import Config832

        config = Config832()
        project_name = "BLS-00520_dyparkinson"
        source = FileSystemEndpoint(
            name="CFS",
            root_path="/global/cfs/cdirs/als/data_mover/8.3.2/raw/",
            uri="nersc.gov"
        )
        destination = HPSSEndpoint(
            name="HPSS",
            root_path=config.hpss_alsdev["root_path"],
            uri=config.hpss_alsdev["uri"]
        )
        success = cfs_to_hpss_flow(
            file_path=project_name,
            source=source,
            destination=destination,
            config=config
        )
        logger.info(f"Transfer success: {success}")
        config = Config832()
        relative_file_path = f"{config.beamline_id}/raw/BLS-00520_dyparkinson/BLS-00520_dyparkinson_2022-2.tar"
        source = HPSSEndpoint(
            name="HPSS",
            root_path=config.hpss_alsdev["root_path"],  # root_path: /home/a/alsdev/data_mover
            uri=config.hpss_alsdev["uri"]
        )
        destination = FileSystemEndpoint(
            name="CFS",
            root_path="/global/cfs/cdirs/als/data_mover/8.3.2/retrieved_from_tape",
            uri="nersc.gov"
        )

        files_to_extract = [
            "20221028_101514_arun_JSC-1.h5",
            "20220923_160531_ethan_robin_climbing-vine_x00y05.h5",
            "20221222_082548_strangpresse_20pCFABS_800rpm_Non-vacuum.h5",
            "20220923_160531_ethan_robin_climbing-vine_x00y04.h5"
        ]

        hpss_to_cfs_flow(
            file_path=f"{relative_file_path}",
            source=source,
            destination=destination,
            files_to_extract=files_to_extract,
            config=config
        )


def test_prune_controllers(
    file_path: str,
    test_globus: bool,
    test_filesystem: bool,
    test_hpss: bool,
    config: BeamlineConfig,
) -> None:
    """
    Test the prune controllers by pruning files from each endpoint.

    Note: not pruning the source endpoint test.txt file, so it can be used in future tests.

    Args:
        file_path (str): Path to the file to prune.
        test_globus (bool): Whether to test the Globus pruner.
        test_filesystem (bool): Whether to test the filesystem pruner.
        test_hpss (bool): Whether to test the HPSS pruner.
        config (BeamlineConfig): Configuration object for the beam

    Returns:
        None
    """
    logger.info("Testing prune controllers...")
    logger.info(f"File path: {file_path}")
    logger.info(f"Test Globus: {test_globus}")
    logger.info(f"Test Filesystem: {test_filesystem}")
    logger.info(f"Test HPSS: {test_hpss}")

    if test_globus:
        globus_prune_controller = get_prune_controller(
            prune_type=PruneMethod.GLOBUS,
            config=config
        )

        # PRUNE FROM SOURCE ENDPOINT
        # Configure the source and destination endpoints
        # Use the NERSC alsdev endpoint with root_path: /global/homes/a/alsdev/test_directory/source/ as the source
        # source_endpoint = GlobusEndpoint(
        #     uuid=config.nersc_alsdev.uuid,
        #     uri=config.nersc_alsdev.uri,
        #     root_path=config.nersc_alsdev.root_path + "source/",
        #     name="source_endpoint"
        # )

        # Prune the source endpoint
        # globus_prune_controller.prune(
        #     file_path=file_path,
        #     source_endpoint=source_endpoint,
        #     check_endpoint=None,
        #     days_from_now=timedelta(days=0)
        # )

        # PRUNE FROM DESTINATION ENDPOINT
        # Use the NERSC alsdev endpoint with root_path: /global/homes/a/alsdev/test_directory/destination/ as the destination
        destination_endpoint = GlobusEndpoint(
            uuid=config.nersc_alsdev.uuid,
            uri=config.nersc_alsdev.uri,
            root_path=config.nersc_alsdev.root_path + "destination/",
            name="destination_endpoint"
        )

        # Assume files were created and transferred in the previous test
        # Prune the destination endpoint
        globus_prune_controller.prune(
            file_path=file_path,
            source_endpoint=destination_endpoint,
            check_endpoint=None,
            days_from_now=timedelta(days=0)
        )

    if test_filesystem:
        filesystem_prune_controller = get_prune_controller(
            prune_type=PruneMethod.SIMPLE,
            config=config
        )

        # Configure the source and destination endpoints to match the TransferController test
        # Create temporary directories for testing
        base_test_dir = os.path.join(os.getcwd(), "orchestration_test_dir")
        source_dir = os.path.join(base_test_dir, "source")
        dest_dir = os.path.join(base_test_dir, "destination")

        # Use the defined FileSystemEndpoint
        source_endpoint = FileSystemEndpoint(
            name="source_endpoint",
            root_path=source_dir,
            uri="source.test"
        )

        destination_endpoint = FileSystemEndpoint(
            name="destination_endpoint",
            root_path=dest_dir,
            uri="destination.test"
        )

        # Assume files were created and transferred in the previous test

        # Prune the source endpoint
        filesystem_prune_controller.prune(
            file_path=file_path,
            source_endpoint=source_endpoint,
            check_endpoint=None,
            days_from_now=timedelta(days=0)
        )

        # Prune the destination endpoint
        filesystem_prune_controller.prune(
            file_path=file_path,
            source_endpoint=destination_endpoint,
            check_endpoint=None,
            days_from_now=timedelta(days=0)
        )

        # After pruning in the filesystem pruner test
        source_file_path = os.path.join(source_dir, file_path)
        assert not os.path.exists(source_file_path), "File wasn't removed from source"
        if not os.path.exists(source_file_path):
            logger.info(f"File successfully deleted from source: {source_file_path}")
        dest_file_path = os.path.join(dest_dir, file_path)
        assert not os.path.exists(dest_file_path), "File wasn't removed from destination"
        if not os.path.exists(dest_file_path):
            logger.info(f"File successfully deleted from destination: {dest_file_path}")

    if test_hpss:
        hpss_prune_controller = get_prune_controller(
            prune_type=PruneMethod.HPSS,
            config=config
        )

        hpss_prune_controller.prune()
        # TODO: Finish this test


def test_scicat_ingest(
    file_path: str = "test.txt"

) -> None:
    """
    Test the SciCat ingestor controller by ingesting a file.
    """
    config = TestConfig()
    test_ingestor = TestIngestorController(config)

    # Login to SciCat, assuming credentials are saved in environment variables
    # If not, and you are testing with scicatlive, use these defaults:
    # SCICAT_API_URL="http://localhost:3000/api/v3/"
    # SCICAT_INGEST_USER="admin"
    # SCICAT_INGEST_PASSWORD="2jf70TPNZsS"

    test_ingestor.login_to_scicat(
        scicat_base_url=os.getenv("SCICAT_API_URL"),
        scicat_user=os.getenv("SCICAT_INGEST_USER"),
        scicat_password=os.getenv("SCICAT_INGEST_PASSWORD")
    )

    raw_id = test_ingestor.ingest_new_raw_dataset(
        file_path=file_path
    )

    test_ingestor.ingest_new_derived_dataset(
        file_path=file_path,
        raw_dataset_id=raw_id
    )

    test_ingestor.add_new_dataset_location(
        dataset_id=raw_id,
        source_folder="test_folder",
        source_folder_host="test_host"
    )

    # This will probably fail. Need to figure out default scicatlive user permissions.
    test_ingestor.remove_dataset_location(
        dataset_id=raw_id,
        source_folder_host="test_host"
    )


def test_it_all(
    test_globus: bool = True,
    test_filesystem: bool = False,
    test_hpss: bool = False,
    test_scicat: bool = False
) -> None:
    """
    Run end-to-end tests for transfer and prune controllers."
    """
    try:
        check_required_envvars()
    except Exception as e:
        logger.error(f"Error checking environment variables: {e}")
        return
    finally:
        logger.info("Continuing with tests...")

    config = TestConfig()

    try:
        test_transfer_controllers(
            file_path="test.txt",
            test_globus=test_globus,
            test_filesystem=test_filesystem,
            test_hpss=test_hpss,
            config=config
        )
        logger.info("Transfer controller tests passed.")
    except Exception as e:
        logger.error(f"Error running transfer controller tests: {e}")
        return

    if test_scicat:
        try:
            test_scicat_ingest(
                file_path="test.txt"
            )
        except Exception as e:
            logger.error(f"Error running SciCat ingestor tests: {e}")
            return

    try:
        test_prune_controllers(
            file_path="test.txt",
            test_globus=test_globus,
            test_filesystem=test_filesystem,
            test_hpss=test_hpss,
            config=config
        )
        logger.info("Prune controller tests passed.")
    except Exception as e:
        logger.error(f"Error running prune controller tests: {e}")
        return

    logger.info("All tests passed. Cleaning up...")
    base_test_dir = os.path.join(os.getcwd(), "orchestration_test_dir")
    if os.path.exists(base_test_dir):
        shutil.rmtree(base_test_dir)


if __name__ == "__main__":
    test_it_all(
        test_globus=False,
        test_filesystem=False,
        test_hpss=True,
        test_scicat=False
    )
