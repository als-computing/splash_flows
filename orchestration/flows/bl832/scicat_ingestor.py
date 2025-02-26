# import importlib
import json
from logging import getLogger
import os
from pathlib import Path
from typing import Any, Dict, List  # Optional, Union

import h5py
from pyscicat.client import ScicatClient
from pyscicat.model import (
    Attachment,
    CreateDatasetOrigDatablockDto,
    Datablock,
    DataFile,
    RawDataset,
    DatasetType,
    Ownable,
)

from orchestration.flows.bl832.config import Config832
from orchestration.flows.scicat.ingestor_controller import BeamlineIngestorController
from orchestration.flows.scicat.utils import (
    build_search_terms,
    build_thumbnail,
    clean_email,
    encode_image_2_thumbnail,
    get_file_size,
    get_file_mod_time,
    Issue,
    NPArrayEncoder,
    Severity
)


logger = getLogger(__name__)


class TomographyIngestorController(BeamlineIngestorController):
    """
    Ingestor for Tomo832 beamline.
    """
    DEFAULT_USER = "8.3.2"  # In case there's not proposal number
    INGEST_SPEC = "als832_dx_3"  # Where is this spec defined?

    DATA_SAMPLE_KEYS = [
        "/measurement/instrument/sample_motor_stack/setup/axis1pos",
        "/measurement/instrument/sample_motor_stack/setup/axis2pos",
        "/measurement/instrument/sample_motor_stack/setup/sample_x",
        "/measurement/instrument/sample_motor_stack/setup/axis5pos",
        "/measurement/instrument/camera_motor_stack/setup/camera_elevation",
        "/measurement/instrument/source/current",
        "/measurement/instrument/camera_motor_stack/setup/camera_distance",
        "/measurement/instrument/source/beam_intensity_incident",
        "/measurement/instrument/monochromator/energy",
        "/measurement/instrument/detector/exposure_time",
        "/measurement/instrument/time_stamp",
        "/measurement/instrument/monochromator/setup/turret2",
        "/measurement/instrument/monochromator/setup/turret1",
    ]

    SCICAT_METADATA_KEYS = [
        "/measurement/instrument/instrument_name",
        "/measurement/sample/experiment/beamline",
        "/measurement/sample/experiment/experiment_lead",
        "/measurement/sample/experiment/pi",
        "/measurement/sample/experiment/proposal",
        "/measurement/sample/experimenter/email",
        "/measurement/sample/experimenter/name",
        "/measurement/sample/file_name",
    ]

    SCIENTIFIC_METADATA_KEYS = [
        "/measurement/instrument/attenuator/setup/filter_y",
        "/measurement/instrument/camera_motor_stack/setup/tilt_motor",
        "/measurement/instrument/detection_system/objective/camera_objective",
        "/measurement/instrument/detection_system/scintillator/scintillator_type",
        "/measurement/instrument/detector/binning_x",
        "/measurement/instrument/detector/binning_y",
        "/measurement/instrument/detector/dark_field_value",
        "/measurement/instrument/detector/delay_time",
        "/measurement/instrument/detector/dimension_x",
        "/measurement/instrument/detector/dimension_y",
        "/measurement/instrument/detector/model",
        "/measurement/instrument/detector/pixel_size",
        "/measurement/instrument/detector/temperature",
        "/measurement/instrument/monochromator/setup/Z2",
        "/measurement/instrument/monochromator/setup/temperature_tc2",
        "/measurement/instrument/monochromator/setup/temperature_tc3",
        "/measurement/instrument/slits/setup/hslits_A_Door",
        "/measurement/instrument/slits/setup/hslits_A_Wall",
        "/measurement/instrument/slits/setup/hslits_center",
        "/measurement/instrument/slits/setup/hslits_size",
        "/measurement/instrument/slits/setup/vslits_Lead_Flag",
        "/measurement/instrument/source/source_name",
        "/process/acquisition/dark_fields/dark_num_avg_of",
        "/process/acquisition/dark_fields/num_dark_fields",
        "/process/acquisition/flat_fields/i0_move_x",
        "/process/acquisition/flat_fields/i0_move_y",
        "/process/acquisition/flat_fields/i0cycle",
        "/process/acquisition/flat_fields/num_flat_fields",
        "/process/acquisition/flat_fields/usebrightexpose",
        "/process/acquisition/mosaic/tile_xmovedist",
        "/process/acquisition/mosaic/tile_xnumimg",
        "/process/acquisition/mosaic/tile_xorig",
        "/process/acquisition/mosaic/tile_xoverlap",
        "/process/acquisition/mosaic/tile_ymovedist",
        "/process/acquisition/mosaic/tile_ynumimg",
        "/process/acquisition/mosaic/tile_yorig",
        "/process/acquisition/mosaic/tile_yoverlap",
        "/process/acquisition/name",
        "/process/acquisition/rotation/blur_limit",
        "/process/acquisition/rotation/blur_limit",
        "/process/acquisition/rotation/multiRev",
        "/process/acquisition/rotation/nhalfCir",
        "/process/acquisition/rotation/num_angles",
        "/process/acquisition/rotation/range",
    ]

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
        Ingest a new raw tomography dataset from the 8.3.2 beamline.

        This method integrates the full ingestion process:
          - Reading and parsing the HDF5 file.
          - Extracting SciCat and scientific metadata.
          - Calculating access controls.
          - Creating and uploading the RawDataset and datablock.
          - Generating and uploading a thumbnail attachment.

        :param file_path: Path to the file to ingest.
        :return: SciCat dataset ID.
        :raises ValueError: If required environment variables are missing.
        :raises Exception: If any issues are encountered during ingestion.
        """
        issues: List[Issue] = []
        logger.setLevel("INFO")

        # Retrieve required environment variables for storage paths
        INGEST_STORAGE_ROOT_PATH = os.getenv("INGEST_STORAGE_ROOT_PATH")
        INGEST_SOURCE_ROOT_PATH = os.getenv("INGEST_SOURCE_ROOT_PATH")
        if not INGEST_STORAGE_ROOT_PATH or not INGEST_SOURCE_ROOT_PATH:
            raise ValueError(
                "INGEST_STORAGE_ROOT_PATH and INGEST_SOURCE_ROOT_PATH must be set"
            )

        file_path_obj = Path(file_path)
        with h5py.File(file_path, "r") as file:
            # Extract metadata from the HDF5 file using beamline-specific keys
            scicat_metadata = self._extract_fields(file, self.SCICAT_METADATA_KEYS, issues)
            scientific_metadata = self._extract_fields(file, self.SCIENTIFIC_METADATA_KEYS, issues)
            scientific_metadata["data_sample"] = self._get_data_sample(file)

            # Encode scientific metadata using NPArrayEncoder
            encoded_scientific_metadata = json.loads(
                json.dumps(scientific_metadata, cls=NPArrayEncoder)
            )

            # Calculate access controls
            access_controls = self._calculate_access_controls(
                self.DEFAULT_USER,
                scicat_metadata.get("/measurement/sample/experiment/beamline"),
                scicat_metadata.get("/measurement/sample/experiment/proposal"),
            )
            logger.info(
                f"Access controls for {file_path_obj} - access_groups: {access_controls.get('access_groups')} "
                f"owner_group: {access_controls.get('owner_group')}"
            )

            ownable = Ownable(
                ownerGroup=access_controls["owner_group"],
                accessGroups=access_controls["access_groups"],
            )

            # Create and upload the raw dataset
            dataset_id = self._upload_raw_dataset(
                file_path_obj,
                scicat_metadata,
                encoded_scientific_metadata,
                ownable,
            )

            # Upload the data block (associated files)
            self._upload_data_block(
                file_path_obj,
                dataset_id,
                INGEST_STORAGE_ROOT_PATH,
                INGEST_SOURCE_ROOT_PATH,
            )

            # Generate and upload a thumbnail attachment
            # The "/exchange/data" key is specific to the Tomo832 HDF5 file structure.
            thumbnail_file = build_thumbnail(file["/exchange/data"][0])
            encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
            self._upload_attachment(
                encoded_thumbnail,
                dataset_id,
                ownable,
            )

        if issues:
            for issue in issues:
                logger.error(issue)
            raise Exception(f"SciCat ingest failed with {len(issues)} issues")
        return dataset_id

    def ingest_new_derived_dataset(
        self,
        folder_path: str = "",
        raw_dataset_id: str = "",
    ) -> str:
        """
        Ingest a new derived dataset from the Tomo832 beamline.

        :param file_path: Path to the file to ingest.
        :return: SciCat ID of the dataset.
        """
        pass

    def _calculate_access_controls(
        username,
        beamline,
        proposal
    ) -> Dict:
        """Calculate access controls for a dataset."""

        # make an access group list that includes the name of the proposal and the name of the beamline
        access_groups = []
        # sometimes the beamline name is super dirty  " '8.3.2', "" '8.3.2', "
        beamline = beamline.replace(" '", "").replace("', ", "") if beamline else None
        # set owner_group to username so that at least someone has access in case no proposal number is found
        owner_group = username
        if beamline:
            access_groups.append(beamline)
            # username lets the user see the Dataset in order to ingest objects after the Dataset
            access_groups.append(username)
            # temporary mapping while beamline controls process request to match beamline name with what comes
            # from ALSHub
            if beamline == "bl832" and "8.3.2" not in access_groups:
                access_groups.append("8.3.2")

        if proposal and proposal != "None":
            owner_group = proposal

        # this is a bit of a kludge. Add 8.3.2 into the access groups so that staff will be able to see it
        return {"owner_group": owner_group, "access_groups": access_groups}

    def _create_data_files(
        self,
        file_path: Path,
        storage_path: str
    ) -> List[DataFile]:
        "Collects all fits files"
        datafiles = []
        datafile = DataFile(
            path=storage_path,
            size=get_file_size(file_path),
            time=get_file_mod_time(file_path),
            type="RawDatasets",
        )
        datafiles.append(datafile)
        return datafiles

    def _extract_fields(
        self,
        file,
        keys,
        issues
    ) -> Dict[str, Any]:
        metadata = {}
        for md_key in keys:
            dataset = file.get(md_key)
            if not dataset:
                issues.append(
                    Issue(msg=f"dataset not found {md_key}", severity=Severity.warning)
                )
                continue
            metadata[md_key] = self._get_dataset_value(file[md_key])
        return metadata

    def _get_dataset_value(
        self,
        data_set
    ):
        """
        Extracts the value of a dataset from an HDF5 file.
        """
        logger.debug(f"{data_set}  {data_set.dtype}")
        try:
            if "S" in data_set.dtype.str:
                if data_set.shape == (1,):
                    return data_set.asstr()[0]
                elif data_set.shape == ():
                    return data_set[()].decode("utf-8")
                else:
                    return list(data_set.asstr())
            else:
                if data_set.maxshape == (1,):
                    logger.debug(f"{data_set}  {data_set[()][0]}")
                    return data_set[()][0]
                else:
                    logger.debug(f"{data_set}  {data_set[()]}")
                    return data_set[()]
        except Exception:
            logger.exception("Exception extracting dataset value")
            return None

    def _get_data_sample(
        self,
        file,
        sample_size=10
    ):
        """ Extracts a sample of the data from the HDF5 file. """
        data_sample = {}
        for key in self.DATA_SAMPLE_KEYS:
            data_array = file.get(key)
            if not data_array:
                continue
            step_size = int(len(data_array) / sample_size)
            if step_size == 0:
                step_size = 1
            sample = data_array[0::step_size]
            data_sample[key] = sample

        return data_sample

    def _upload_data_block(
        self,
        file_path: Path,
        dataset_id: str,
        storage_root_path: str,
        source_root_path: str
    ) -> Datablock:
        "Creates a datablock of files"
        # calculate the path where the file will as known to SciCat
        storage_path = str(file_path).replace(source_root_path, storage_root_path)
        datafiles = self._create_data_files(file_path, storage_path)

        datablock = CreateDatasetOrigDatablockDto(
            size=get_file_size(file_path),
            dataFileList=datafiles
        )
        return self.scicat_client.upload_dataset_origdatablock(dataset_id, datablock)

    def _upload_attachment(
        self,
        encoded_thumnbnail: str,
        dataset_id: str,
        ownable: Ownable,
    ) -> Attachment:
        "Creates a thumbnail png"
        attachment = Attachment(
            datasetId=dataset_id,
            thumbnail=encoded_thumnbnail,
            caption="raw image",
            **ownable.dict(),
        )
        self.scicat_client.upload_attachment(attachment)

    def _upload_raw_dataset(
        self,
        file_path: Path,
        scicat_metadata: Dict,
        scientific_metadata: Dict,
        ownable: Ownable,
    ) -> str:
        """
        Create and upload a new raw dataset to SciCat.

        :param file_path: Path to the file to ingest.
        :param scicat_metadata: SciCat metadata.
        :param scientific_metadata: Scientific metadata.
        :param ownable: Ownable object.
        :return: SciCat ID of the dataset
        """
        file_size = get_file_size(file_path)
        file_mod_time = get_file_mod_time(file_path)
        file_name = scicat_metadata.get("/measurement/sample/file_name")
        description = build_search_terms(file_name)
        appended_keywords = description.split()

        dataset = RawDataset(
            owner=scicat_metadata.get("/measurement/sample/experiment/pi") or "Unknown",
            contactEmail=clean_email(scicat_metadata.get("/measurement/sample/experimenter/email"))
            or "Unknown",
            creationLocation=scicat_metadata.get("/measurement/instrument/instrument_name")
            or "Unknown",
            datasetName=file_name,
            type=DatasetType.raw,
            instrumentId=scicat_metadata.get("/measurement/instrument/instrument_name")
            or "Unknown",
            proposalId=scicat_metadata.get("/measurement/sample/experiment/proposal"),
            dataFormat="DX",
            principalInvestigator=scicat_metadata.get("/measurement/sample/experiment/pi")
            or "Unknown",
            sourceFolder=str(file_path.parent),
            size=file_size,
            scientificMetadata=scientific_metadata,
            sampleId=description,
            isPublished=False,
            description=description,
            keywords=appended_keywords,
            creationTime=file_mod_time,
            **ownable.dict(),
        )
        logger.debug(f"dataset: {dataset}")
        dataset_id = self.scicat_client.upload_new_dataset(dataset)
        return dataset_id
