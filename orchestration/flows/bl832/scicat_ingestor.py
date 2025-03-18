# import importlib
import json
from logging import getLogger
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import h5py
from pyscicat.client import ScicatClient
from pyscicat.model import (
    Attachment,
    CreateDatasetOrigDatablockDto,
    Datablock,
    DataFile,
    DerivedDataset,
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
        scicat_client: Optional[ScicatClient] = None
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

        This method handles ingestion of derived datasets generated during tomography reconstruction:
        1. A directory of TIFF slices
        2. A Zarr directory (derived from the TIFFs)

        :param folder_path: Path to the folder containing the derived data.
        :param raw_dataset_id: ID of the raw dataset this derived data is based on.
        :return: SciCat ID of the derived dataset.
        :raises ValueError: If required environment variables are missing.
        :raises Exception: If any issues are encountered during ingestion.
        """
        issues: List[Issue] = []
        logger.setLevel("INFO")

        logger.info(f"Ingesting derived dataset from folder: {folder_path}")
        # Retrieve required environment variables for storage paths
        INGEST_STORAGE_ROOT_PATH = os.getenv("INGEST_STORAGE_ROOT_PATH")
        INGEST_SOURCE_ROOT_PATH = os.getenv("INGEST_SOURCE_ROOT_PATH")
        if not INGEST_STORAGE_ROOT_PATH or not INGEST_SOURCE_ROOT_PATH:
            raise ValueError(
                "INGEST_STORAGE_ROOT_PATH and INGEST_SOURCE_ROOT_PATH must be set"
            )

        logger.info("Getting raw dataset from SciCat to link to")
        # Get the raw dataset to link to
        try:
            raw_dataset = self.scicat_client.datasets_get_one(raw_dataset_id)
            logger.info(f"Found raw dataset to link: {raw_dataset_id}")
        except Exception as e:
            raise ValueError(f"Failed to find raw dataset with ID {raw_dataset_id}: {e}")

        folder_path_obj = Path(folder_path)
        if not folder_path_obj.exists():
            raise ValueError(f"Folder path does not exist: {folder_path}")

        logger.info(raw_dataset)
        # Calculate access controls - use the same as the raw dataset
        access_controls = {
            "owner_group": raw_dataset["ownerGroup"],
            "access_groups": raw_dataset["accessGroups"]
        }
        logger.info(f"Using access controls from raw dataset: {access_controls}")

        ownable = Ownable(
            ownerGroup=access_controls["owner_group"],
            accessGroups=access_controls["access_groups"],
        )

        # Get main HDF5 file if exists, otherwise use first file
        main_file = None
        for file in folder_path_obj.glob("*.h5"):
            main_file = file
            break

        if not main_file:
            # If no HDF5 file, use the first file in the directory
            for file in folder_path_obj.iterdir():
                if file.is_file():
                    main_file = file
                    break
            if not main_file:
                raise ValueError(f"No files found in directory: {folder_path}")

        # Extract scientific metadata
        scientific_metadata = {
            "derived_from": raw_dataset_id,
            "processing_date": get_file_mod_time(main_file),
        }

        # Try to extract metadata from HDF5 file if available
        if main_file and main_file.suffix.lower() == ".h5":
            try:
                with h5py.File(main_file, "r") as file:
                    scientific_metadata.update(
                        self._extract_fields(file, self.SCIENTIFIC_METADATA_KEYS, issues)
                    )
            except Exception as e:
                logger.warning(f"Could not extract metadata from HDF5 file: {e}")

        # Encode scientific metadata using NPArrayEncoder
        encoded_scientific_metadata = json.loads(
            json.dumps(scientific_metadata, cls=NPArrayEncoder)
        )

        # Create and upload the derived dataset

        # Use folder name as dataset name if nothing better
        dataset_name = folder_path_obj.name

        # Determine if this is a TIFF directory or a Zarr directory
        is_zarr = dataset_name.endswith('.zarr')
        data_format = "Zarr" if is_zarr else "TIFF"

        # Build description/keywords from the folder name
        description = build_search_terms(dataset_name)
        keywords = description.split()

        # Add additional descriptive information
        if is_zarr:
            description = f"Multi-resolution Zarr dataset derived from reconstructed tomography slices: {description}"
            keywords.extend(["zarr", "multi-resolution", "volume"])
        else:
            description = f"Reconstructed tomography slices: {description}"
            keywords.extend(["tiff", "slices", "reconstruction"])

        # Create the derived dataset
        dataset = DerivedDataset(
            owner=raw_dataset.get("owner"),
            contactEmail=raw_dataset.get("contactEmail"),
            creationLocation=raw_dataset.get("creationLocation"),
            datasetName=dataset_name,
            type=DatasetType.derived,
            proposalId=raw_dataset.get("proposalId"),
            dataFormat=data_format,
            principalInvestigator=raw_dataset.get("principalInvestigator"),
            sourceFolder=str(folder_path_obj),
            size=sum(f.stat().st_size for f in folder_path_obj.glob("**/*") if f.is_file()),
            scientificMetadata=encoded_scientific_metadata,
            sampleId=description,
            isPublished=False,
            description=description,
            keywords=keywords,
            creationTime=get_file_mod_time(folder_path_obj),
            investigator=raw_dataset.get("owner"),
            inputDatasets=[raw_dataset_id],
            usedSoftware=["TomoPy", "Zarr"] if is_zarr else ["TomoPy"],
            jobParameters={"source_folder": str(folder_path_obj)},
            **ownable.dict(),
        )
        # Upload the derived dataset
        dataset_id = self.scicat_client.upload_new_dataset(dataset)
        logger.info(f"Created derived dataset with ID: {dataset_id}")

        # Upload datablock for all files in the directory
        total_size = 0
        datafiles = []

        for file_path in folder_path_obj.glob("**/*"):
            if file_path.is_file():
                storage_path = str(file_path).replace(INGEST_SOURCE_ROOT_PATH, INGEST_STORAGE_ROOT_PATH)
                datafile = DataFile(
                    path=storage_path,
                    size=get_file_size(file_path),
                    time=get_file_mod_time(file_path),
                    type="DerivedDatasets",
                )
                datafiles.append(datafile)
                total_size += datafile.size

        # Upload the datablock
        datablock = CreateDatasetOrigDatablockDto(
            size=total_size,
            dataFileList=datafiles,
            datasetId=dataset_id,
            **ownable.dict(),
        )
        self.scicat_client.upload_dataset_origdatablock(dataset_id, datablock)
        logger.info(f"Uploaded datablock with {len(datafiles)} files")

        # Try to generate and upload a thumbnail if possible
        try:
            if is_zarr:
                # For Zarr, generate the thumbnail in memory
                thumb_buffer = self._generate_zarr_thumbnail(folder_path_obj)
                if thumb_buffer:
                    encoded_thumbnail = encode_image_2_thumbnail(thumb_buffer)
                    self._upload_attachment(encoded_thumbnail, dataset_id, ownable)
                    logger.info("Uploaded thumbnail for Zarr dataset")
            elif main_file and main_file.suffix.lower() == ".h5":
                with h5py.File(main_file, "r") as file:
                    # Try to find a suitable dataset for thumbnail
                    for key in ["/exchange/data", "/data", "/reconstruction"]:
                        if key in file:
                            thumbnail_file = build_thumbnail(file[key][0])
                            encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
                            self._upload_attachment(
                                encoded_thumbnail,
                                dataset_id,
                                ownable,
                            )
                            logger.info("Uploaded thumbnail for derived dataset")
                            break
            else:
                # For TIFF files, use a middle slice as thumbnail
                tiff_files = sorted(list(folder_path_obj.glob("*.tiff"))) + sorted(list(folder_path_obj.glob("*.tif")))
                if tiff_files:
                    # Use a slice from the middle of the volume for the thumbnail
                    middle_slice = tiff_files[len(tiff_files) // 2]
                    from PIL import Image
                    import io
                    import numpy as np
                    image = Image.open(middle_slice)
                    # Convert image to a numpy array
                    arr = np.array(image, dtype=np.float32)

                    # Compute min and max; if they are equal, use a default scaling to avoid division by zero.
                    arr_min = np.min(arr)
                    arr_max = np.max(arr)
                    if arr_max == arr_min:
                        # In case of no contrast, simply use a zeros array or leave the image unchanged.
                        arr_scaled = np.zeros(arr.shape, dtype=np.uint8)
                    else:
                        # Normalize the array to 0-255
                        arr_scaled = ((arr - arr_min) / (arr_max - arr_min) * 255).astype(np.uint8)

                    # Create a new image from the scaled array
                    scaled_image = Image.fromarray(arr_scaled)
                    thumbnail_buffer = io.BytesIO()
                    scaled_image.save(thumbnail_buffer, format="PNG")
                    thumbnail_buffer.seek(0)
                    encoded_thumbnail = encode_image_2_thumbnail(thumbnail_buffer)
                    self._upload_attachment(encoded_thumbnail, dataset_id, ownable)

                    logger.info("Uploaded thumbnail from TIFF slice")
        except Exception as e:
            logger.warning(f"Failed to generate thumbnail: {e}")

        if issues:
            for issue in issues:
                logger.error(issue)
            raise Exception(f"SciCat derived dataset ingest failed with {len(issues)} issues")

        return dataset_id

    def _generate_zarr_thumbnail(self, zarr_path: Path):
        """
        Generate a thumbnail image from an NGFF Zarr dataset using ngff_zarr.
        This implementation extracts a mid-slice and returns a BytesIO buffer.

        :param zarr_path: Path to the Zarr directory.
        :return: A BytesIO object containing the PNG image data, or None on failure.
        """
        try:
            import ngff_zarr as nz
            from PIL import Image
            import numpy as np
            import io

            # Load the multiscale image from the Zarr store
            multiscales = nz.from_ngff_zarr(str(zarr_path))
            # Here we assume a specific scale index (e.g. 3) and take the mid-slice along the first dimension
            # Adjust this index as needed for your dataset.
            image = multiscales.images[3].data
            middle_index = image.shape[0] // 2
            mid_slice = image[middle_index, :, :]
            # Ensure we have a NumPy array
            mid_slice = mid_slice.compute() if hasattr(mid_slice, "compute") else np.array(mid_slice)

            # Normalize the image to 8-bit
            mid_slice = mid_slice.astype(np.float32)
            dmin, dmax = np.min(mid_slice), np.max(mid_slice)
            if dmax != dmin:
                norm_array = ((mid_slice - dmin) / (dmax - dmin) * 255).astype(np.uint8)
            else:
                norm_array = np.zeros_like(mid_slice, dtype=np.uint8)

            # Create a PIL image from the normalized array
            img = Image.fromarray(norm_array)
            if img.mode == "F":
                img = img.convert("L")

            # Save the image to an in-memory bytes buffer
            buffer = io.BytesIO()
            img.save(buffer, format="PNG")
            buffer.seek(0)
            return buffer
        except ImportError:
            logger.warning("ngff_zarr package is not installed. Install it with `pip install ngff_zarr`.")
            return None
        except Exception as e:
            logger.warning(f"Failed to generate Zarr thumbnail using ngff_zarr: {e}")
            return None

    def _calculate_access_controls(
        self,
        username,
        beamline,
        proposal
    ) -> Dict:
        """
        Calculate access controls for a dataset.
        """

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
        """
        Collects all fits files
        """
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
        """
        Creates a datablock of files
        """
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


if __name__ == "__main__":

    config = Config832()
    file_path = "/Users/david/Documents/data/tomo/raw/20241216_153047_ddd.h5"
    proposal_id = "test832"
    ingestor = TomographyIngestorController(config)

    # login_to_scicat assumes that the environment variables are set in the environment
    # in this test, just using the scicatlive (3.2.5) backend defaults (admin user)

    logger.info("Setting up metadata SciCat ingestion")

    ingestor.login_to_scicat(
        scicat_base_url="http://localhost:3000/api/v3/",
        scicat_user="admin",
        scicat_password="2jf70TPNZsS"
    )

    # INGEST_STORAGE_ROOT_PATH and INGEST_SOURCE_ROOT_PATH must be set
    os.environ["INGEST_STORAGE_ROOT_PATH"] = "/global/cfs/cdirs/als/data_mover/8.3.2"
    os.environ["INGEST_SOURCE_ROOT_PATH"] = "/data832-raw"

    logger.info(f"Ingesting {file_path}")
    id = ingestor.ingest_new_raw_dataset(file_path)
    logger.info(f"Ingested with SciCat ID: {id}")

    # logger.info(f"Testing SciCat ID lookup after ingestion based on {file_path}")
    # try:
    #     # Test lookup based on filename after ingestion
    #     id = ingestor._find_dataset(file_name=file_path)
    #     logger.info(f"Found dataset id {id}")
    # except Exception as e:
    #     logger.error(f"Failed to find dataset {e}")

    # Pretend we moved to tape:
    # /home/a/alsdev/data_mover/[beamline]/raw/[proposal_name]/[proposal_name]_[year]-[cycle].tar
    ingestor.add_new_dataset_location(
        dataset_id=id,
        proposal_id=proposal_id,
        file_name="20241216_153047_ddd.h5",
        source_folder=f"/home/a/alsdev/data_mover/{config.beamline_id}/raw/{proposal_id}/{proposal_id}_2024-12.tar",
        source_folder_host="HPSS",
    )

    # ingestor needs to add the derived dataset ingestion method

    # ingestor needs to add new "origdatablock" method for raw data on different filesystems
    # ingestor needs to add new "datablock" method for raw data on HPSS system
    # same for derived data

    ingestor.ingest_new_derived_dataset(
        folder_path="/Users/david/Documents/data/tomo/scratch/"
        "rec20230606_152011_jong-seto_fungal-mycelia_flat-AQ_fungi2_fast.zarr",
        raw_dataset_id=id
    )
    ingestor.ingest_new_derived_dataset(
        folder_path="/Users/david/Documents/data/tomo/scratch/rec20230224_132553_sea_shell",
        raw_dataset_id=id
    )

    admin_ingestor = TomographyIngestorController(config)
    admin_ingestor.login_to_scicat(
        scicat_base_url="http://localhost:3000/api/v3/",
        scicat_user="archiveManager",
        scicat_password="aman"
    )
    admin_ingestor.remove_dataset_location(
        dataset_id=id,
        source_folder_host="HPSS",
    )
