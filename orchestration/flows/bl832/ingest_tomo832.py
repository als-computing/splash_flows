from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
from typing import Any, Dict, List

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

from orchestration.flows.scicat.utils import (
    build_search_terms,
    build_thumbnail,
    calculate_access_controls,
    encode_image_2_thumbnail,
    Issue,
    NPArrayEncoder,
    Severity
)

DEFAULT_USER = "8.3.2"  # In case there's not proposal number
UNKNOWN_EMAIL = "unknown@example.com"
ingest_spec = "als832_dx_4"  # "als832_dx_3"

logger = logging.getLogger("scicat_ingest")


def ingest(
    scicat_client: ScicatClient,
    file_path: str,
    issues: List[Issue],
    log_level: str = "INFO",
) -> str:
    """Ingests a file into scicat

    Ingestion to takes a "best effort" stance to ingestion. Along the way,
    many things can go wrong. Rather than failing the entire ingestion, we
    collect issues and return them to the caller. These issues are updated
    in the input issues list.

    Parameters
    ----------
    scicat_client : ScicatClient
        client to talk to the scicat server
    file_path : str
        Path to find the file to ingest
    issues : List[Issue]
        Issues where problems are recorded

    Returns
    -------
    str
        Dataset id of the new
    """
    logger.setLevel(log_level)
    INGEST_STORAGE_ROOT_PATH = os.getenv("INGEST_STORAGE_ROOT_PATH")
    INGEST_SOURCE_ROOT_PATH = os.getenv("INGEST_SOURCE_ROOT_PATH")

    if not INGEST_SOURCE_ROOT_PATH or not INGEST_SOURCE_ROOT_PATH:
        raise ValueError(
            "INGEST_STORAGE_ROOT_PATH and INGEST_SOURCE_ROOT_PATH must be set"
        )

    with h5py.File(file_path, "r") as file:
        file_path = Path(file_path)
        scicat_metadata = _extract_fields(file, scicat_metadata_keys, issues)
        scientific_metadata = _extract_fields(file, scientific_metadata_keys, issues)
        scientific_metadata["data_sample"] = _get_data_sample(file)
        encoded_scientific_metadata = json.loads(
            json.dumps(scientific_metadata, cls=NPArrayEncoder)
        )
        access_controls = calculate_access_controls(
            DEFAULT_USER,
            scicat_metadata.get("/measurement/sample/experiment/beamline"),
            scicat_metadata.get("/measurement/sample/experiment/proposal"),
        )
        logger.info(
            f"Access controls for  {file_path}  access_groups: {access_controls.get('accessroups')} "
            f"owner_group: {access_controls.get('owner_group')}"
        )

        ownable = Ownable(
            ownerGroup=access_controls["owner_group"],
            accessGroups=access_controls["access_groups"],
        )
        dataset_id = upload_raw_dataset(
            scicat_client,
            file_path,
            scicat_metadata,
            encoded_scientific_metadata,
            ownable,
        )
        upload_data_block(
            scicat_client,
            file_path,
            dataset_id,
            INGEST_STORAGE_ROOT_PATH,
            INGEST_SOURCE_ROOT_PATH
        )

        thumbnail_file = build_thumbnail(file["/exchange/data"][0])
        encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
        upload_attachment(
            scicat_client,
            encoded_thumbnail,
            dataset_id,
            ownable
        )

        return dataset_id


def upload_raw_dataset(
    scicat_client: ScicatClient,
    file_path: Path,
    scicat_metadata: Dict,
    scientific_metadata: Dict,
    ownable: Ownable,
) -> str:
    "Creates a dataset object"
    file_size = get_file_size(file_path)
    file_mod_time = get_file_mod_time(file_path)
    file_name = scicat_metadata.get("/measurement/sample/file_name")
    description = build_search_terms(file_name)
    appended_keywords = description.split()
    logger.info(f"email: {scicat_metadata.get('/measurement/sample/experimenter/email')}")
    dataset = RawDataset(
        owner=scicat_metadata.get("/measurement/sample/experiment/pi") or "Unknown",
        contactEmail=clean_email(scicat_metadata.get("/measurement/sample/experimenter/email"))
        or "unknown@example.com",
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
    dataset_id = scicat_client.upload_new_dataset(dataset)
    return dataset_id


def create_data_files(file_path: Path, storage_path: str) -> List[DataFile]:
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


def upload_data_block(
    scicat_client: ScicatClient,
    file_path: Path,
    dataset_id: str,
    storage_root_path: str,
    source_root_path: str
) -> Datablock:
    "Creates a datablock of files"
    # calculate the path where the file will as known to SciCat
    storage_path = str(file_path).replace(source_root_path, storage_root_path)
    datafiles = create_data_files(file_path, storage_path)

    datablock = CreateDatasetOrigDatablockDto(
        size=get_file_size(file_path),
        dataFileList=datafiles
    )
    return scicat_client.upload_dataset_origdatablock(dataset_id, datablock)


def upload_attachment(
    scicat_client: ScicatClient,
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
    scicat_client.upload_attachment(attachment)


def get_file_size(file_path: Path) -> int:
    return file_path.lstat().st_size


def get_file_mod_time(file_path: Path) -> str:
    return datetime.fromtimestamp(file_path.lstat().st_mtime).isoformat()


def _extract_fields(file, keys, issues) -> Dict[str, Any]:
    metadata = {}
    for md_key in keys:
        dataset = file.get(md_key)
        if not dataset:
            issues.append(
                Issue(msg=f"dataset not found {md_key}", severity=Severity.warning)
            )
            continue
        metadata[md_key] = _get_dataset_value(file[md_key])
    return metadata


def _get_dataset_value(data_set):
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


def _get_data_sample(file, sample_size=10):
    data_sample = {}
    for key in data_sample_keys:
        data_array = file.get(key)
        if not data_array:
            continue
        step_size = int(len(data_array) / sample_size)
        if step_size == 0:
            step_size = 1
        sample = data_array[0::step_size]
        data_sample[key] = sample

    return data_sample


# TODO: Move clean_email() to the scicat_beamline repository
# https://github.com/als-computing/scicat_beamline
def clean_email(email: any) -> str:
    """
    Clean the provided email address.

    This function ensures that the input is a valid email address.
    It returns a default email if:
      - The input is not a string,
      - The input is empty after stripping,
      - The input equals "NONE" (case-insensitive), or
      - The input does not contain an "@" symbol.

    Parameters
    ----------
    email : any
        The raw email value extracted from metadata.

    Returns
    -------
    str
        A cleaned email address if valid, otherwise the default unknown email.

    Example
    -------
    >>> clean_email("  user@example.com  ")
    'user@example.com'
    >>> clean_email("garbage")
    'unknown@example.com'
    >>> clean_email(None)
    'unknown@example.com'
    """
    # Check that the email is a string
    if not isinstance(email, str):
        logger.info(f"Input email is not a string. Returning {UNKNOWN_EMAIL}")
        return UNKNOWN_EMAIL

    # Remove surrounding whitespace
    cleaned = email.strip()

    # Remove leading/trailing quotes, commas, and whitespace
    cleaned = re.sub(r'^["\'\s,]+|["\'\s,]+$', '', email)

    # Fallback if the email is empty, equals "NONE", or lacks an "@" symbol
    if not cleaned or cleaned.upper() == "NONE" or "@" not in cleaned:
        logger.info(f"Invalid email address. Returning {UNKNOWN_EMAIL}")
        return UNKNOWN_EMAIL

    # Optionally, remove spaces from inside the email (typically invalid in an email address)
    cleaned = cleaned.replace(" ", "")

    # Final verification: ensure that the cleaned email contains "@".
    if "@" not in cleaned:
        logger.info(f"Invalid email address: {cleaned}. Returning {UNKNOWN_EMAIL}")
        return UNKNOWN_EMAIL

    return cleaned


scicat_metadata_keys = [
    "/measurement/instrument/instrument_name",
    "/measurement/sample/experiment/beamline",
    "/measurement/sample/experiment/experiment_lead",
    "/measurement/sample/experiment/pi",
    "/measurement/sample/experiment/proposal",
    "/measurement/sample/experimenter/email",
    "/measurement/sample/experimenter/name",
    "/measurement/sample/file_name",
]

scientific_metadata_keys = [
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
    # NOTE: These are commented out because they are no longer present in the h5 file as of March 25, 2025
    # Keeping them commented out in case they are needed in the future
    # "/measurement/instrument/monochromator/setup/temperature_tc2",
    # "/measurement/instrument/monochromator/setup/temperature_tc3",
    # "/measurement/instrument/slits/setup/hslits_A_Door",
    # "/measurement/instrument/slits/setup/hslits_A_Wall",
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

data_sample_keys = [
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


def test_ingest_raw_tomo() -> bool:
    from orchestration.flows.scicat.ingest import ingest_dataset
    TOMO_INGESTOR_MODULE = "orchestration.flows.bl832.ingest_tomo832"
    file_path = "examples/tomo_scan_no_email.h5"
    print(f"Ingesting {file_path} with {TOMO_INGESTOR_MODULE}")
    try:
        ingest_dataset(file_path, TOMO_INGESTOR_MODULE)
        return True
    except Exception as e:
        print(f"SciCat ingest failed with {e}")
        return False


if __name__ == "__main__":
    # ingest(
    #     ScicatClient(
    #         # "http://localhost:3000/api/v3",
    #         os.environ.get("SCICAT_API_URL"),
    #         None,
    #         os.environ.get("SCICAT_INGEST_USER"),
    #         os.environ.get("SCICAT_INGEST_PASSWORD"),
    #     ),
    #     "/Users/dylanmcreynolds/data/beamlines/8.3.2/raw/"
    #     "20231013_065251_MSB_Book1_Proj77_Cell3_Gen2_Li_R2G_FastCharge_DuringCharge0.h5",
    #     [],
    #     log_level="DEBUG",
    # )

    test_ingest_raw_tomo()
