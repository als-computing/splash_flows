from pathlib import Path
from typing import Dict, Any

from prefect import flow, get_run_logger

from scicat_beamline import ingest


@flow(name="scicat-ingest-flow")
def scicat_ingest_flow(
    dataset_path: Path | list[Path],
    ingester_spec: str | None = None,
    owner_username: str | None = None,
    scicat_url: str | None = None,
    scicat_username: str | None = None,
    scicat_password: str | None = None,
    datasettracker_url: str | None = None,
    datasettracker_username: str | None = None,
    datasettracker_password: str | None = None,
) -> Dict[str, Any]:
    """
    Runs the SciCat ingestion process implemented for the given spec identifier,
    on the given folder or file.

    :param dataset_path: Path or list of Paths of the asset(s) to ingest. May be file or directory depending on the spec.
        If SICAT_INGEST_INTERNAL_BASE_FOLDER or SCICAT_INGEST_BASE_FOLDER is set, this path is
        considered relative to that base folder.

    These remaining args are optional; if not provided, environment variables will be used:

    :param ingester_spec: Spec to ingest with. (or set SCICAT_INGEST_INGESTER_SPEC)
    :param owner_username: User doing the ingesting. May be different from the user_name, especially if using a token
                        (or set SCICAT_INGEST_OWNER_USERNAME)
    :param scicat_url: Scicat server base url. If not provided, will try localhost default (or set SCICAT_INGEST_URL)
    :param scicat_username: Scicat server username (or set SCICAT_INGEST_USERNAME)
    :param scicat_password: Scicat server password (or set SCICAT_INGEST_PASSWORD)

    :param datasettracker_url: Dataset Tracker base url (or set DATASETTRACKER_URL). Will not use the Dataset Tracker if not provided. 
    :param datasettracker_username: Dataset Tracker server username (or set DATASETTRACKER_USERNAME)
    :param datasettracker_password: Dataset Tracker server password (or set DATASETTRACKER_PASSWORD)

    :returns: Dict containing task results or skip message
    """
    # Get the Prefect logger for the current flow run
    prefect_adapter = get_run_logger()

    return ingest(
        dataset_path=dataset_path,
        ingester_spec=ingester_spec,
        owner_username=owner_username,
        scicat_url=scicat_url,
        scicat_username=scicat_username,
        scicat_password=scicat_password,
        datasettracker_url=datasettracker_url,
        datasettracker_username=datasettracker_username,
        datasettracker_password=datasettracker_password,
        logger=prefect_adapter.logger
    )


if __name__ == "__main__":
    pass
