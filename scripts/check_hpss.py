from orchestration.hpss import (
    cfs_to_hpss_flow,
    hpss_to_cfs_flow,
    get_prune_controller,
    get_transfer_controller,
    PruneMethod,
    CopyMethod,
)
from orchestration.transfer_endpoints import FileSystemEndpoint, HPSSEndpoint
import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
TEST_HPSS_PRUNE = False
TEST_CFS_TO_HPSS = False
TEST_HPSS_TO_CFS = False
TEST_HPSS_LS = True

# ------------------------------------------------------
# Test pruning from HPSS
# ------------------------------------------------------
if TEST_HPSS_PRUNE:
    from orchestration.flows.bl832.config import Config832
    config = Config832()
    file_name = "8.3.2/raw/ALS-11193_nbalsara/ALS-11193_nbalsara_2022-2.tar"
    source = HPSSEndpoint(
        name="HPSS",
        root_path=config.hpss_alsdev["root_path"],
        uri=config.hpss_alsdev["uri"]
    )

    days_from_now = datetime.timedelta(days=0)  # Prune immediately

    prune_controller = get_prune_controller(
        prune_type=PruneMethod.HPSS,
        config=config
    )
    prune_controller.prune(
        file_path=f"{file_name}",
        source_endpoint=source,
        check_endpoint=None,
        days_from_now=days_from_now
    )
# ------------------------------------------------------
# Test transfer from CFS to HPSS
# ------------------------------------------------------
if TEST_CFS_TO_HPSS:
    from orchestration.flows.bl832.config import Config832
    config = Config832()
    project_name = "ALS-11193_nbalsara"
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
    cfs_to_hpss_flow(
        file_path=f"{project_name}",
        source=source,
        destination=destination,
        config=config
    )

# ------------------------------------------------------
# Test transfer from HPSS to CFS
# ------------------------------------------------------
if TEST_HPSS_TO_CFS:
    from orchestration.flows.bl832.config import Config832
    config = Config832()
    relative_file_path = f"{config.beamline_id}/raw/ALS-11193_nbalsara/ALS-11193_nbalsara_2022-2.tar"
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
        "20221109_012020_MSB_Book1_Proj33_Cell5_2pFEC_LiR2_6C_Rest3.h5",
        "20221012_172023_DTH_100722_LiT_r01_cell3_10x_0_19_CP2.h5",
    ]

    hpss_to_cfs_flow(
        file_path=f"{relative_file_path}",
        source=source,
        destination=destination,
        files_to_extract=files_to_extract,
        config=config
    )

# ------------------------------------------------------
# Test listing HPSS files
# ------------------------------------------------------
if TEST_HPSS_LS:
    from orchestration.flows.bl832.config import Config832

    # Build client, config, endpoint
    config = Config832()
    endpoint = HPSSEndpoint(
        name="HPSS",
        root_path=config.hpss_alsdev["root_path"],
        uri=config.hpss_alsdev["uri"]
    )

    # Instantiate controller
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.CFS_TO_HPSS,
        config=config
    )

    # Directory listing
    project_path = f"{config.beamline_id}/raw/BLS-00564_dyparkinson"
    logger.info("Controller-based directory listing on HPSS:")
    output_file = transfer_controller.list_hpss(
        endpoint=endpoint,
        remote_path=project_path,
        recursive=True
    )

    # TAR archive listing
    archive_name = project_path.split("/")[-1]
    tar_path = f"{project_path}/{archive_name}_2023-1.tar"
    logger.info("Controller-based tar archive listing on HPSS:")
    output_file = transfer_controller.list_hpss(
        endpoint=endpoint,
        remote_path=tar_path,
        recursive=False
    )
