from globus_sdk import TransferClient
from orchestration.globus import transfer


# TODO: Use BeamlineConfig base class (Waiting for PR #62 to be merged)
class ConfigDichroism:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.bl402_compute_dtn = self.endpoints["bl402-compute-dtn"]
        self.bl402_nersc_alsdev_raw = self.endpoints["bl402-nersc_alsdev_raw"]
        self.bl402_beegfs_raw = self.endpoints["bl402-beegfs_raw"]
        self.bl631_compute_dtn = self.endpoints["bl631-compute-dtn"]
        self.bl631_nersc_alsdev_raw = self.endpoints["bl631-nersc_alsdev_raw"]
        self.bl631_beegfs_raw = self.endpoints["bl631-beegfs_raw"]
