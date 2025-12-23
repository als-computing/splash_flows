from globus_sdk import TransferClient
from orchestration.globus import transfer


# TODO: Use BeamlineConfig base class (Waiting for PR #62 to be merged)
class Config931:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.bl931_compute_dtn = self.endpoints["bl931-compute-dtn"]
        self.bl931_nersc_alsdev_raw = self.endpoints["bl931-nersc_alsdev_raw"]
