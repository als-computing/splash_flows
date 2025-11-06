from globus_sdk import TransferClient
from orchestration.globus import transfer


# TODO: Use BeamlineConfig base class (Waiting for PR #62 to be merged)
class Config531:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.bl531_nersc_alsdev = self.endpoints["bl531-nersc_alsdev"]
        self.bl531_compute_dtn = self.endpoints["bl531-compute-dtn"]
