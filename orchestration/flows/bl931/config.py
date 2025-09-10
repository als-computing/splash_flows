from globus_sdk import TransferClient
from orchestration.globus import transfer


# TODO: Use BeamlineConfig base class (Waiting for PR #62 to be merged)
class Config931:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.data931 = self.endpoints["data931"]
        self.data931_raw = self.endpoints["data931_raw"]
        self.nersc931_alsdev_raw = self.endpoints["nersc931_alsdev_raw"]
