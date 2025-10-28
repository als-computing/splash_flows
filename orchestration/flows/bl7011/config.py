from globus_sdk import TransferClient
from orchestration.globus import transfer


# TODO: Use BeamlineConfig base class (Waiting for PR #62 to be merged)
class Config7011:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.qnap7011 = self.endpoints["qnap7011"]
        self.qnap7011_raw = self.endpoints["qnap7011_raw"]
        self.nersc7011_alsdev_raw = self.endpoints["nersc7011_alsdev_raw"]
