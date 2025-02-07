from globus_sdk import TransferClient
from orchestration.globus import transfer


class Config733:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.data733 = self.endpoints["data733"]
        self.data733_raw = self.endpoints["data733_raw"]
        self.nersc733_alsdev_raw = self.endpoints["nersc733_alsdev_raw"]
