from globus_sdk import TransferClient

from orchestration.config import BeamlineConfig
from orchestration.globus import transfer


class Config733(BeamlineConfig):
    def __init__(self) -> None:
        super().__init__(beamline_id="7.3.3")

    def _beam_specific_config(self) -> None:
        self.endpoints = transfer.build_endpoints(self.config)
        self.apps = transfer.build_apps(self.config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.data733 = self.endpoints["bl733-als-data733"]
        self.data733_raw = self.endpoints["bl733-als-data733_raw"]
        self.nersc733_alsdev_raw = self.endpoints["bl733-nersc-alsdev_raw"]
        self.beegfs733 = self.endpoints["bl733-beegfs-data"]
