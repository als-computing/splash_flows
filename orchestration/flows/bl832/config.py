from globus_sdk import TransferClient

from orchestration.config import BeamlineConfig
from orchestration.globus import flows, transfer


class Config832(BeamlineConfig):
    def __init__(self) -> None:
        super().__init__(beamline_id="8.3.2")

    def _beam_specific_config(self) -> None:
        # config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(self.config)
        self.apps = transfer.build_apps(self.config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.flow_client = flows.get_flows_client()
        self.spot832 = self.endpoints["spot832"]
        self.data832 = self.endpoints["data832"]
        self.data832_raw = self.endpoints["data832_raw"]
        self.data832_scratch = self.endpoints["data832_scratch"]
        self.nersc832 = self.endpoints["nersc832"]
        self.nersc_alsdev = self.endpoints["nersc_alsdev"]
        self.nersc832_alsdev_raw = self.endpoints["nersc832_alsdev_raw"]
        self.nersc832_alsdev_scratch = self.endpoints["nersc832_alsdev_scratch"]
        self.nersc832_alsdev_pscratch_raw = self.endpoints["nersc832_alsdev_pscratch_raw"]
        self.nersc832_alsdev_pscratch_scratch = self.endpoints["nersc832_alsdev_pscratch_scratch"]
        self.nersc832_alsdev_recon_scripts = self.endpoints["nersc832_alsdev_recon_scripts"]
        self.alcf832_raw = self.endpoints["alcf832_raw"]
        self.alcf832_scratch = self.endpoints["alcf832_scratch"]
        self.hpss_alsdev = self.config["hpss_alsdev"]
        self.scicat = self.config["scicat"]
        self.ghcr_images832 = self.config["ghcr_images832"]
