from globus_sdk import TransferClient
from orchestration.globus import transfer


# TODO: Use BeamlineConfig base class (Waiting for PR #62 to be merged)
class Config7011:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.bl7011_als_cosmic_scattering = self.endpoints["bl7011-als-cosmic-scattering"]
        self.bl7011_nersc_alsdev = self.endpoints["bl7011-nersc-alsdev"]
        self.bl7011_compute_dtn = self.endpoints["bl7011-compute-dtn"]
