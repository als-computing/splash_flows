from pathlib import Path

from pytest import MonkeyPatch

from orchestration.config import read_config


def test_config():
    config_file = Path(__file__).parent / "test_config.yml"
    with MonkeyPatch.context():
        config = read_config(config_file=config_file)
        assert config
        assert (
            config["globus"]["globus_endpoints"]["test_endpoint"]["root_path"]
            == "/data"
        )
