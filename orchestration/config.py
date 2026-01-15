from abc import ABC, abstractmethod
import collections
import builtins
from pathlib import Path
import re
import os
import yaml

from dynaconf import Dynaconf

settings = Dynaconf(
    settings_files=["config.yml"],
)


def get_config():
    return read_config(config_file=Path(__file__).parent.parent / "config.yml")


def read_config(config_file="config.yml"):
    with open(config_file, "r") as end_file:
        config = yaml.safe_load(end_file)
    return expand_environment_variables(config)


def expand_environment_variables(config):
    """Expand environment variables in a nested config dictionary
    VENDORED FROM dask.config and tiled.
    This function will recursively search through any nested dictionaries
    and/or lists.
    Parameters
    ----------
    config : dict, iterable, or str
        Input object to search for environment variables
    Returns
    -------
    config : same type as input
    Examples
    --------
    >>> expand_environment_variables({'x': [1, 2, '$USER']})  # doctest: +SKIP
    {'x': [1, 2, 'my-username']}
    """
    if isinstance(config, collections.abc.Mapping):
        return {k: expand_environment_variables(v) for k, v in config.items()}
    elif isinstance(config, str):
        return os.path.expandvars(config)
    elif isinstance(config, (list, tuple, builtins.set)):
        return type(config)([expand_environment_variables(v) for v in config])
    else:
        return config


class BeamlineConfig(ABC):
    """
    Base class for beamline configurations.

    This class reads the common configuration from disk, builds endpoints and apps,
    and initializes the Globus Transfer and Flows clients. Beamline-specific subclasses
    must override the _setup_specific_config() method to assign their own attributes.

    :param beamline_id: Beamline number identifier with periods (e.g. "8.3.2" or "7.3.3").
    :param config: The loaded configuration dictionary.
    """

    def __init__(
        self,
        beamline_id: str
    ) -> None:
        """
        Initialize the BeamlineConfig with beamline-specific settings.
        :param beamline_id: Beamline number identifier with periods (e.g. "8.3.2" or "7.3.3").
        :raises ValueError: If beamline_id format is invalid.
        :return: None
         """
        pattern = r'^\d+(\.\d+)+$'
        if not re.match(pattern, beamline_id):
            raise ValueError(f"Invalid beamline_id format: '{beamline_id}'."
                             f"Expected format: digits separated by dots (e.g., '8.3.2', '7.0.1.2', '12.3')")
        self.beamline_id = beamline_id
        self.config = settings
        self._beam_specific_config()
        self.config = None  # Clear reference to config after beam-specific setup

    @abstractmethod
    def _beam_specific_config(self) -> None:
        """
        Set up beamline-specific configuration attributes.

        This method must be implemented by subclasses. Typical assignments
        include selecting endpoints (using keys that include the beamline ID),
        and other beamline-specific parameters.
        """
        pass
