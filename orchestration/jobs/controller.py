# orchestration/jobs/controller.py
"""Generic job controller abstractions.

Defines the minimal base class and target enum that any backend-specific
controller (NERSC, ALCF, future local clusters) builds on. Beamline-specific
controllers live under ``orchestration/flows/<beamline>/`` and subclass
:class:`JobController` (or a domain-specific intermediate like
``TomographyJobController``).
"""

from abc import ABC
from enum import Enum
import logging

from orchestration.config import BeamlineConfig

logger = logging.getLogger(__name__)


class JobTarget(Enum):
    """Identifies a job execution target.

    Used by beamline-specific factories (e.g. ``flows/bl832/job_controller.py``)
    to dispatch to the appropriate concrete controller class.

    Members:
        ALCF: Argonne Leadership Computing Facility
        NERSC: National Energy Research Scientific Computing Center
        OLCF: Oak Ridge Leadership Computing Facility
    """

    ALCF = "ALCF"
    NERSC = "NERSC"
    OLCF = "OLCF"


class JobController(ABC):
    """Base class for job controllers.

    Subclasses provide target-specific job submission and monitoring (see
    :class:`orchestration.jobs.nersc.controller.NERSCJobController` and
    :class:`orchestration.jobs.alcf.controller.ALCFJobController`).

    No abstract methods are declared here because submission shapes differ
    fundamentally between targets — NERSC submits Slurm scripts and returns
    job IDs, ALCF submits Python callables via Globus Compute and returns
    futures. Domain-specific intermediates (e.g. ``TomographyJobController``)
    may add ``@abstractmethod``s like ``reconstruct(file_path)`` when multiple
    controllers share an interface.

    Args:
        config: Beamline configuration object. Stored as ``self.config`` for
            subclass use.
    """

    def __init__(self, config: BeamlineConfig) -> None:
        self.config = config
