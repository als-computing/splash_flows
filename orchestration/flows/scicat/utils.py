
import base64
from dataclasses import dataclass
from enum import Enum
import io
import json
import logging
from pathlib import Path
import re
from typing import Dict, Optional, Union
from uuid import uuid4

import numpy as np
import numpy.typing as npt
from PIL import Image, ImageOps

logger = logging.getLogger("splash_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)

class Severity(str, Enum):
    warning = "warning"
    error = "error"


@dataclass
class Issue:
    severity: Severity
    msg: str
    exception: Optional[Union[str, None]] = None
