
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


class NPArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return [None if np.isnan(item) or np.isinf(item) else item for item in obj]
        return json.JSONEncoder.default(self, obj)


def calculate_access_controls(username, beamline, proposal) -> Dict:
    # make an access group list that includes the name of the proposal and the name of the beamline
    access_groups = []
    # sometimes the beamline name is super dirty  " '8.3.2', "" '8.3.2', "
    beamline = beamline.replace(" '", "").replace("', ", "") if beamline else None
    # set owner_group to username so that at least someone has access in case no proposal number is found
    owner_group = username
    if beamline:
        access_groups.append(beamline)
        # username lets the user see the Dataset in order to ingest objects after the Dataset
        access_groups.append(username)
        # temporary mapping while beamline controls process request to match beamline name with what comes
        # from ALSHub
        if beamline == "bl832" and "8.3.2" not in access_groups:
            access_groups.append("8.3.2")

    if proposal and proposal != "None":
        owner_group = proposal

    # this is a bit of a kludge. Add 8.3.2 into the access groups so that staff will be able to see it
    return {"owner_group": owner_group, "access_groups": access_groups}


def build_search_terms(sample_name):
    """extract search terms from sample name to provide something pleasing to search on"""
    terms = re.split("[^a-zA-Z0-9]", sample_name)
    description = [term.lower() for term in terms if len(term) > 0]
    return " ".join(description)

