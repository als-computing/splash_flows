
import base64
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from enum import Enum
import io
import json
import logging
import re
from typing import Optional, Union

import numpy as np
import numpy.typing as npt
from PIL import Image, ImageOps

logger = logging.getLogger("splash_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)


class Severity(
    str,
    Enum
):
    """Enum for issue severity."""
    warning = "warning"
    error = "error"


@dataclass
class Issue:
    """Dataclass for issues."""
    severity: Severity
    msg: str
    exception: Optional[Union[str, None]] = None


class NPArrayEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types."""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return [None if np.isnan(item) or np.isinf(item) else item for item in obj]
        return json.JSONEncoder.default(self, obj)


def build_search_terms(
    sample_name: str
) -> str:
    """extract search terms from sample name to provide something pleasing to search on"""
    terms = re.split("[^a-zA-Z0-9]", sample_name)
    description = [term.lower() for term in terms if len(term) > 0]
    return " ".join(description)


def build_thumbnail(
    image_array: npt.ArrayLike
) -> io.BytesIO:
    """Create a thumbnail from an image array."""
    image_array = image_array - np.min(image_array) + 1.001
    image_array = np.log(image_array)
    image_array = 205 * image_array / (np.max(image_array))
    auto_contrast_image = Image.fromarray(image_array.astype("uint8"))
    auto_contrast_image = ImageOps.autocontrast(auto_contrast_image, cutoff=0.1)
    # filename = str(uuid4()) + ".png"
    file = io.BytesIO()
    # file = thumbnail_dir / Path(filename)
    auto_contrast_image.save(file, format="png")
    file.seek(0)
    return file


def clean_email(email: str):
    """Clean up email addresses."""

    if email:
        if not email or email.upper() == "NONE":
            # this is a brutal case, but the beamline sometimes puts in "None" and
            # the new scicat backend hates that.
            unknown_email = "unknown@example.com"
            return unknown_email
        return email.replace(" ", "").replace(",", "").replace("'", "")
    return None


def encode_image_2_thumbnail(
    filebuffer,
    imType="jpg"
) -> str:
    """Encode an image file to a base 64 string for use as a thumbnail."""
    logging.info("Creating thumbnail for dataset")
    header = "data:image/{imType};base64,".format(imType=imType)
    dataBytes = base64.b64encode(filebuffer.read())
    dataStr = dataBytes.decode("UTF-8")
    return header + dataStr


def get_file_size(file_path: Path) -> int:
    """Return the size of the file in bytes."""
    return file_path.lstat().st_size


def get_file_mod_time(file_path: Path) -> str:
    """Return the file modification time in ISO format."""
    return datetime.fromtimestamp(file_path.lstat().st_mtime).isoformat()
