import logging
import importlib

import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
def is_package_installed(package_name):
    try:
        importlib.metadata.version(package_name)
        return True
    except importlib.metadata.PackageNotFoundError:
        return False


try:
    import cv2

    cv2.ocl.setUseOpenCL(False)

    logger.debug("CV2 found for image processing.")
except ImportError as e:
    if is_package_installed("opencv-python"):
        raise ImportError(
            f"OpenCV is installed, but we failed to import it. This may be because "
            f"you need to install `opencv-python-headless` instead of "
            f"`opencv-python`. Error message: {e}",
        )
    cv2 = None


@DeveloperAPI
def resize(img: np.ndarray, height: int, width: int) -> np.ndarray:
    if not cv2:
        raise ModuleNotFoundError(
            "`opencv` not installed! Do `pip install opencv-python`"
        )
    return cv2.resize(img, (width, height), interpolation=cv2.INTER_AREA)


@DeveloperAPI
def rgb2gray(img: np.ndarray) -> np.ndarray:
    if not cv2:
        raise ModuleNotFoundError(
            "`opencv` not installed! Do `pip install opencv-python`"
        )
    return cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)


@DeveloperAPI
def imread(img_file: str) -> np.ndarray:
    if not cv2:
        raise ModuleNotFoundError(
            "`opencv` not installed! Do `pip install opencv-python`"
        )
    return cv2.imread(img_file).astype(np.float32)
