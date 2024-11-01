import logging

import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

try:
    import cv2

    cv2.ocl.setUseOpenCL(False)

    logger.debug("CV2 found for image processing.")
except ImportError:
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
