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

if cv2 is None:
    try:
        from skimage import color, io, transform

        logger.debug("CV2 not found for image processing, using Skimage.")
    except ImportError:
        raise ModuleNotFoundError("Either scikit-image or opencv is required")


@DeveloperAPI
def resize(img: np.ndarray, height: int, width: int) -> np.ndarray:
    if cv2:
        return cv2.resize(img, (width, height), interpolation=cv2.INTER_AREA)
    return transform.resize(img, (height, width))


@DeveloperAPI
def rgb2gray(img: np.ndarray) -> np.ndarray:
    if cv2:
        return cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    return color.rgb2gray(img)


@DeveloperAPI
def imread(img_file: str) -> np.ndarray:
    if cv2:
        return cv2.imread(img_file).astype(np.float32)
    return io.imread(img_file).astype(np.float32)
