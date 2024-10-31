import logging

import cv2
import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

cv2.ocl.setUseOpenCL(False)


@DeveloperAPI
def resize(img: np.ndarray, height: int, width: int) -> np.ndarray:
    return cv2.resize(img, (width, height), interpolation=cv2.INTER_AREA)


@DeveloperAPI
def rgb2gray(img: np.ndarray) -> np.ndarray:
    return cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)


@DeveloperAPI
def imread(img_file: str) -> np.ndarray:
    return cv2.imread(img_file).astype(np.float32)
