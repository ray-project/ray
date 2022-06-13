import logging

logger = logging.getLogger(__name__)

train = None
predict = None
RayParams = None
RayDMatrix = None
RayFileType = None
RayLGBMClassifier = None
RayLGBMRegressor = None

try:
    from lightgbm_ray import (
        RayDMatrix,
        RayFileType,
        RayLGBMClassifier,
        RayLGBMRegressor,
        RayParams,
        predict,
        train,
    )
except ImportError:
    logger.info(
        "lightgbm_ray is not installed. Please run "
        "`pip install git+https://github.com/ray-project/lightgbm_ray`."
    )

__all__ = [
    "train",
    "predict",
    "RayParams",
    "RayDMatrix",
    "RayFileType",
    "RayLGBMClassifier",
    "RayLGBMRegressor",
]
