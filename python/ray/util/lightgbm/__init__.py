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
        train,
        predict,
        RayParams,
        RayDMatrix,
        RayFileType,
        RayLGBMClassifier,
        RayLGBMRegressor,
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
