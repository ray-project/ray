import logging

logger = logging.getLogger(__name__)

train = None
predict = None
RayParams = None
RayDMatrix = None
RayFileType = None
RayXGBClassifier = None
RayXGBRegressor = None
RayXGBRFClassifier = None
RayXGBRFRegressor = None

try:
    from xgboost_ray import (
        train,
        predict,
        RayParams,
        RayDMatrix,
        RayFileType,
        RayXGBClassifier,
        RayXGBRegressor,
        RayXGBRFClassifier,
        RayXGBRFRegressor,
    )
except ImportError:
    logger.info(
        "xgboost_ray is not installed. Please run "
        "`pip install 'git+https://github.com/ray-project/"
        "xgboost_ray#egg=xgboost_ray'`."
    )

__all__ = [
    "train",
    "predict",
    "RayParams",
    "RayDMatrix",
    "RayFileType",
    "RayXGBClassifier",
    "RayXGBRegressor",
    "RayXGBRFClassifier",
    "RayXGBRFRegressor",
]
