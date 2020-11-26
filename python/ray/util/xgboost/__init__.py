import logging

logger = logging.getLogger(__name__)

train = None
predict = None
RayDMatrix = None

try:
    from xgboost_ray import train, predict, RayDMatrix, RayFileType
except ImportError:
    logger.info("xgboost_ray is not installed. Please run "
                "`pip install git+https://github.com/ray-project/xgboost_ray`.")

__all__ = ["train", "predict", "RayDMatrix", "RayFileType"]
