import logging

logger = logging.getLogger(__name__)

RayPlugin = None
HorovodRayPlugin = None
RayShardedPlugin = None

try:
    from ray_lightning import RayPlugin, HorovodRayPlugin, RayShardedPlugin
except ImportError:
    logger.info(
        "ray_lightning is not installed. Please run " "`pip install ray-lightning`."
    )

__all__ = ["RayPlugin", "HorovodRayPlugin", "RayShardedPlugin"]
