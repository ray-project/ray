import logging
logger = logging.getLogger(__name__)
logger.info("Initializing platform_events module")

from ray.dashboard.modules.platform_events.platform_events_head import PlatformEventsHead

__all__ = ["PlatformEventsHead"]
