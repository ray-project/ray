from typing import Dict, Optional

from ray.serve._private.publish_provider import PublishProvider


class TestPublishProvider(PublishProvider):
    """PublishProvider implementation for unit testing."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.published_config = None
        self.published_name = None
        self.published_ray_version = None
        self.published_base_image = None

    def publish(
        self,
        config: Dict,
        *,
        name: str,
        ray_version: str,
        base_image: Optional[str] = None,
    ):
        self.published_config = config
        self.published_name = name
        self.published_ray_version = ray_version
        self.published_base_image = base_image


PUBLISH_PROVIDER_SINGLETON = None


def get_ray_serve_publish_provider():
    global PUBLISH_PROVIDER_SINGLETON
    if PUBLISH_PROVIDER_SINGLETON is None:
        PUBLISH_PROVIDER_SINGLETON = TestPublishProvider()

    return PUBLISH_PROVIDER_SINGLETON
