from ray.util import metrics
from typing import Tuple, Optional, Dict, List
from ray.serve import context

DEPLOYMENT_TAG = "deployment"
REPLICA_TAG = "replica"
APPLICATION_TAG = "application"


def _add_serve_metric_tags(tag_keys: Optional[Tuple[str]] = None) -> Tuple[str]:
    """Add serve context tags to the tag_keys"""
    if tag_keys is None:
        tag_keys = tuple()

    # If the context doesn't exist, no serve tag is added.
    if context.get_internal_replica_context() is None:
        return tag_keys
    # Check no collision with customer tag
    if DEPLOYMENT_TAG in tag_keys:
        raise ValueError(f"'{DEPLOYMENT_TAG}' tag is reserved for Ray Serve metrics")
    if REPLICA_TAG in tag_keys:
        raise ValueError(f"'{REPLICA_TAG}' tag is reserved for Ray Serve metrics")
    if APPLICATION_TAG in tag_keys:
        raise ValueError(f"'{APPLICATION_TAG}' tag is reserved for Ray Serve metrics")

    # Get serve tag inserted:
    ray_serve_tags = (DEPLOYMENT_TAG, REPLICA_TAG)
    if context.get_internal_replica_context().app_name:
        ray_serve_tags += (APPLICATION_TAG,)
    if tag_keys:
        tag_keys = ray_serve_tags + tag_keys
    else:
        tag_keys = ray_serve_tags
    return tag_keys


def _add_serve_metric_default_tags(default_tags: Dict[str, str]):
    """Add serve context tags and values to the default_tags"""
    if context.get_internal_replica_context() is None:
        return default_tags
    if DEPLOYMENT_TAG in default_tags:
        raise ValueError(f"'{DEPLOYMENT_TAG}' tag is reserved for Ray Serve metrics")
    if REPLICA_TAG in default_tags:
        raise ValueError(f"'{REPLICA_TAG}' tag is reserved for Ray Serve metrics")
    if APPLICATION_TAG in default_tags:
        raise ValueError(f"'{APPLICATION_TAG}' tag is reserved for Ray Serve metrics")
    replica_context = context.get_internal_replica_context()
    default_tags[DEPLOYMENT_TAG] = replica_context.deployment
    default_tags[REPLICA_TAG] = replica_context.replica_tag
    if replica_context.app_name:
        default_tags[APPLICATION_TAG] = replica_context.app_name
    return default_tags


class Counter(metrics.Counter):
    def __init__(
        self, name: str, description: str = "", tag_keys: Optional[Tuple[str]] = None
    ):
        if tag_keys and not isinstance(tag_keys, tuple):
            raise TypeError(
                "tag_keys should be a tuple type, got: " f"{type(tag_keys)}"
            )
        tag_keys = _add_serve_metric_tags(tag_keys)
        super().__init__(name, description, tag_keys)
        self.set_default_tags({})

    def set_default_tags(self, default_tags: Dict[str, str]):
        super().set_default_tags(_add_serve_metric_default_tags(default_tags))


class Gauge(metrics.Gauge):
    def __init__(
        self, name: str, description: str = "", tag_keys: Optional[Tuple[str]] = None
    ):
        if tag_keys and not isinstance(tag_keys, tuple):
            raise TypeError(
                "tag_keys should be a tuple type, got: " f"{type(tag_keys)}"
            )
        tag_keys = _add_serve_metric_tags(tag_keys)
        super().__init__(name, description, tag_keys)
        self.set_default_tags({})

    def set_default_tags(self, default_tags: Dict[str, str]):
        super().set_default_tags(_add_serve_metric_default_tags(default_tags))


class Histogram(metrics.Histogram):
    def __init__(
        self,
        name: str,
        description: str = "",
        boundaries: List[float] = None,
        tag_keys: Optional[Tuple[str]] = None,
    ):
        if tag_keys and not isinstance(tag_keys, tuple):
            raise TypeError(
                "tag_keys should be a tuple type, got: " f"{type(tag_keys)}"
            )
        tag_keys = _add_serve_metric_tags(tag_keys)
        super().__init__(name, description, boundaries, tag_keys)
        self.set_default_tags({})

    def set_default_tags(self, default_tags: Dict[str, str]):
        super().set_default_tags(_add_serve_metric_default_tags(default_tags))
