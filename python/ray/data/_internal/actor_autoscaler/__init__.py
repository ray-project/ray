import enum
import logging
import os
from typing import TYPE_CHECKING

from .autoscaling_actor_pool import ActorPoolScalingRequest, AutoscalingActorPool
from .backlog_aware_actor_autoscaler import BacklogAwareActorAutoscaler
from .base_actor_autoscaler import ActorAutoscaler
from .default_actor_autoscaler import DefaultActorAutoscaler, _get_max_scale_up

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data.context import AutoscalingConfig

logger = logging.getLogger(__name__)

ACTOR_AUTOSCALER_ENV_KEY = "RAY_DATA_ACTOR_AUTOSCALER"
DEFAULT_ACTOR_AUTOSCALER_VERSION = "DEFAULT"


class ActorAutoscalerVersion(str, enum.Enum):
    DEFAULT = "DEFAULT"
    BACKLOG_AWARE = "BACKLOG_AWARE"


def create_actor_autoscaler(
    topology: "Topology",
    resource_manager: "ResourceManager",
    config: "AutoscalingConfig",
) -> ActorAutoscaler:
    actor_autoscaler_version = os.environ.get(
        ACTOR_AUTOSCALER_ENV_KEY, DEFAULT_ACTOR_AUTOSCALER_VERSION
    )
    logger.debug(f"Using actor autoscaler version: {actor_autoscaler_version!r}")

    if actor_autoscaler_version == ActorAutoscalerVersion.BACKLOG_AWARE:
        return BacklogAwareActorAutoscaler(
            topology,
            resource_manager,
            config=config,
        )

    elif actor_autoscaler_version == ActorAutoscalerVersion.DEFAULT:
        return DefaultActorAutoscaler(
            topology,
            resource_manager,
            config=config,
        )

    else:
        valid_values = [version.value for version in ActorAutoscalerVersion]
        raise ValueError(
            f"Actor autoscaler version of {actor_autoscaler_version} isn't a valid "
            f"option. Valid options are: {valid_values}."
        )


__all__ = [
    "ActorAutoscaler",
    "ActorPoolScalingRequest",
    "AutoscalingActorPool",
    "BacklogAwareActorAutoscaler",
    "create_actor_autoscaler",
    "_get_max_scale_up",
]
