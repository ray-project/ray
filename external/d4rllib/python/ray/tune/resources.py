import json
import logging
from collections import namedtuple

# For compatibility under py2 to consider unicode as str
from typing import Optional

from ray.tune.error import TuneError
from ray.tune.execution.placement_groups import (
    PlacementGroupFactory,
    resource_dict_to_pg_factory,
)
from ray.tune.utils.resource_updater import _Resources
from ray.util.annotations import Deprecated, DeveloperAPI

logger = logging.getLogger(__name__)


@Deprecated
class Resources(
    namedtuple(
        "Resources",
        [
            "cpu",
            "gpu",
            "memory",
            "object_store_memory",
            "extra_cpu",
            "extra_gpu",
            "extra_memory",
            "extra_object_store_memory",
            "custom_resources",
            "extra_custom_resources",
            "has_placement_group",
        ],
    )
):
    __slots__ = ()

    def __new__(
        cls,
        cpu: float,
        gpu: float,
        memory: float = 0,
        object_store_memory: float = 0.0,
        extra_cpu: float = 0.0,
        extra_gpu: float = 0.0,
        extra_memory: float = 0.0,
        extra_object_store_memory: float = 0.0,
        custom_resources: Optional[dict] = None,
        extra_custom_resources: Optional[dict] = None,
        has_placement_group: bool = False,
    ):
        raise DeprecationWarning(
            "tune.Resources is depracted. Use tune.PlacementGroupFactory instead."
        )


@DeveloperAPI
def json_to_resources(data: Optional[str]) -> Optional[PlacementGroupFactory]:
    if data is None or data == "null":
        return None
    if isinstance(data, str):
        data = json.loads(data)

    for k in data:
        if k in ["driver_cpu_limit", "driver_gpu_limit"]:
            raise TuneError(
                "The field `{}` is no longer supported. Use `extra_cpu` "
                "or `extra_gpu` instead.".format(k)
            )
        if k not in _Resources._fields:
            raise ValueError(
                "Unknown resource field {}, must be one of {}".format(
                    k, Resources._fields
                )
            )
    resource_dict_to_pg_factory(
        dict(
            cpu=data.get("cpu", 1),
            gpu=data.get("gpu", 0),
            memory=data.get("memory", 0),
            custom_resources=data.get("custom_resources"),
        )
    )


@Deprecated
def resources_to_json(*args, **kwargs):
    raise DeprecationWarning(
        "tune.Resources is depracted. Use tune.PlacementGroupFactory instead."
    )
