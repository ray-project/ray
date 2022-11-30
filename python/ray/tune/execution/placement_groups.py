from typing import Dict, Optional
from ray.air.execution.resources.request import ResourceRequest
from ray.tune.resources import Resources
from ray.util.annotations import DeveloperAPI, Deprecated


@Deprecated
class PlacementGroupFactory(ResourceRequest):
    """Request for bundles of resources.

    This class is used to request resources, e.g. to trigger autoscaling.
    """

    pass


@DeveloperAPI
def resource_dict_to_pg_factory(spec: Optional[Dict[str, float]]):
    """Translates resource dict into PlacementGroupFactory."""
    spec = spec or {"cpu": 1}

    if isinstance(spec, Resources):
        spec = spec._asdict()

    spec = spec.copy()

    cpus = spec.pop("cpu", spec.pop("CPU", 0.0))
    gpus = spec.pop("gpu", spec.pop("GPU", 0.0))
    memory = spec.pop("memory", 0.0)

    bundle = {k: v for k, v in spec.pop("custom_resources", {}).items()}

    bundle.update(
        {
            "CPU": cpus,
            "GPU": gpus,
            "memory": memory,
        }
    )

    return PlacementGroupFactory([bundle])
