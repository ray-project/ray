from typing import Dict, Optional
from ray.air.execution.resources.request import ResourceRequest
from ray.tune.resources import Resources
from ray.util.annotations import DeveloperAPI, Deprecated


@Deprecated(
    message="`tune.PlacementGroupFactory` has been renamed to `air.ResourceRequest`"
)
class PlacementGroupFactory(ResourceRequest):
    """Request for bundles of resources.

    This class is used to request resources, e.g. to trigger autoscaling.
    """

    pass


@DeveloperAPI
def resource_dict_to_request(spec: Optional[Dict[str, float]]) -> ResourceRequest:
    """Translates resource dict into ResourceRequest."""
    spec = spec or {"cpu": 1}

    if isinstance(spec, Resources):
        spec = spec._asdict()

    spec = spec.copy()

    cpus = spec.pop("cpu", 0.0)
    gpus = spec.pop("gpu", 0.0)
    memory = spec.pop("memory", 0.0)

    bundle = {k: v for k, v in spec.pop("custom_resources", {}).items()}

    bundle.update(
        {
            "CPU": cpus,
            "GPU": gpus,
            "memory": memory,
        }
    )

    return ResourceRequest([bundle])
