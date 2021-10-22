from typing import Dict, Any
from ray._private.utils import resources_from_resource_arguments


def _resources_from_ray_options(ray_options) -> Dict[str, Any]:
    num_cpus = ray_options.get("num_cpus", 1)
    num_gpus = ray_options.get("num_gpus")
    memory = ray_options.get("memory")
    object_store_memory = ray_options.get("object_store_memory")
    _resources = ray_options.get("resources")
    accelerator_type = ray_options.get("accelerator_type")

    resources = resources_from_resource_arguments(
        num_cpus, num_gpus, memory, object_store_memory, _resources,
        accelerator_type, num_cpus, num_gpus, memory, object_store_memory,
        _resources, accelerator_type)
    return resources


def resources_equal(ray_options_1: Dict, ray_options_2: Dict) -> bool:
    resources_1 = _resources_from_ray_options(ray_options_1)
    resources_2 = _resources_from_ray_options(ray_options_2)
    if resources_1 != resources_2:
        return False
    resources = ("num_cpus", "num_gpus", "memory", "object_store_memory",
                 "accelerator_type", "resources")
    ray_options_1 = ray_options_1.copy()
    ray_options_2 = ray_options_2.copy()
    for res in resources:
        ray_options_1.pop(res, None)
        ray_options_2.pop(res, None)
    return ray_options_1 == ray_options_2
