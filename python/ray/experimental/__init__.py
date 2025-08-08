from ray.experimental.dynamic_resources import set_resource
from ray.experimental.locations import get_local_object_locations, get_object_locations
from ray.experimental.gpu_object_manager import GPUObjectManager, wait_tensor_freed

__all__ = [
    "get_object_locations",
    "get_local_object_locations",
    "set_resource",
    "GPUObjectManager",
    "wait_tensor_freed",
]
