from ray.air._internal.accelerator_utils.device_manager import TorchDeviceManager
from ray.air._internal.accelerator_utils.utils import (
    get_torch_device_manager_cls_by_resources,
    try_register_torch_accelerator_module,
)

__all__ = [
    "TorchDeviceManager",
    "try_register_torch_accelerator_module",
    "get_torch_device_manager_cls_by_resources",
]
