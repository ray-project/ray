from ray.air._internal.device_manager.device_manager import DeviceManager
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager
from ray.air._internal.device_manager.utils import (
    get_torch_device_manager_cls_by_resources,
    try_register_torch_accelerator_module,
)

__all__ = [
    "DeviceManager",
    "TorchDeviceManager",
    "try_register_torch_accelerator_module",
    "get_torch_device_manager_cls_by_resources",
]
