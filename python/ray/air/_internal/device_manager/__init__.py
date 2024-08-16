import logging
import threading
from typing import Optional, Type

import ray
import ray._private.ray_constants as ray_constants
from ray.air._internal.device_manager.cpu import CPUTorchDeviceManager
from ray.air._internal.device_manager.hpu import HPUTorchDeviceManager
from ray.air._internal.device_manager.npu import NPUTorchDeviceManager
from ray.air._internal.device_manager.nvidia_gpu import CUDATorchDeviceManager
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager

logger = logging.getLogger(__name__)


DEFAULT_TORCH_DEVICE_MANAGER_CLS = CPUTorchDeviceManager


SUPPORTED_ACCELERATOR_TORCH_DEVICE_MANAGER = {
    ray_constants.GPU: CUDATorchDeviceManager,
    ray_constants.HPU: HPUTorchDeviceManager,
    ray_constants.NPU: NPUTorchDeviceManager,
}


def register_custom_torch_dist_backend(backend: Optional[str] = None) -> None:
    if backend == "hccl":
        # The name for the communication backend of Habana and torch-npu is the same.
        HPUTorchDeviceManager.register_custom_torch_dist_backend()

        NPUTorchDeviceManager.register_custom_torch_dist_backend()


def get_torch_device_manager_cls_by_resources(
    resources: Optional[dict],
) -> Type[TorchDeviceManager]:
    existing_device_manager = None

    # input resources may be None
    if not resources:
        return DEFAULT_TORCH_DEVICE_MANAGER_CLS

    # select correct accelerator type from resources
    for resource_type, resource_value in resources.items():
        device_manager = SUPPORTED_ACCELERATOR_TORCH_DEVICE_MANAGER.get(
            resource_type, None
        )
        if resource_value and device_manager:
            # An error will raise when multiple accelerators are specified.
            if existing_device_manager:
                raise RuntimeError(
                    "Unable to determine the appropriate DeviceManager "
                    f"for the specified resources {resources}."
                )
            else:
                existing_device_manager = device_manager

    return existing_device_manager or DEFAULT_TORCH_DEVICE_MANAGER_CLS


def get_torch_device_manager_cls_by_device_type(device_type: str):
    if device_type.lower() == ray_constants.GPU.lower() or device_type == "cuda":
        return CUDATorchDeviceManager
    elif device_type.lower() == ray_constants.NPU.lower():
        return NPUTorchDeviceManager
    elif device_type.lower() == ray_constants.HPU.lower():
        return HPUTorchDeviceManager
    elif device_type.lower() == "cpu":
        return CPUTorchDeviceManager

    raise RuntimeError(f"Device type {device_type} cannot be recognized.")


_torch_device_manager = None
_torch_device_manager_lock = threading.Lock()


def get_torch_device_manager(device_type: Optional[str] = None) -> TorchDeviceManager:
    if device_type:
        # Specify the device type to retrieve the device manager directly,
        # rather than relying on the remote environment to determine it.
        return get_torch_device_manager_cls_by_device_type(device_type)()

    with _torch_device_manager_lock:
        if not _torch_device_manager:
            init_torch_device_manager()

    return _torch_device_manager


def init_torch_device_manager() -> None:
    global _torch_device_manager

    resources = ray.get_runtime_context().get_accelerator_ids()

    _torch_device_manager = get_torch_device_manager_cls_by_resources(resources)()


__all__ = [
    TorchDeviceManager,
    CPUTorchDeviceManager,
    CUDATorchDeviceManager,
    HPUTorchDeviceManager,
    NPUTorchDeviceManager,
    register_custom_torch_dist_backend,
    get_torch_device_manager,
    init_torch_device_manager,
]
