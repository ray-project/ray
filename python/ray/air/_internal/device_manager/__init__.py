import logging
from typing import Optional, Type

import ray
import ray._private.ray_constants as ray_constants
from ray._private.accelerators.hpu import HPU_PACKAGE_AVAILABLE
from ray.air._internal.device_manager.hpu import HPUTorchDeviceManager
from ray.air._internal.device_manager.npu import (
    NPU_TORCH_PACKAGE_AVAILABLE,
    NPUTorchDeviceManager,
)
from ray.air._internal.device_manager.nvidia_gpu import CUDATorchDeviceManager
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager

logger = logging.getLogger(__name__)


DEFAULT_TORCH_DEVICE_MANAGER_CLS = CUDATorchDeviceManager


SUPPORTED_ACCELERATOR_TORCH_DEVICE_MANAGER = {
    ray_constants.GPU: CUDATorchDeviceManager,
    ray_constants.HPU: HPUTorchDeviceManager,
    ray_constants.NPU: NPUTorchDeviceManager,
}


def try_register_torch_accelerator_module(backend=None) -> None:
    try:
        if HPU_PACKAGE_AVAILABLE:
            import habana_frameworks.torch.hpu  # noqa: F401

        if HPU_PACKAGE_AVAILABLE and backend == "hccl":
            import habana_frameworks.torch.core  # noqa: F401
            import habana_frameworks.torch.distributed.hccl  # noqa: F401

        if NPU_TORCH_PACKAGE_AVAILABLE:
            import torch_npu  # noqa: F401

    except ImportError:
        raise ImportError(
            "PyTorch extension modules for accelerators exits but fails to import."
        )


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
            # An error is raised when multiple accelerators are specified.
            if existing_device_manager:
                raise RuntimeError(
                    "Unable to determine the appropriate DeviceManager "
                    f"for the specified resources {resources}."
                )
            else:
                existing_device_manager = device_manager

    return existing_device_manager or DEFAULT_TORCH_DEVICE_MANAGER_CLS


_torch_device_manager = None


def get_torch_device_manager() -> TorchDeviceManager:
    return _torch_device_manager or DEFAULT_TORCH_DEVICE_MANAGER_CLS()


def init_torch_device_manager() -> None:
    global _torch_device_manager

    resources = ray.get_runtime_context().get_accelerator_ids()

    _torch_device_manager = get_torch_device_manager_cls_by_resources(resources)()


__all__ = [
    TorchDeviceManager,
    CUDATorchDeviceManager,
    HPUTorchDeviceManager,
    NPUTorchDeviceManager,
    try_register_torch_accelerator_module,
    get_torch_device_manager,
    init_torch_device_manager,
]
