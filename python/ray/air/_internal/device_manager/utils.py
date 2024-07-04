import logging
from typing import Optional

from ray._private.accelerators.hpu import HPU_PACKAGE_AVAILABLE
from ray.air._internal.device_manager.hpu import HPUTorchDeviceManager
from ray.air._internal.device_manager.npu import (
    NPU_TORCH_PACKAGE_AVAILABLE,
    NPUTorchDeviceManager,
)
from ray.air._internal.device_manager.nvidia_gpu import CUDATorchDeviceManager
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager

logger = logging.getLogger(__name__)


SUPPORTED_ACCELERATOR_TORCH_DEVICE_MANAGER = {
    "GPU": CUDATorchDeviceManager,
    "HPU": HPUTorchDeviceManager,
    "NPU": NPUTorchDeviceManager,
}


def try_register_torch_accelerator_module() -> None:
    try:
        if NPU_TORCH_PACKAGE_AVAILABLE:
            import torch_npu  # noqa: F401

        if HPU_PACKAGE_AVAILABLE:
            import habana_frameworks.torch.hpu as torch_hpu  # noqa: F401

    except ImportError:
        raise ImportError("Could not import PyTorch")


def get_torch_device_manager_cls_by_resources(
    resources: Optional[dict],
) -> TorchDeviceManager:
    device_manager = None

    # input resources may be None
    if not resources:
        return CUDATorchDeviceManager

    # select correct accelerator type from resources
    for resource_type, resource_value in resources.items():
        if resource_value and resource_type != "CPU":
            device_manager = SUPPORTED_ACCELERATOR_TORCH_DEVICE_MANAGER.get(
                resource_type, None
            )

    return device_manager or CUDATorchDeviceManager
