import logging

from ray._private.accelerators.hpu import HPU_PACKAGE_AVAILABLE
from ray.air._internal.accelerator_utils.device_manager import TorchDeviceManager
from ray.air._internal.accelerator_utils.hpu import HPUTorchDeviceManager
from ray.air._internal.accelerator_utils.npu import (
    NPU_TORCH_PACKAGE_AVAILABLE,
    NPUTorchDeviceManager,
)
from ray.air._internal.accelerator_utils.nvidia_gpu import CUDATorchDeviceManager

logger = logging.getLogger(__name__)


ACCELERATOR_TORCH_DEVICE_MANAGER = {
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


def get_torch_device_manager_cls_by_resources(resources) -> TorchDeviceManager:
    device_manager = None

    for resource_type, resource_value in resources.items():
        if resource_value > 0 and resource_type != "CPU":
            device_manager = ACCELERATOR_TORCH_DEVICE_MANAGER.get(resource_type, None)

    return device_manager or CUDATorchDeviceManager
