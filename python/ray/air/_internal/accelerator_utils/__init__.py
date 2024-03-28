import importlib
import glob
from typing import Optional, Set
import logging

import ray
from ray._private.accelerators.hpu import HPU_PACKAGE_AVAILABLE
from ray.air._internal.accelerator_utils.device_manager import TorchDeviceManager
from ray.air._internal.accelerator_utils.nvidia_gpu import CUDATorchDeviceMananger
from ray.air._internal.accelerator_utils.npu import NPUTorchDeviceManager
from ray.air._internal.accelerator_utils.hpu import HPUTorchDeviceManager

logger = logging.getLogger(__name__)


def try_import_torch():
    try:
        import torch
        import torch.nn as nn

        if importlib.util.find_spec("torch_npu"):
            if not len(glob.glob("/dev/davinci?")):
                logger.warning(
                    "It seems that there is no Ascend NPU device in "
                    "/dev path and torch_npu will not be imported."
                )
            else:
                import torch_npu  # noqa: F401

        if HPU_PACKAGE_AVAILABLE:
            import habana_frameworks.torch.hpu as torch_hpu  # noqa: F401

        return torch, nn
    except ImportError:
        raise ImportError("Could not import PyTorch")


def get_all_torch_device_manager() -> Set[TorchDeviceManager]:
    """Get all device manager supported by Ray"""
    return {CUDATorchDeviceMananger, HPUTorchDeviceManager, NPUTorchDeviceManager}


def get_torch_device_manager_for_resources(
    resource_name: str,
) -> Optional[TorchDeviceManager]:
    """Get the corresponding device manager for the given accelerator resource name.

    E.g. NPUTorchDeviceManager is returned if resource name is "NPU"
    """
    for torch_device_manager in get_all_torch_device_manager():
        if torch_device_manager.get_accelerator_name() == resource_name:
            return torch_device_manager


def get_torch_device_manager_by_runtime_context() -> Optional[TorchDeviceManager]:
    """Return a corresponding TorchDeviceManager class if some
    accelerator is allocated and supported, otherwise return None.
    """
    allocated_accelerators = ray.get_runtime_context().get_accelerator_ids()
    device_manager = None
    for accelerator_type, accelerator_ids in allocated_accelerators.items():
        if len(accelerator_ids) > 0:
            if device_manager:
                # Raise a warning if device manager is selected but another
                # accelerators are allocated
                logger.warning(
                    "There is more than one accelerator allocated which may "
                    "not be fully used."
                )
            else:
                device_manager = get_torch_device_manager_for_resources(
                    accelerator_type
                )

    return device_manager


__all__ = [
    "try_import_torch",
    "CUDATorchDeviceManager",
    "HPUTorchDeviceManager",
    "NPUTorchDeviceManager",
    "get_all_torch_device_manager",
    "get_torch_device_manager_for_resource",
    "get_torch_device_manager_by_runtime_context",
]
