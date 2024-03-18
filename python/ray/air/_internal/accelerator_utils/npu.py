import importlib
import os
from typing import List

import torch
if importlib.util.find_spec("torch_npu"):
    import torch_npu

import ray
from ray._private.accelerators.npu import ASCEND_RT_VISIBLE_DEVICES_ENV_VAR
from ray.air._internal.accelerator_utils.device_manager import TorchDeviceManager


class NPUTorchDeviceManager(TorchDeviceManager):
    """Ascend NPU device manager"""

    @staticmethod
    def get_accelerator_name() -> str:
        return "NPU"
    
    @staticmethod
    def get_device_type() -> str:
        return "npu"

    @staticmethod
    def get_device_module():
        if not importlib.util.find_spec("torch_npu"):
            raise ImportError(
                "torch-npu is not installed so that npu is not available, "
                "please use `pip install torch-npu` to install torch-npu."
            )
        return torch.npu
    
    @staticmethod
    def is_device_available() -> bool():
        if not importlib.util.find_spec("torch_npu"):
            return False
        
        return torch.npu.is_available()
    
    @staticmethod
    def get_torch_device() -> List[torch.device]:
        """Gets the correct torch device list configured for this process.

        Returns a list of torch NPU devices allocated for the current worker.
        If no NPUs are assigned, then it returns a list with a single CPU device.

        Assumes that `ASCEND_RT_VISIBLE_DEVICES` is set and is a
        superset of the `ray.get_gpu_ids()`.
        """
        if importlib.util.find_spec("torch_npu") and torch.npu.is_available():
            npu_ids = [str(id) for id in ray.get_gpu_ids()]

            device_ids = []

            if len(npu_ids) > 0:
                npu_visible_str = os.environ.get(ASCEND_RT_VISIBLE_DEVICES_ENV_VAR, "")
                if npu_visible_str and npu_visible_str != "NoDevFiles":
                    npu_visible_list = npu_visible_str.split(",")
                else:
                    npu_visible_list = []

                for npu_id in npu_ids:
                    try:
                        device_ids.append(npu_visible_list.index(npu_id))
                    except IndexError:
                        raise RuntimeError(
                            "ASCEND_RT_VISIBLE_DEVICES set incorrectly. "
                            f"Got {npu_visible_str}, expected to include {npu_id}. "
                            "Did you override the `ASCEND_RT_VISIBLE_DEVICES` environment"
                            " variable? If not, please help file an issue on Github."
                        )
            else:
                # If called on the driver or outside of Ray Train, return the
                # 0th device.
                device_ids.append(0)

            devices = [torch.device(f"npu:{device_id}") for device_id in device_ids]
        else:
            devices = [torch.device("cpu")]
        
        return devices
