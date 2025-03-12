import os
from typing import List, Union, Optional

import torch

import ray
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager


class CUDATorchDeviceManager(TorchDeviceManager):
    """CUDA device manager"""

    def is_available(self) -> bool():
        return torch.cuda.is_available()

    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device list configured for this process.

        Returns a list of torch CUDA devices allocated for the current worker.
        If no GPUs are assigned, then it returns a list with a single CPU device.

        Assumes that `CUDA_VISIBLE_DEVICES` is set and is a
        superset of the `ray.get_gpu_ids()`.
        """

        # GPU IDs are assigned by Ray after you specify "use_gpu"
        # GPU `ray.get_gpu_ids()` may return ints or may return strings.
        # We should always convert to strings.
        gpu_ids = [str(id) for id in ray.get_gpu_ids()]

        device_ids = []

        if len(gpu_ids) > 0:
            cuda_visible_str = os.environ.get("CUDA_VISIBLE_DEVICES", "")
            if cuda_visible_str and cuda_visible_str != "NoDevFiles":
                cuda_visible_list = cuda_visible_str.split(",")
            else:
                cuda_visible_list = []

            # By default, there should only be one GPU ID if `use_gpu=True`.
            # If there are multiple GPUs, return a list of devices.
            # If using fractional GPUs, these IDs are not guaranteed
            # to be unique across different processes.
            for gpu_id in gpu_ids:
                try:
                    device_ids.append(cuda_visible_list.index(gpu_id))
                except IndexError:
                    raise RuntimeError(
                        "CUDA_VISIBLE_DEVICES set incorrectly. "
                        f"Got {cuda_visible_str}, expected to include {gpu_id}. "
                        "Did you override the `CUDA_VISIBLE_DEVICES` environment"
                        " variable? If not, please help file an issue on Github."
                    )

        else:
            # If called on the driver or outside of Ray Train, return the
            # 0th device.
            device_ids.append(0)

        return [torch.device(f"cuda:{device_id}") for device_id in device_ids]

    def set_device(self, device: Union[torch.device, int, str, None]):
        torch.cuda.set_device(device)

    def supports_stream(self) -> bool:
        """Validate if the device type support create a stream"""
        return True

    def create_stream(self, device: torch.device) -> torch.cuda.Stream:
        """Create a stream on cuda device"""
        return torch.cuda.Stream(device)

    def get_stream_context(self, stream):
        """Get a stream context for cuda device"""
        return torch.cuda.stream(stream)

    def get_current_stream(self) -> torch.cuda.Stream:
        """Get current stream for cuda device"""
        return torch.cuda.current_stream()

    def create_event(self) -> torch.cuda.Event:
        """Create a event on cuda device"""
        return torch.cuda.Event()

    def get_device_context(self, device: torch.device) -> torch.cuda.device:
        """Get a device context for cuda device"""
        return torch.cuda.device(device)

    def get_communicator(
        self,
        world_size: int,
        comm_id: int,
        rank: Optional[int],
        actor_handles: List["ray.actor.ActorHandle"],
        torch_stream: Optional[torch.cuda.Stream],
        use_communication_streams: bool = False,
    ) -> "ray.experimental.channel.communicator.Communicator":
        """Get a communicator"""
        from ray.experimental.channel.nccl_group import _NcclGroup

        return _NcclGroup(
            world_size=world_size,
            comm_id=comm_id,
            rank=rank,
            actor_handles=actor_handles,
            cuda_stream=torch_stream,
            use_communication_streams=use_communication_streams,
        )

    def get_communication_id(self):
        """Get a communicator unique id"""
        from ray.experimental.channel.nccl_group import get_unique_id

        return get_unique_id()
