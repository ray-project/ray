from unittest import mock

import torch

import ray
import ray.dag
import ray.experimental.channel as ray_channel
from ray.experimental.channel.cpu_nccl_group import CPUNcclGroup

class MockCudaStream:
    def __init__(self):
        self.cuda_stream = 0

def start_nccl_mock():
    """
    Patch methods that require CUDA.
    """
    # Mock cupy dependencies.
    nccl_mock = mock.MagicMock()
    nccl_mock.nccl.get_unique_id.return_value = 0
    cp_patcher = mock.patch.dict(
        "sys.modules",
        {
            "cupy.cuda": nccl_mock,
            "cupy": mock.MagicMock(),
            "ray.util.collective.collective_group": mock.MagicMock(),
        },
    )
    cp_patcher.start()

    # Mock send/recv ops to use an actor instead of NCCL.
    ray.experimental.channel.torch_tensor_nccl_channel._NcclGroup = CPUNcclGroup

    # PyTorch mocks.
    stream_patcher = mock.patch(
        "torch.cuda.current_stream", new_callable=lambda: MockCudaStream
    )
    stream_patcher.start()
    new_stream_patcher = mock.patch(
        "torch.cuda.Stream", new_callable=lambda: MockCudaStream
    )
    new_stream_patcher.start()
    tensor_patcher = mock.patch("torch.Tensor.device", torch.device("cuda"))
    tensor_patcher.start()
    tensor_patcher = mock.patch("torch.Tensor.is_cuda", True)
    tensor_patcher.start()
    tensor_allocator_patcher = mock.patch(
        "ray.experimental.channel.torch_tensor_nccl_channel._torch_zeros_allocator",
        lambda shape, dtype: torch.zeros(shape, dtype=dtype),
    )
    tensor_allocator_patcher.start()

    ctx = ray_channel.ChannelContext.get_current()
    ctx.set_torch_device(torch.device("cuda"))


class TracedChannel(ray_channel.shared_memory_channel.Channel):
    """
    Patched Channel that records all write ops for testing.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ops = []

    def write(self, *args, **kwargs):
        self.ops.append((args, kwargs))
        return super().write(*args, **kwargs)
