import pytest
import torch

import ray
import ray.train.torch.train_loop_utils


@pytest.fixture
def ray_start_1_cpu_1_gpu():
    address_info = ray.init(num_cpus=1, num_gpus=1)
    yield address_info
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.parametrize(
    ("device_choice", "auto_transfer"),
    [
        ("cpu", True),
        ("cpu", False),
        ("cuda", True),
        ("cuda", False),
    ],
)
def test_auto_transfer_data_from_host_to_device(
    ray_start_1_cpu_1_gpu, device_choice, auto_transfer
):
    import numpy as np
    import torch

    def compute_average_runtime(func):
        device = torch.device(device_choice)
        start = torch.cuda.Event(enable_timing=True)
        end = torch.cuda.Event(enable_timing=True)
        runtime = []
        for _ in range(10):
            torch.cuda.synchronize()
            start.record()
            func(device)
            end.record()
            torch.cuda.synchronize()
        runtime.append(start.elapsed_time(end))
        return np.mean(runtime)

    small_dataloader = [
        (torch.randn((1024 * 4, 1024 * 4), device="cpu"),) for _ in range(10)
    ]

    def host_to_device(device):
        for (x,) in small_dataloader:
            x = x.to(device)
            torch.matmul(x, x)

    def host_to_device_auto_pipeline(device):
        wrapped_dataloader = ray.train.torch.train_loop_utils._WrappedDataLoader(
            small_dataloader, device, auto_transfer
        )
        for (x,) in wrapped_dataloader:
            torch.matmul(x, x)

    # test if all four configurations are okay
    with_auto_transfer = compute_average_runtime(host_to_device_auto_pipeline)

    if device_choice == "cuda" and auto_transfer:
        assert compute_average_runtime(host_to_device) >= with_auto_transfer


def test_auto_transfer_correct_device(ray_start_4_cpus_2_gpus):
    """Tests that auto_transfer uses the right device for the cuda stream."""
    import pynvml

    pynvml.nvmlInit()

    def get_gpu_used_mem(i):
        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
        info = pynvml.nvmlDeviceGetMemoryInfo(handle)
        return info.used

    start_gpu_memory = get_gpu_used_mem(1)

    device = torch.device("cuda:1")
    small_dataloader = [(torch.randn((1024 * 4, 1024 * 4)),) for _ in range(10)]
    wrapped_dataloader = (  # noqa: F841
        ray.train.torch.train_loop_utils._WrappedDataLoader(
            small_dataloader, device, True
        )
    )

    end_gpu_memory = get_gpu_used_mem(1)

    # Verify GPU memory usage increases on the right cuda device
    assert end_gpu_memory > start_gpu_memory


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
