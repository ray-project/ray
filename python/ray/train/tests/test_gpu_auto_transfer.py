import numpy as np
import pytest
import torch

from ray.train.torch.train_loop_utils import _WrappedDataLoader


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
        wrapped_dataloader = _WrappedDataLoader(small_dataloader, device, auto_transfer)
        for (x,) in wrapped_dataloader:
            torch.matmul(x, x)

    # test if all four configurations are okay
    with_auto_transfer = compute_average_runtime(host_to_device_auto_pipeline)

    if device_choice == "cuda" and auto_transfer:
        assert compute_average_runtime(host_to_device) >= with_auto_transfer


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
