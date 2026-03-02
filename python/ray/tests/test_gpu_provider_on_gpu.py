import os
import sys

import pytest
import torch

from ray.dashboard.modules.reporter.gpu_providers import (
    NvidiaGpuProvider,
)


def test_per_process_gpu_memory_usage_and_total_gpu_memory_usage():
    """
    Allocate a large tensor on GPU, then verify the provider reports process
    memory and overall gpu memory consistent with that.
    """

    device = torch.device("cuda:0")
    tensor_size_mb = 1024
    num_elements = tensor_size_mb * 1024 * 1024
    tensor = torch.zeros(num_elements, dtype=torch.int8, device=device)
    assert tensor.size(0) == num_elements

    provider = NvidiaGpuProvider()
    result = provider.get_gpu_utilization()
    assert len(result) > 0

    # Find the process and the gpu that corresponds to where the tensor was allocated
    pid = os.getpid()
    process_info = None
    gpu_info = None
    for single_gpu_info in result:
        procs = single_gpu_info["processes_pids"]
        if pid in procs:
            process_info = procs[pid]
            gpu_info = single_gpu_info
            break

    assert process_info is not None

    reported_proc_mb = process_info["gpu_memory_usage"]
    # Proc memory usage should be at least the tensor size and within 100mb
    assert reported_proc_mb - tensor_size_mb < 100
    # Check that gpu memory usage is >= proc gpu memory usage and within 100mb
    assert gpu_info["memory_used"] - reported_proc_mb < 100


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
