import os
import sys

import pytest
import torch

from ray._common.test_utils import wait_for_condition
from ray.dashboard.modules.reporter.gpu_providers import (
    NvidiaGpuProvider,
)


def _host_pid() -> int:
    # NVML reports host-namespace PIDs. When running inside a PID-namespaced
    # container (e.g. CI), os.getpid() returns the container PID and will not
    # match anything NVML reports. /proc/self/status exposes the full NSpid
    # chain; the first entry is the outermost (host) namespace.
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("NSpid:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return os.getpid()


def test_per_process_gpu_memory_usage_and_total_gpu_memory_usage():
    """
    Allocate a large tensor on GPU, then verify the provider reports process
    memory and overall gpu memory consistent with that.
    """

    device = torch.device("cuda:0")
    tensor_size_mb = 2048
    num_elements = tensor_size_mb * 1024 * 1024
    tensor = torch.zeros(num_elements, dtype=torch.int8, device=device)
    assert tensor.size(0) == num_elements

    provider = NvidiaGpuProvider()

    def check_utilization():
        result = provider.get_gpu_utilization()
        assert len(result) > 0

        # Find the process and the gpu that corresponds to where the tensor was allocated
        pid = _host_pid()
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
        # Proc memory usage should be at least the tensor size and within 200mb
        assert (
            reported_proc_mb >= tensor_size_mb
            and reported_proc_mb - tensor_size_mb < 200
        )
        # Check that gpu memory usage is >= proc gpu memory usage and within 500mb
        assert (
            gpu_info["memory_used"] > reported_proc_mb
            and gpu_info["memory_used"] - reported_proc_mb < 500
        )
        return True

    wait_for_condition(check_utilization)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
