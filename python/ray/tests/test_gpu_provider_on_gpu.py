import os
import sys

import pytest
import torch

from ray._common.test_utils import wait_for_condition
from ray.dashboard.modules.reporter.gpu_providers import (
    NvidiaGpuProvider,
)


def _candidate_pids() -> set[int]:
    # NVML may report PIDs from a different namespace when this runs in a
    # container. /proc/self/status exposes the PID namespace chain, so accept
    # any PID that can refer to this process.
    pids = {os.getpid()}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("NSpid:"):
                    pids.update(int(pid) for pid in line.split()[1:])
                    break
    except (OSError, ValueError):
        pass
    return pids


def _find_process_info(gpu_utilization, candidate_pids, tensor_size_mb):
    fallback_match = None

    for single_gpu_info in gpu_utilization:
        procs = single_gpu_info["processes_pids"] or {}
        for pid in candidate_pids:
            if pid in procs:
                return single_gpu_info, procs[pid]

        for process_info in procs.values():
            if process_info["gpu_memory_usage"] >= tensor_size_mb:
                if (
                    fallback_match is None
                    or process_info["gpu_memory_usage"]
                    < fallback_match[1]["gpu_memory_usage"]
                ):
                    fallback_match = (single_gpu_info, process_info)

    return fallback_match


def test_per_process_gpu_memory_usage_and_total_gpu_memory_usage():
    """
    Allocate a large tensor on GPU, then verify the provider reports process
    memory and overall gpu memory consistent with that.
    """

    device = torch.device("cuda:0")
    tensor_size_mb = 2048
    num_elements = tensor_size_mb * 1024 * 1024
    tensor = torch.zeros(num_elements, dtype=torch.int8, device=device)
    torch.cuda.synchronize(device)
    assert tensor.size(0) == num_elements

    provider = NvidiaGpuProvider()
    candidate_pids = _candidate_pids()

    def check_utilization():
        result = provider.get_gpu_utilization()
        assert len(result) > 0

        # Find the process and GPU that correspond to the tensor allocation.
        match = _find_process_info(result, candidate_pids, tensor_size_mb)
        assert match is not None
        gpu_info, process_info = match

        reported_proc_mb = process_info["gpu_memory_usage"]
        # Proc memory usage should be at least the tensor size and within a
        # reasonable amount of CUDA context overhead.
        assert (
            reported_proc_mb >= tensor_size_mb
            and reported_proc_mb - tensor_size_mb < 1024
        )
        assert gpu_info["memory_used"] >= reported_proc_mb
        return True

    wait_for_condition(check_utilization)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
