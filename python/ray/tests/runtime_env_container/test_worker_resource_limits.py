import argparse
import os
from pathlib import Path

import ray

parser = argparse.ArgumentParser()
parser.add_argument("--image", required=True)
args = parser.parse_args()

ray.init(
    num_cpus=4,
    _system_config={"worker_resource_limits_enabled": True},
)


@ray.remote(runtime_env={"image_uri": args.image})
def read_limits():
    return {
        "pid": os.getpid(),
        "cpu": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
        "memory": Path("/sys/fs/cgroup/memory.max").read_text().strip(),
    }


def run_with_limits(cpus, memory_bytes):
    result = ray.get(read_limits.options(num_cpus=cpus, memory=memory_bytes).remote())
    assert result["cpu"] == f"{round(cpus * 100000)} 100000", result
    assert result["memory"] == str(memory_bytes), result
    return result["pid"]


memory_bytes = 256 * 1024 * 1024
half_cpu_pid = run_with_limits(0.5, memory_bytes)
assert run_with_limits(0.5, memory_bytes) == half_cpu_pid

one_cpu_pid = run_with_limits(1.0, memory_bytes)
one_and_half_cpu_pid = run_with_limits(1.5, memory_bytes)
different_memory_pid = run_with_limits(0.5, 512 * 1024 * 1024)

assert len({half_cpu_pid, one_cpu_pid, one_and_half_cpu_pid, different_memory_pid}) == 4
