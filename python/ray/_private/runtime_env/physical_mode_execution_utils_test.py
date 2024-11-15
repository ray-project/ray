import uuid
import pytest
from datetime import datetime
import sys
import os

_TIMES = 100000


class PhysicalModeExecutionContext:
    """Specs for physical mode execution."""

    # Memory related spec.
    #
    # Whether this particular memory context is shared with other processes.
    # As the initial version, we only supports two forms:
    # 1. Exclusive, which means we place one process into a cgroup control memory usage
    # 2. Shared, which means a bunch of processes are grouped into one cgroup.
    # UUID here is to indicate cgroup path, it's necessary to enable cgroup-based
    # memory control.
    memory_cgroup_uuid: str = None
    # Unit: bytes. Corresponds to cgroup V2 `memory.high`, which is used to trigger
    # reclamation for unreferenced memory.
    high_memory: int = None
    # Unit: bytes. Corresponds to cgroup V2 `memory.max`, which enforces hard cap on
    # max memory consumption.
    max_memory: int = None
    # Unit: bytes. Corresponds to cgroup V2 `memory.low`, which is used to prevent
    # reclamation for unreferenced memory.
    low_memory: int = None
    # Unit: bytes. Corresponds to cgroup V2 `memory.min`, which enforces hard cap on
    # min memory reservation.
    min_memory: int = None


def setup_cgroup_for_memory(
    physical_exec_ctx: PhysicalModeExecutionContext,
) -> None:
    """Setup cgroup for memory consumption."""
    if not physical_exec_ctx.memory_cgroup_uuid:
        return True

    cgroup_path = f"/sys/fs/cgroup/{physical_exec_ctx.memory_cgroup_uuid}"
    os.makedirs(cgroup_path, exist_ok=True)
    pid = os.getpid()

    with open(f"{cgroup_path}/cgroup.procs", "w") as f:
        f.write(str(pid))

    if physical_exec_ctx.low_memory:
        with open(f"{cgroup_path}/memory.low", "w") as f:
            f.write(str(physical_exec_ctx.low_memory))

    if physical_exec_ctx.min_memory:
        with open(f"{cgroup_path}/memory.min", "w") as f:
            f.write(str(physical_exec_ctx.min_memory))

    if physical_exec_ctx.high_memory:
        with open(f"{cgroup_path}/memory.high", "w") as f:
            f.write(str(physical_exec_ctx.high_memory))

    if physical_exec_ctx.max_memory:
        with open(f"{cgroup_path}/memory.max", "w") as f:
            f.write(str(physical_exec_ctx.max_memory))

    return True


class TestRuntime:
    def test_runtime(self):
        current_timestamp = datetime.now().timestamp()
        print(current_timestamp)

        for _ in range(_TIMES):
            unique_id = uuid.uuid4()
            ctx = PhysicalModeExecutionContext()
            ctx.memory_cgroup_uuid = unique_id
            ctx.high_memory = 1024 * 1024
            setup_cgroup_for_memory(ctx)

        current_timestamp = datetime.now().timestamp()
        print(current_timestamp)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
