"""A few util functions for physical mode execution.

NOTE:
1. It should be only used under linux environment; for other platforms, all functions
are no-op.
2. It assumes cgroup v2 has been mounted.
"""
# TODO(hjiang):
# 1. Consider whether we need to support V1.
# 2. Check the existence for cgroup V2.

from ray._private.runtime_env import physical_mode_context

import sys
import os


def setup_cgroup_for_memory(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
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


def setup_cgroup(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Setup cgroup for the partcular execution context.

    If an error happens, the function will log the error and return False.
    """
    if sys.platform == "win32":
        return True

    if not setup_cgroup_for_memory:
        return False

    return True
