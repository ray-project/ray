"""A few util functions for physical mode execution.
NOTE:
1. It should be only used under linux environment; for other platforms, all functions
are no-op.
2. It assumes cgroup v2 has been mounted with rw permission.
"""
# TODO(hjiang):
# 1. Consider whether we need to support V1.
# 2. Check the existence for cgroup V2.
# 3. Add error handling; for now just use boolean return to differentiate success or
# failure. Better to log the error.

from ray._private.runtime_env import physical_mode_context

import sys
import os
import shutil

# There're two types of memory cgroup constraints:
# 1. For those with limit capped, they will be created a dedicated cgroup;
# 2. For those without limit specified, they will be added to the default cgroup.
DEFAULT_CGROUP_UUID = "default_cgroup_uuid"


def _setup_new_cgroup(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Setup a new cgroup for consumption. Return whether constraint is applied
    successfully."""

    # Sanity check.
    if not physical_exec_ctx.uuid:
        return False
    if physical_exec_ctx.uuid == DEFAULT_CGROUP_UUID:
        return False

    cgroup_folder = (
        f"{physical_exec_ctx.application_cgroup_path}/{physical_exec_ctx.uuid}"
    )
    os.makedirs(cgroup_folder, exist_ok=True)
    pid = os.getpid()

    with open(f"{cgroup_folder}/cgroup.procs", "w") as f:
        f.write(str(pid))

    if physical_exec_ctx.min_memory:
        with open(f"{cgroup_folder}/memory.min", "w") as f:
            f.write(str(physical_exec_ctx.min_memory))

    if physical_exec_ctx.max_memory:
        with open(f"{cgroup_folder}/memory.max", "w") as f:
            f.write(str(physical_exec_ctx.max_memory))

    return True


def _update_default_cgroup_for_at_setup(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Add current process to the default cgroup, and update reserved resource if
    necessary."""
    # Sanity check.
    if physical_exec_ctx.uuid != DEFAULT_CGROUP_UUID:
        return False
    assert physical_exec_ctx.max_memory is None

    cgroup_folder = f"{physical_exec_ctx.application_cgroup_path}/{DEFAULT_CGROUP_UUID}"
    os.makedirs(cgroup_folder, exist_ok=True)
    pid = os.getpid()

    cgroup_procs_path = f"{cgroup_folder}/cgroup.procs"
    with open(cgroup_procs_path, "a") as f:
        f.write(f"{pid}\n")

    if not physical_exec_ctx.min_memory:
        return True

    cur_memory_min = 0
    if os.path.exists(f"{cgroup_folder}/memory.min"):
        with open(f"{cgroup_folder}/memory.min", "r") as f:
            cur_memory_min = int(f.read().strip())

    new_memory_min = cur_memory_min + physical_exec_ctx.min_memory
    with open(f"{cgroup_folder}/memory.min", "w") as f:
        f.write(str(new_memory_min))

    return True


def _setup_cgroup(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Setup cgroup for consumption. Return whether constraint is applied
    successfully."""
    # Create a new dedicated cgroup if resource max specified.
    if physical_exec_ctx.max_memory:
        return _setup_new_cgroup(physical_exec_ctx)

    # Otherwise, place the process to the default cgroup.
    return _update_default_cgroup_for_at_setup(physical_exec_ctx)


def setup_cgroup_for_context(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Setup cgroup for the partcular execution context."""
    if sys.platform == "win32":
        return True

    if not _setup_cgroup(physical_exec_ctx):
        return False

    return True


def _cleanup_dedicated_cgroup(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Cleanup the dedicated cgroup."""
    if physical_exec_ctx.uuid == DEFAULT_CGROUP_UUID:
        return False

    cgroup_folder = (
        f"{physical_exec_ctx.application_cgroup_path}/{physical_exec_ctx.uuid}"
    )
    shutil.rmtree(cgroup_folder)

    return True


def _cleanup_default_cgroup(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Cleanup the default cgroup for the given context."""

    if physical_exec_ctx.uuid != DEFAULT_CGROUP_UUID:
        return False

    cgroup_folder = f"{physical_exec_ctx.application_cgroup_path}/{DEFAULT_CGROUP_UUID}"
    cur_memory_min = 0
    if not os.path.exists(f"{cgroup_folder}/memory.min"):
        return False

    with open(f"{cgroup_folder}/memory.min", "r") as f:
        cur_memory_min = int(f.read().strip())

    if cur_memory_min < physical_exec_ctx.min_memory:
        return False

    new_memory_min = cur_memory_min - physical_exec_ctx.min_memory
    with open(f"{cgroup_folder}/memory.min", "w") as f:
        f.write(str(new_memory_min))

    return True


def _cleanup_cgroup(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Cleanup cgroup for memory consumption. Return whether cleanup opertion succeeds
    or not."""
    # Cleanup a new dedicated cgroup if max memory specified.
    if physical_exec_ctx.max_memory:
        return _cleanup_dedicated_cgroup(physical_exec_ctx)

    # Cleanup the default memory cgroup.
    return _cleanup_default_cgroup(physical_exec_ctx)


def cleanup_cgroup_for_context(
    physical_exec_ctx: physical_mode_context.PhysicalModeExecutionContext,
) -> bool:
    """Cleanup cgroup for the particular execution context."""
    if sys.platform == "win32":
        return True

    if not _cleanup_cgroup(physical_exec_ctx):
        return False

    return True
