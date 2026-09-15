"""Logical memory reservation for V2 read tasks.

A read task's heap footprint is dominated by the block it produces, and for
Parquet the footer's ``SizeStatistics`` tell us that block's Arrow size exactly
(see :mod:`~ray.data._internal.datasource_v2.chunkers.parquet_decoded_size`).
That makes it worth reserving Ray ``memory`` up front rather than leaving read
tasks to the operator-level default, which is a flat per-CPU number that has to
assume the worst.
"""

from typing import Any, Dict

from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.util import MiB
from ray.data.context import DataContext
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# What a read worker costs before it touches any data: interpreter, the
# ``ray`` / ``pyarrow`` / ``numpy`` / ``pandas`` imports, and allocator warmup.
# Measured at 168-189 MiB of peak worker RSS reading an 8 KiB file on Linux
# (glibc 2.41), rounded up for headroom since the real figure grows with
# whatever else a job imports into its workers.
READ_TASK_BASE_MEMORY = 256 * MiB

# How much peak RSS a read task adds per Arrow byte of the block it produces.
# Measured against real Ray worker peak RSS on Linux, fitting peak against
# target block size across a 16-type matrix: the slope ranges from 3.7x for flat
# ``int64`` up to 8.1x for 50 KiB strings.
#
# It exceeds 1x because the transient decode state peaks *alongside* the output
# block rather than replacing it -- encoded pages, batches in flight, and the
# copy made when the block builder seals the block. The spread is by column
# type, so this takes the steepest measured slope; the alternative is
# under-reserving the very workloads (wide lists, tensors, large strings) most
# likely to exhaust a worker.
READ_TASK_MEMORY_PER_DECODED_BYTE = 8


def read_task_memory(max_partition_decoded_bytes: int) -> int:
    """Bytes of Ray ``memory`` to reserve for one read task.

    Args:
        max_partition_decoded_bytes: Upper bound on the Arrow size of the data a
            single read task handles.

    Returns:
        The reservation, in bytes.
    """
    return (
        READ_TASK_BASE_MEMORY
        + READ_TASK_MEMORY_PER_DECODED_BYTE * max_partition_decoded_bytes
    )


def apply_read_task_memory(
    ray_remote_args: Dict[str, Any],
    partitioner: FilePartitioner,
    ctx: DataContext,
) -> Dict[str, Any]:
    """Add a ``memory`` reservation to ``ray_remote_args`` when we can size one.

    No-ops unless the partitioner can bound a partition's decoded size, which
    today means the Parquet footer path -- every other partitioner sizes
    partitions from an encoding-ratio guess that isn't worth reserving against.

    Args:
        ray_remote_args: Remote args for the read op. Not mutated.
        partitioner: The partitioner grouping listing rows into read tasks.
        ctx: The active data context, consulted for scheduling strategy.

    Returns:
        ``ray_remote_args``, or a copy carrying an added ``memory`` key.
    """
    # An explicit `memory=` from the caller wins.
    if "memory" in ray_remote_args:
        return ray_remote_args

    max_bytes = partitioner.max_partition_decoded_bytes
    if not max_bytes:
        return ray_remote_args

    # Reserving memory inside a placement group that reserves none would leave
    # read tasks permanently unschedulable. ``ConfigureMapTaskMemoryRule``
    # declines for the same reason.
    if any(
        isinstance(strategy, PlacementGroupSchedulingStrategy)
        for strategy in (
            ray_remote_args.get("scheduling_strategy"),
            ctx.scheduling_strategy,
            ctx.scheduling_strategy_large_args,
        )
    ):
        return ray_remote_args

    return {**ray_remote_args, "memory": read_task_memory(max_bytes)}
