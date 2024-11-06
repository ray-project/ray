"""Context for physial mode execution.

Refer to design doc:
https://docs.google.com/document/d/1oNG1U3FZhTO2o876M_UJm0IwA_QSfYNZ9ep4087Yjgc/edit?tab=t.0
"""

from dataclasses import dataclass


@dataclass
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
