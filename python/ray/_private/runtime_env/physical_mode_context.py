"""Context for physial mode execution.
"""

from dataclasses import dataclass

from typing import Optional


@dataclass
class PhysicalModeExecutionContext:
    """Specs for physical mode execution."""

    # Memory related spec.
    # UUID here is to indicate cgroup path, it's necessary to enable cgroup-based
    # memory control.
    memory_cgroup_uuid: str = None
    # Unit: bytes. Corresponds to cgroup V2 `memory.max`, which enforces hard cap on
    # max memory consumption.
    max_memory: Optional[int] = None
    # Unit: bytes. Corresponds to cgroup V2 `memory.min`, which enforces hard cap on
    # min memory reservation.
    min_memory: Optional[int] = None
