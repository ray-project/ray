"""Context for physial mode execution.
"""

from dataclasses import dataclass

from typing import Optional


@dataclass
class PhysicalModeExecutionContext:
    """Specs for physical mode execution."""

    # Cgroup for physical mode is organized as tree structure, all application cgroups
    # are placed under the given application cgroup node.
    application_cgroup_path: str = None
    # UUID to indicate current task / actor.
    uuid: str = None

    # Memory related spec.
    #
    # Unit: bytes. Corresponds to cgroup V2 `memory.max`, which enforces hard cap on
    # max memory consumption.
    max_memory: Optional[int] = None
    # Unit: bytes. Corresponds to cgroup V2 `memory.min`, which enforces hard cap on
    # min memory reservation.
    min_memory: Optional[int] = None
