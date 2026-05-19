"""Preemption signal data types.

This module is the source of truth for the PreemptionInfo struct that flows
from Ray Core's drain signal -> Ray Train controller -> worker actor ->
TrainContext -> user UDF via ``ray.train.preemption_status()``.
"""

import time
from dataclasses import dataclass, field
from typing import List

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class PreemptionInfo:
    """Information about an in-flight preemption.

    Returned from :func:`ray.train.preemption_status` when one or more workers
    in the current worker group have been marked for preemption by the
    underlying scheduler (e.g., GKE node drain). Frozen / immutable.

    Args:
        deadline: UNIX timestamp by which preempted hosts will be reclaimed.
            ``float("inf")`` means no deadline was reported by Ray Core.
        reason: Free-form reason string from the signal source (e.g., "drain",
            "scheduled_maintenance").
        preempted_ranks: All worker ranks whose hosts are being reclaimed.
            Includes ranks sharing a failure domain (e.g., the entire TPU slice)
            even if their own host has not yet entered drain.
        preempted_node_ids: Corresponding Ray node IDs (hex strings).

    Example:

        .. code-block:: python

            info = ray.train.preemption_status()
            if info is not None:
                rank = ray.train.get_context().get_world_rank()
                if rank in info.preempted_ranks:
                    cleanup_local()
                else:
                    save_jit_checkpoint(state)
                return
    """

    deadline: float
    reason: str
    preempted_ranks: List[int] = field(default_factory=list)
    preempted_node_ids: List[str] = field(default_factory=list)

    @property
    def seconds_remaining(self) -> float:
        """Time remaining until the (earliest) preemption deadline. Computed
        live on access so it does not go stale as wall-clock advances."""
        if self.deadline == float("inf"):
            return float("inf")
        return max(0.0, self.deadline - time.time())
