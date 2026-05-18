"""Preemption signal data types and helpers.

This module is the source of truth for the PreemptionInfo struct that flows
from Ray Core's drain signal -> Ray Train controller -> worker actor ->
TrainContext -> user UDF via ``ray.train.preemption_status()``.
"""

import time
from dataclasses import dataclass, field
from typing import List, Optional

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
        this_worker_preempted: Convenience: whether the calling worker is in
            ``preempted_ranks``. Filled in per-worker by the controller before
            the ``mark_preempt`` RPC is sent.
    """

    deadline: float
    reason: str
    preempted_ranks: List[int] = field(default_factory=list)
    preempted_node_ids: List[str] = field(default_factory=list)
    this_worker_preempted: bool = False

    @property
    def seconds_remaining(self) -> float:
        """Time remaining until the (earliest) preemption deadline. Computed
        live on access so it does not go stale as wall-clock advances."""
        if self.deadline == float("inf"):
            return float("inf")
        return max(0.0, self.deadline - time.time())


def make_preemption_info_for_worker(
    base: PreemptionInfo, worker_rank: int
) -> PreemptionInfo:
    """Return a copy of ``base`` with ``this_worker_preempted`` set for the
    given rank. Used by PreemptionCallback when fanning out mark_preempt RPCs."""
    from dataclasses import replace

    return replace(
        base, this_worker_preempted=(worker_rank in base.preempted_ranks)
    )
