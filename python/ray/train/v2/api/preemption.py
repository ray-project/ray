from dataclasses import dataclass
from typing import Dict, List, Optional

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class PreemptionInfo:
    """Information about an imminent preemption event.

    Attributes:
        deadline_ms: Earliest preemption deadline (UNIX time in milliseconds)
            across all preempted nodes. ``None`` if no deadline was reported.
        preempted_node_to_ranks: Map of each preempted ``node_id`` to the
            affected worker world ranks when that node is preempted.
    """

    deadline_ms: Optional[int]
    preempted_node_to_ranks: Dict[str, List[int]]

    @property
    def preempted_node_ids(self) -> List[str]:
        """Preempted node IDs, sorted lexicographically."""
        return sorted(self.preempted_node_to_ranks)

    @property
    def preempted_ranks(self) -> List[int]:
        """All affected ranks across the preempted nodes, sorted ascending."""
        return sorted(
            {r for ranks in self.preempted_node_to_ranks.values() for r in ranks}
        )
