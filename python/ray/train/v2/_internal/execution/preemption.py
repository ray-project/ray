import logging
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

import ray
from ray.train.v2._internal.constants import DEFAULT_PREEMPTION_POLL_INTERVAL_S
from ray.util.tpu import get_tpu_slice_name_from_node

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PreemptionInfo:
    """Information about an imminent preemption event.

    Attributes:
        deadline_ms: Earliest preemption deadline (UNIX time in milliseconds)
            across all preempted nodes. ``None`` if no deadline was reported.
        preempted_node_to_ranks: Map of preempted ``node_id`` to the worker ``world_rank``s affected when that node
            is preempted.
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


def _get_draining_nodes() -> Dict[str, int]:
    """Ray Core's draining nodes as ``{node_id_hex: deadline_ms}`` (0 = no deadline)."""
    return ray._private.state.state.get_draining_nodes()


class PreemptionWatcher:
    """Polls Ray Core for node drains and logs detected preemption events.

    One watcher per worker group, spawned as a ``num_cpus=0`` actor by
    ``PreemptionCallback``. The poll loop runs in a background thread. The
    failure-domain map is built once on construction and is immutable for the
    watcher's lifetime.

    The failure-domain map records which of our ranks are affected if a node is
    preempted: for a GPU node, the ranks on that node; for a TPU node, every
    rank in the node's slice, since a TPU slice is preempted atomically.

    Args:
        node_to_ranks: Map ``node_id_hex -> [ranks on that node]``. Used both
            as the set of nodes we care about (drains elsewhere are ignored)
            and as the seed for failure-domain expansion.
        poll_interval_s: Seconds between drain-state polls.
    """

    def __init__(
        self,
        node_to_ranks: Dict[str, List[int]],
        poll_interval_s: float = DEFAULT_PREEMPTION_POLL_INTERVAL_S,
    ):
        self._node_to_ranks: Dict[str, List[int]] = {
            nid: sorted(ranks) for nid, ranks in node_to_ranks.items()
        }
        self._poll_interval_s = poll_interval_s
        self._failure_domain_map: Dict[str, List[int]] = self._build_failure_domain_map(
            self._node_to_ranks
        )

        self._stop_event = threading.Event()
        self._last_drained: Dict[str, int] = {}
        self._latest_info: Optional[PreemptionInfo] = None

        self._monitor_thread = threading.Thread(
            target=self._watch_loop,
            name="PreemptionWatcher",
            daemon=True,
        )
        self._monitor_thread.start()

    @staticmethod
    def _build_failure_domain_map(
        node_to_ranks: Dict[str, List[int]],
    ) -> Dict[str, List[int]]:
        """Map each node we host to all ranks in its failure domain.

        - Non-TPU (e.g. GPU) clusters: the failure domain is the node itself,
          so a drain on a node flags only the ranks this job runs there.
        - TPU multislice: every host in a slice is reclaimed atomically, so a
          drain on any host is fate-shared with the rest.
        """
        per_node = {nid: sorted(set(ranks)) for nid, ranks in node_to_ranks.items()}

        try:
            all_nodes = ray.nodes()

            # Slice label for each node we host (None for non-TPU nodes).
            node_to_slice: Dict[str, Optional[str]] = {
                node["NodeID"]: get_tpu_slice_name_from_node(node)
                for node in all_nodes
                if node["NodeID"] in node_to_ranks
            }

            # Union our ranks per slice.
            slice_to_ranks: Dict[str, Set[int]] = {}
            for node_id, ranks in node_to_ranks.items():
                slice_label = node_to_slice.get(node_id)
                if slice_label:
                    slice_to_ranks.setdefault(slice_label, set()).update(ranks)

            # Non-TPU cluster (or none of our nodes are on a slice): per-node.
            if not slice_to_ranks:
                return per_node

            result: Dict[str, List[int]] = {}
            for node_id, ranks in node_to_ranks.items():
                slice_label = node_to_slice.get(node_id)
                if slice_label:
                    result[node_id] = sorted(slice_to_ranks[slice_label])
                else:
                    result[node_id] = sorted(set(ranks))
            return result
        except Exception:
            logger.debug(
                "Could not build failure-domain map; falling back to per-node "
                "domains (no TPU-slice expansion).",
                exc_info=True,
            )
            return per_node

    def get_latest_preemption_info(self) -> Optional[PreemptionInfo]:
        """Most recent :class:`PreemptionInfo` observed, or ``None``."""
        return self._latest_info

    def _watch_loop(self) -> None:
        logger.debug(
            "PreemptionWatcher polling %d node(s) every %.1fs.",
            len(self._node_to_ranks),
            self._poll_interval_s,
        )
        while not self._stop_event.is_set():
            self._poll_once()
            self._stop_event.wait(timeout=self._poll_interval_s)
        logger.debug("PreemptionWatcher stopped.")

    def _poll_once(self) -> None:
        """Poll the drain source once and dispatch on change.

        Per-poll exceptions are caught and logged so a transient GCS hiccup
        doesn't kill the watcher loop.
        """
        try:
            drained = _get_draining_nodes() or {}
            # Keep only drains on this job's own nodes (others are ignored).
            # That's complete for TPU — an SPMD job fully occupies its slice, so
            # every fate-shared host is one of our nodes and a drain on any slice
            # host appears here. For GPU, a drain on a host we don't run on is
            # correctly irrelevant.
            relevant = {
                n: d for n, d in drained.items() if n in self._failure_domain_map
            }
            if relevant != self._last_drained:
                self._on_drain_change(relevant)
                self._last_drained = relevant
        except Exception:
            # TODO(lehui): consider exponential backoff when the drain API keeps
            # failing, instead of retrying at the fixed poll interval.
            logger.warning("PreemptionWatcher poll failed", exc_info=True)

    def _on_drain_change(self, drained: Dict[str, int]) -> None:
        """Handle a change in the drained-node set.

        ``drained`` has already been narrowed to this job's nodes by the
        caller (``_poll_once``).
        """
        if not drained:
            return

        affected_node_ids = sorted(drained.keys())
        preempted_node_to_ranks = {
            node_id: self._failure_domain_map[node_id] for node_id in affected_node_ids
        }

        # Earliest deadline across the preempted nodes; None if none reported one
        # (Ray Core uses 0 for "no deadline", which is falsy and filtered out).
        reported_deadlines = [drained[n] for n in affected_node_ids if drained[n]]
        deadline_ms = min(reported_deadlines) if reported_deadlines else None

        info = PreemptionInfo(
            deadline_ms=deadline_ms,
            preempted_node_to_ranks=preempted_node_to_ranks,
        )
        self._latest_info = info

        logger.warning(
            "PreemptionWatcher: preemption detected — "
            "preempted_node_ids=%s, preempted_ranks=%s, deadline_ms=%s",
            info.preempted_node_ids,
            info.preempted_ranks,
            deadline_ms,
        )
        # TODO(lehui): forward the detected preemption to the workers so the
        # training loop can react to it.
        # TODO(lehui): coalesce preemptions seen within one window into a single
        # worker-group restart, so a staggered drain (node A at t, node B at
        # t+60s) doesn't cause back-to-back restarts.
