import logging
import threading
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set

import ray
from ray.train.v2._internal.constants import DEFAULT_PREEMPTION_POLL_INTERVAL_S
from ray.util.tpu import get_tpu_slice_name_from_node

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PreemptionInfo:
    """Information about an imminent preemption event.

    Attributes:
        deadline_ms: UNIX timestamp in milliseconds by which the affected nodes
            are expected to be reclaimed, as reported by Ray Core's
            ``get_draining_nodes``. ``0`` means the source reported no deadline.
            (Divide by 1000 to compare against ``time.time()``.)
        preempted_ranks: Worker `world_rank` whose nodes are being reclaimed, expanded to all ranks in the same failure domain
            (e.g., all ranks in a fate-shared TPU slice). Sorted ascending.
        preempted_node_ids: Node IDs being reclaimed, hex-encoded as returned
            by Ray Core. Sorted lexicographically.
    """

    deadline_ms: int
    preempted_ranks: List[int]
    preempted_node_ids: List[str]


# Drain source contract: () -> {node_id_hex: deadline_ms}, where deadline_ms
# == 0 means "no deadline reported". The default reads Ray Core's GCS state;
# unit tests inject a synthetic source.
DrainSource = Callable[[], Dict[str, int]]


def _default_drain_source() -> Dict[str, int]:
    return ray._private.state.state.get_draining_nodes()


class PreemptionWatcher:
    """Polls Ray Core for node drains and logs detected preemption events.

    One watcher per worker group, spawned as a ``num_cpus=0`` actor by
    ``PreemptionCallback``. The poll loop runs in a background thread. The
    failure-domain map is built once on construction and is immutable for the
    watcher's lifetime.

    Args:
        node_to_ranks: Map ``node_id_hex -> [ranks on that node]``. Used both
            as the set of nodes we care about (drains elsewhere are ignored)
            and as the seed for failure-domain expansion.
        poll_interval_s: Seconds between drain-state polls.
        drain_source: Override the default drain source (used by unit tests).
    """

    def __init__(
        self,
        node_to_ranks: Dict[str, List[int]],
        poll_interval_s: float = DEFAULT_PREEMPTION_POLL_INTERVAL_S,
        drain_source: Optional[DrainSource] = None,
    ):
        self._node_to_ranks: Dict[str, List[int]] = {
            nid: sorted(ranks) for nid, ranks in node_to_ranks.items()
        }
        self._poll_interval_s = poll_interval_s
        self._drain_source = drain_source or _default_drain_source
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

        # Wrap the whole build: any failure (GCS unavailable, a malformed node
        # entry, a TPU-label lookup error) must fall back to per-node domains
        # rather than fail the actor's __init__ — which, under max_restarts=-1,
        # would otherwise spin in a restart loop.
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

    def get_latest_info(self) -> Optional[PreemptionInfo]:
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
            drained = self._drain_source() or {}
            # Only consider drains on this job's own nodes. That's complete for
            # TPU — an SPMD job fully occupies its slice, so every fate-shared
            # host is one of our nodes and a drain on any slice host appears
            # here. For GPU, a drain on a host we don't run on is correctly
            # irrelevant.
            relevant = {
                n: d for n, d in drained.items() if n in self._failure_domain_map
            }
            if relevant != self._last_drained:
                self._on_drain_change(relevant)
                self._last_drained = relevant
        except Exception:
            logger.warning("PreemptionWatcher poll failed", exc_info=True)

    def _on_drain_change(self, drained: Dict[str, int]) -> None:
        """Handle a change in the (already-filtered) drained-node set."""
        if not drained:
            return

        affected_node_ids = sorted(drained.keys())
        affected_ranks = sorted(
            {
                r
                for node_id in affected_node_ids
                for r in self._failure_domain_map[node_id]
            }
        )

        # Earliest non-zero deadline wins; 0 means "unknown" when no node
        # reported a deadline. ``or 0`` guards against a None/missing value.
        nonzero_deadlines = [
            drained[n] for n in affected_node_ids if (drained[n] or 0) > 0
        ]
        deadline_ms = min(nonzero_deadlines) if nonzero_deadlines else 0

        info = PreemptionInfo(
            deadline_ms=deadline_ms,
            preempted_ranks=affected_ranks,
            preempted_node_ids=affected_node_ids,
        )
        self._latest_info = info

        logger.warning(
            "PreemptionWatcher: drain detected — "
            "preempted_node_ids=%s, preempted_ranks=%s, deadline_ms=%s",
            affected_node_ids,
            affected_ranks,
            deadline_ms,
        )
        # TODO(lehui): fan out mark_preempt(info) to the worker actors here
        # so the UDF can read it via ray.train.preemption_status().
