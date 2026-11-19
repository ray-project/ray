"""Shared pieces for the constraint-aware scheduling release tests
(constraint_label_scheduling.py / constraint_pg_scheduling.py).

The sleep-based UDF stages, a placement sweep over the state API, and a poller
that records each operator's max simultaneous ALIVE actors during the run (a
post-run snapshot would miss pools that already drained).

Actors are matched to operators through the state API's ``class_name``: Ray
Data creates a dynamic actor class per operator named
``MapWorker(<operator name>)``, so an actor belongs to a stage iff its
``class_name`` contains the stage's UDF class name. Keep the UDF class names
distinctive.
"""

import os
import threading
import time
from collections import defaultdict
from typing import Callable, Dict, List, Optional

import numpy as np

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.datasource import Datasink
from ray.util.state import list_actors, list_nodes

# Node label key set per worker pool in constraint_scheduling_labels_compute.yaml.
POOL_LABEL_KEY = "ray-pool"

LIST_ACTORS_LIMIT = 2_000

# ~1 MB per row (125K float64 elements x 8 bytes). Env-overridable so the
# same scripts can run as a lightweight local smoke on a dev box.
ROW_SIZE = int(os.environ.get("CONSTRAINT_TEST_ROW_SIZE_ELEMS", "125000"))


class GenStage:
    """Generate ~1 MB of data per row.

    A callable class (not a function) so autoscaling tuple ``concurrency``
    is accepted by ``map_batches``.
    """

    def __call__(self, batch):
        n = len(batch["id"])
        batch["data"] = [np.random.rand(ROW_SIZE) for _ in range(n)]
        return batch


class _SleepStage:
    """Base for the synthetic pipeline stages: sleep per batch, pass data through."""

    SLEEP_PER_BATCH_S = 0.0

    def __init__(self, sleep_s: Optional[float] = None):
        self._sleep_s = self.SLEEP_PER_BATCH_S if sleep_s is None else sleep_s

    def __call__(self, batch):
        time.sleep(self._sleep_s)
        batch["processed"] = [1] * len(batch["data"])
        return batch


class LabeledStage(_SleepStage):
    """The constrained stage; pinned to nodes via a per-op label_selector."""

    SLEEP_PER_BATCH_S = 2.0


class HogStage(_SleepStage):
    """Unconstrained CPU hog (autoscales toward one 5-CPU actor per node)."""

    SLEEP_PER_BATCH_S = 3.5


class FakeGPUInfer(_SleepStage):
    """Simulated GPU inference stage (logical GPUs only)."""

    SLEEP_PER_BATCH_S = 2.5

    def __init__(self, sleep_s: Optional[float] = None):
        super().__init__(sleep_s)
        # Simulate model loading.
        time.sleep(2)


class NullDatasink(Datasink):
    """Datasink that discards all data."""

    def write(self, blocks, ctx: TaskContext):
        for _ in blocks:
            pass


def node_labels_by_id() -> Dict[str, Dict[str, str]]:
    """node_id -> labels for all alive nodes."""
    return {
        n.node_id: (n.labels or {})
        for n in list_nodes(detail=True, limit=1000)
        if n.state == "ALIVE"
    }


def pool_nodes(pool: str) -> List[str]:
    """Sorted node ids of the alive nodes labeled ``ray-pool: <pool>``."""
    return sorted(
        node_id
        for node_id, labels in node_labels_by_id().items()
        if labels.get(POOL_LABEL_KEY) == pool
    )


def pick_pool_node(pool: str) -> str:
    """Deterministically pick one alive node from a labeled pool: the
    lexicographically smallest node id, so reruns on the same cluster pick
    the same node."""
    candidates = pool_nodes(pool)
    if not candidates:
        raise RuntimeError(f"No alive node with {POOL_LABEL_KEY}={pool!r}")
    return candidates[0]


def stage_actors(stage_cls_name: str, detail: bool = True) -> List:
    """All state-API actors belonging to the operator wrapping ``stage_cls_name``."""
    return [
        a
        for a in list_actors(detail=detail, limit=LIST_ACTORS_LIMIT)
        if stage_cls_name in (a.class_name or "")
    ]


# Actor states that count as requested-but-not-yet-running.
PENDING_ACTOR_STATES = ("DEPENDENCIES_UNREADY", "PENDING_CREATION")


class MaxAliveActorPoller:
    """Background thread recording each stage's actor high-water marks.

    ``max_alive`` is the max simultaneous ALIVE actors; ``max_requested`` is
    the max simultaneous ALIVE + PENDING actors. The latter catches a sizer
    that over-requests: actors that cannot fit sit in PENDING, so ALIVE alone
    would still report the capacity.

    The sizer may drain pools once their input is exhausted, so a post-run
    snapshot undercounts; sampling during the run captures the high-water mark.
    """

    def __init__(self, stage_cls_names: List[str], interval_s: float = 10.0):
        self._stage_cls_names = stage_cls_names
        self._interval_s = interval_s
        self.max_alive: Dict[str, int] = defaultdict(int)
        self.max_requested: Dict[str, int] = defaultdict(int)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _sample(self) -> None:
        try:
            actors = list_actors(detail=False, limit=LIST_ACTORS_LIMIT)
        except Exception as e:
            print(f"MaxAliveActorPoller: list_actors failed, skipping sample: {e}")
            return
        for stage in self._stage_cls_names:
            states = [a.state for a in actors if stage in (a.class_name or "")]
            alive = states.count("ALIVE")
            pending = sum(states.count(s) for s in PENDING_ACTOR_STATES)
            self.max_alive[stage] = max(self.max_alive[stage], alive)
            self.max_requested[stage] = max(self.max_requested[stage], alive + pending)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(self._interval_s)

    def __enter__(self) -> "MaxAliveActorPoller":
        self._thread.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self._stop.set()
        self._thread.join(timeout=30)
        # One final sample so short runs aren't missed entirely.
        self._sample()


def verify_stage_placement(
    stage_cls_name: str,
    is_allowed_node: Callable[[str, Dict[str, str]], bool],
    allowed_desc: str,
    require_nodes: Optional[List[str]] = None,
) -> Dict[str, object]:
    """Sweep the state API and check where a stage's actors actually ran.

    Args:
        stage_cls_name: UDF class name identifying the stage's actors.
        is_allowed_node: ``(node_id, labels) -> bool`` placement predicate.
        allowed_desc: Human-readable description of the allowed node set.
        require_nodes: Node ids that must each have hosted >= 1 actor.

    Returns:
        A metrics dict with an ``errors`` list (empty when placement is clean).
        DEAD actors are included: a mis-placed actor that already exited is
        still a placement violation.
    """
    labels_by_id = node_labels_by_id()
    actors = stage_actors(stage_cls_name)
    misplaced = []
    nodes_used = set()
    for a in actors:
        if not a.node_id:
            continue  # never scheduled (e.g. still pending at teardown)
        nodes_used.add(a.node_id)
        node_labels = labels_by_id.get(a.node_id, {})
        if not is_allowed_node(a.node_id, node_labels):
            misplaced.append((a.actor_id, a.class_name, a.node_id, dict(node_labels)))

    errors: List[str] = []
    if not actors:
        errors.append(f"No actors found for stage {stage_cls_name!r}")
    if misplaced:
        errors.append(
            f"{len(misplaced)} {stage_cls_name} actor(s) ran outside "
            f"{allowed_desc}: {misplaced}"
        )
    for node_id in require_nodes or []:
        if node_id not in nodes_used:
            errors.append(
                f"Expected >= 1 {stage_cls_name} actor on node {node_id}, got 0"
            )

    print(
        f"Placement sweep [{stage_cls_name}]: {len(actors)} actors across "
        f"{len(nodes_used)} node(s); allowed: {allowed_desc}; "
        f"{len(misplaced)} misplaced."
    )
    return {
        "num_actors": len(actors),
        "num_nodes_used": len(nodes_used),
        "num_misplaced": len(misplaced),
        "errors": errors,
    }
