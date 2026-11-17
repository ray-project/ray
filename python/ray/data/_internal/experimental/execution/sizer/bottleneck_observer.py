"""Observation-only bottleneck model for the actor-only sizer.

Per operator, tracks what fraction of its serving actors' time over a window
went to each of:

- busy: holding work, not output-backpressured
- output-backpressured (obp): pull budget exhausted, so outputs can't move
- starved: idle with an empty input queue

From those, each edge ``producer -> consumer`` between actor-pool operators
gets an arrow pointing toward the side that limits it (the arrow-based method
for serial production lines): if obp(producer) exceeds starved(consumer) by
more than a margin, the limit is at or past the consumer; if it falls short by
more than the margin, the limit is at or before the producer; otherwise the
edge is a tie. Operators with no outgoing arrow are bottlenecks.

Nothing here changes a sizing decision. The sizer samples on every
scheduling-loop step and logs one line per tick, so the named bottlenecks can
be checked against release-test runs before any rule depends on them.
"""

from __future__ import annotations

import enum
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Generic,
    Hashable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

from ray.data._internal.execution.execution_flags import (
    SIZER_BOTTLENECK_ARROW_MARGIN,
    SIZER_BOTTLENECK_SLACK_MARGIN,
    SIZER_BOTTLENECK_WINDOW_S,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.sizer.op_profile import (
        OpProfiles,
    )

logger = logging.getLogger(__name__)

K = TypeVar("K", bound=Hashable)


@dataclass(frozen=True)
class OpShares:
    """One operator's time shares over the window, plus its current size."""

    busy: float
    obp: float
    starved: float
    # Serving actors at the latest sample.
    actors: int


class Arrow(enum.Enum):
    DOWNSTREAM = "->"
    UPSTREAM = "<-"
    TIE = "~"


@dataclass(frozen=True)
class Edge(Generic[K]):
    producer: K
    consumer: K
    producer_obp: float
    consumer_starved: float
    arrow: Arrow


@dataclass(frozen=True)
class Snapshot(Generic[K]):
    shares: Mapping[K, OpShares]
    edges: Tuple[Edge[K], ...]
    # Bottlenecks, most severe first, with their severity.
    bottlenecks: Tuple[Tuple[K, float], ...]
    # Non-bottleneck ops' slack (actors they could give up) toward the most
    # severe bottleneck growing by one actor.
    slack: Mapping[K, int]


# --- pure model ------------------------------------------------------------


def classify_edge(producer_obp: float, consumer_starved: float, margin: float) -> Arrow:
    """Which side of ``producer -> consumer`` limits the flow across it."""
    diff = producer_obp - consumer_starved
    if diff > margin:
        return Arrow.DOWNSTREAM
    if diff < -margin:
        return Arrow.UPSTREAM
    return Arrow.TIE


def build_edges(
    shares: Mapping[K, OpShares],
    next_ops: Mapping[K, Sequence[K]],
    margin: float,
) -> Tuple[Edge[K], ...]:
    """Every edge whose two ends both have shares, with its arrow."""
    edges: List[Edge[K]] = []
    for producer, consumers in next_ops.items():
        if producer not in shares:
            continue
        for consumer in consumers:
            if consumer not in shares:
                continue
            p_obp = shares[producer].obp
            c_starved = shares[consumer].starved
            edges.append(
                Edge(
                    producer,
                    consumer,
                    p_obp,
                    c_starved,
                    classify_edge(p_obp, c_starved, margin),
                )
            )
    return tuple(edges)


def find_bottlenecks(shares: Mapping[K, OpShares], edges: Sequence[Edge[K]]) -> List[K]:
    """Ops with shares and no outgoing arrow. A tie points nowhere."""
    has_outgoing = set()
    for edge in edges:
        if edge.arrow is Arrow.DOWNSTREAM:
            has_outgoing.add(edge.producer)
        elif edge.arrow is Arrow.UPSTREAM:
            has_outgoing.add(edge.consumer)
    return [op for op in shares if op not in has_outgoing]


def severity(
    op: K,
    shares: Mapping[K, OpShares],
    prev_ops: Mapping[K, Sequence[K]],
    next_ops: Mapping[K, Sequence[K]],
) -> float:
    """How much neighbours wait on ``op``, net of its own idle time:
    obp(producers) + starved(consumers) - (obp(op) + starved(op))."""
    waiting_on = sum(shares[p].obp for p in prev_ops.get(op, ()) if p in shares)
    waiting_on += sum(shares[c].starved for c in next_ops.get(op, ()) if c in shares)
    own = shares[op].obp + shares[op].starved
    return waiting_on - own


def slack(actors: int, busy: float, growth: float, margin: float) -> int:
    """Actors an op could give up and still keep up after the bottleneck grows
    by ``growth`` (its new size over its current size). Negative means it is
    already short for that growth."""
    needed = math.ceil(actors * busy * growth * (1 + margin))
    return actors - needed


def analyze(
    shares: Mapping[K, OpShares],
    prev_ops: Mapping[K, Sequence[K]],
    next_ops: Mapping[K, Sequence[K]],
    *,
    arrow_margin: float,
    slack_margin: float,
) -> Snapshot[K]:
    edges = build_edges(shares, next_ops, arrow_margin)
    ranked = sorted(
        (
            (op, severity(op, shares, prev_ops, next_ops))
            for op in find_bottlenecks(shares, edges)
        ),
        key=lambda entry: entry[1],
        reverse=True,
    )
    slack_by_op: Dict[K, int] = {}
    if ranked:
        top_actors = max(1, shares[ranked[0][0]].actors)
        growth = (top_actors + 1) / top_actors
        bottleneck_ops = {op for op, _ in ranked}
        for op, op_shares in shares.items():
            if op not in bottleneck_ops:
                slack_by_op[op] = slack(
                    op_shares.actors, op_shares.busy, growth, slack_margin
                )
    return Snapshot(
        shares=dict(shares.items()),
        edges=edges,
        bottlenecks=tuple(ranked),
        slack=slack_by_op,
    )


# --- sampling --------------------------------------------------------------


class ShareWindow:
    """Time-weighted actor counts over the last ``window_s`` seconds.

    Each sample covers the interval since the previous one, so shares are
    actor-time fractions rather than per-sample averages.
    """

    def __init__(self, window_s: float):
        assert window_s > 0
        self._window_s = window_s
        # (dt, serving, busy, obp, starved)
        self._samples: Deque[Tuple[float, int, int, int, int]] = deque()
        self._span_s = 0.0
        self._latest_serving = 0

    def record(
        self, dt: float, serving: int, busy: int, obp: int, starved: int
    ) -> None:
        self._latest_serving = serving
        if dt <= 0:
            return
        self._samples.append((dt, serving, busy, obp, starved))
        self._span_s += dt
        while self._samples and self._span_s - self._samples[0][0] >= self._window_s:
            self._span_s -= self._samples.popleft()[0]

    def shares(self) -> Optional[OpShares]:
        """None until a full window has been seen, or if no actor served."""
        if self._span_s < self._window_s:
            return None
        serving_time = sum(dt * s for dt, s, _, _, _ in self._samples)
        if serving_time <= 0:
            return None
        busy = sum(dt * b for dt, _, b, _, _ in self._samples) / serving_time
        obp = sum(dt * o for dt, _, _, o, _ in self._samples) / serving_time
        starved = sum(dt * st for dt, _, _, _, st in self._samples) / serving_time
        return OpShares(
            busy=busy, obp=obp, starved=starved, actors=self._latest_serving
        )


class BottleneckObserver:
    """Samples every actor-pool op each scheduling-loop step and reports the
    bottleneck model once per sizer tick. Read-only: it never scales a pool."""

    def __init__(
        self,
        profiles: "OpProfiles",
        *,
        clock: Callable[[], float] = time.monotonic,
        window_s: float = SIZER_BOTTLENECK_WINDOW_S,
        arrow_margin: float = SIZER_BOTTLENECK_ARROW_MARGIN,
        slack_margin: float = SIZER_BOTTLENECK_SLACK_MARGIN,
    ):
        self._profiles = profiles
        self._clock = clock
        self._window_s = window_s
        self._arrow_margin = arrow_margin
        self._slack_margin = slack_margin
        self._windows: Dict["ExperimentalActorPoolMapOperator", ShareWindow] = {}
        self._last_t: Optional[float] = None
        self._names = _display_names(profiles.actor_pool_ops)

    def observe(self, topology: "Topology") -> None:
        now = self._clock()
        dt = 0.0 if self._last_t is None else now - self._last_t
        self._last_t = now
        for op in self._profiles.actor_pool_ops:
            if op.has_completed():
                self._windows.pop(op, None)
                continue
            serving, busy, obp, idle = op.serving_actor_breakdown()
            queued = topology[op].total_enqueued_input_blocks()
            # Idle with work queued is a dispatch gap, not starvation.
            starved = idle if queued == 0 else 0
            window = self._windows.get(op)
            if window is None:
                window = self._windows[op] = ShareWindow(self._window_s)
            window.record(dt, serving, busy, obp, starved)

    def snapshot(self) -> Snapshot["ExperimentalActorPoolMapOperator"]:
        shares: Dict["ExperimentalActorPoolMapOperator", OpShares] = {}
        for op, window in self._windows.items():
            op_shares = window.shares()
            if op_shares is not None:
                shares[op] = op_shares
        return analyze(
            shares,
            self._profiles.prev_ops,
            self._profiles.next_ops,
            arrow_margin=self._arrow_margin,
            slack_margin=self._slack_margin,
        )

    def log_tick(self) -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug(
            "BottleneckObserver: %s", format_snapshot(self.snapshot(), self._names)
        )


def _display_names(
    ops: Sequence["ExperimentalActorPoolMapOperator"],
) -> Dict[Hashable, str]:
    """Op name, or name plus id when two ops share a name (e.g. two Projects)."""
    counts: Dict[str, int] = {}
    for op in ops:
        counts[op.name] = counts.get(op.name, 0) + 1
    return {
        op: op.name if counts[op.name] == 1 else f"{op.name}[{op.id}]" for op in ops
    }


def format_snapshot(snapshot: Snapshot[Any], names: Mapping[Any, str]) -> str:
    def name(op: Any) -> str:
        return names.get(op, str(op))

    if not snapshot.shares:
        return "warming up (no op has a full window yet)"
    bottlenecks = ", ".join(
        f"{name(op)}(sev={sev:.2f}, n={snapshot.shares[op].actors})"
        for op, sev in snapshot.bottlenecks
    )
    edges = ", ".join(
        f"{name(e.producer)}->{name(e.consumer)} "
        f"obp={e.producer_obp:.2f}/starved={e.consumer_starved:.2f} [{e.arrow.value}]"
        for e in snapshot.edges
    )
    ops = ", ".join(
        f"{name(op)} busy={s.busy:.2f} obp={s.obp:.2f} starved={s.starved:.2f} "
        f"n={s.actors}"
        + (f" slack={snapshot.slack[op]}" if op in snapshot.slack else "")
        for op, s in snapshot.shares.items()
    )
    return f"bottlenecks=[{bottlenecks}] | edges=[{edges}] | ops=[{ops}]"
