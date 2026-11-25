"""The bottleneck snapshot the actor-only sizer decides on.

Per operator, tracks what fraction of its serving task slots' time over a
window went to each of (a slot is one of an actor's ``max_concurrency`` task
seats):

- busy: holding a task on an actor that is not output-backpressured. Taken
  from the pool's event-driven busy-slot integral, so tasks shorter than a
  sizer tick still count.
- output-backpressured (obp): every slot of an actor whose pull budget is
  exhausted, so its outputs can't move (sampled each scheduling step)
- starved: idle slots while the input queue is empty

An op with queued input but no serving actor at all (e.g. a Write sink that
could not place its first actor) counts that time as one fully busy actor:
it has work and no capacity, which is the tightest a stage can be. Without
this it would have no actor time, drop out of the snapshot, and its producer --
fully output-backpressured on it -- would be named instead.

From those, each edge ``producer -> consumer`` between actor-pool operators
gets an arrow pointing toward the side that limits it (the arrow-based method
for serial production lines): if obp(producer) exceeds starved(consumer) by
more than a margin, the limit is at or past the consumer; if it falls short by
more than the margin, the limit is at or before the producer; otherwise the
edge is a tie. Operators with no outgoing arrow are bottlenecks, except one
that is itself output-backpressured nearly all the time (whatever limits it is
downstream, outside the snapshot, e.g. a non-actor-pool consumer), or one whose
severity is below a floor (nothing waits on it much more than it waits itself,
e.g. a near-tie edge during ramp-up, or the last op working through its tail
after every upstream op has completed).

Nothing here scales a pool: the sizer samples on every scheduling-loop step,
logs one line per tick, and its sizing policy (``policy``) decides from the
snapshot.
"""

from __future__ import annotations

import enum
import logging
import time
from collections import deque
from dataclasses import dataclass, field
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
    SIZER_BOTTLENECK_MIN_SEVERITY,
    SIZER_BOTTLENECK_WINDOW_S,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator as EAPMO,
        SlotUsage,
    )
    from ray.data._internal.experimental.execution.sizer.op_profile import (
        OpProfiles,
    )

logger = logging.getLogger(__name__)

K = TypeVar("K", bound=Hashable)


@dataclass(frozen=True)
class OpShareBreakdown:
    """One operator's slot-time shares over the window, plus its current
    size (in actors)."""

    busy: float
    obp: float
    starved: float
    # Serving actors at the latest sample.
    actors: int
    # Share of the window spent with queued input and no serving actor (each
    # such second counts as one fully busy slot, so it is part of ``busy``).
    unstaffed: float = 0.0


class Arrow(enum.Enum):
    DOWNSTREAM = "->"
    UPSTREAM = "<-"
    TIE = "~"


@dataclass(frozen=True)
class EdgeShares:
    """One edge's joint shares over the window, as fractions of the
    producer's slot time: its output-backpressured time, split by what the
    consumer was doing at that moment."""

    # The consumer had no idle slot: it is what holds the producer back.
    blocked: float
    # The consumer had an idle slot: the producer's own output budget (blocks
    # still in flight, counted against it until consumed) held it back.
    self_limited: float


@dataclass(frozen=True)
class Edge(Generic[K]):
    producer: K
    consumer: K
    producer_blocked: float
    producer_self_limited: float
    consumer_starved: float
    arrow: Arrow


@dataclass(frozen=True)
class Snapshot(Generic[K]):
    shares: Mapping[K, OpShareBreakdown]
    edges: Tuple[Edge[K], ...]
    # Bottlenecks, most severe first, with their severity.
    bottlenecks: Tuple[Tuple[K, float], ...]
    # Joint shares per (producer, consumer); an edge missing here falls back
    # to the producer's whole output backpressure as ``blocked``.
    edge_shares: Mapping[Tuple[K, K], EdgeShares] = field(default_factory=dict)


# --- pure analysis ---------------------------------------------------------


def classify_edge(
    producer_blocked: float,
    producer_self_limited: float,
    consumer_starved: float,
    margin: float,
) -> Arrow:
    """Which side of ``producer -> consumer`` limits the flow across it.

    Downstream when the producer is blocked by a consumer with no idle slot
    more than the consumer is starved; upstream when the consumer is starved,
    or the producer is held back by its own budget while the consumer has
    room, more than the producer is blocked.
    """
    diff = producer_blocked - (consumer_starved + producer_self_limited)
    if diff > margin:
        return Arrow.DOWNSTREAM
    if diff < -margin:
        return Arrow.UPSTREAM
    return Arrow.TIE


def edge_shares_of(
    producer: K,
    consumer: K,
    shares: Mapping[K, OpShareBreakdown],
    edge_shares: Optional[Mapping[Tuple[K, K], EdgeShares]],
) -> EdgeShares:
    """The edge's joint shares, or, without them, all of the producer's
    output backpressure counted as blocked by the consumer."""
    joint = (edge_shares or {}).get((producer, consumer))
    if joint is not None:
        return joint
    return EdgeShares(blocked=shares[producer].obp, self_limited=0.0)


def blocked_share(
    op: K,
    shares: Mapping[K, OpShareBreakdown],
    next_ops: Mapping[K, Sequence[K]],
    edge_shares: Optional[Mapping[Tuple[K, K], EdgeShares]],
) -> float:
    """Share of ``op``'s slot time a consumer held it back: the largest over
    its out-edges to consumers with shares, or its whole output backpressure
    when no consumer has shares (e.g. a non-actor-pool sink)."""
    consumers = [c for c in next_ops.get(op, ()) if c in shares]
    if not consumers:
        return shares[op].obp
    return max(edge_shares_of(op, c, shares, edge_shares).blocked for c in consumers)


def build_edges(
    shares: Mapping[K, OpShareBreakdown],
    next_ops: Mapping[K, Sequence[K]],
    margin: float,
    edge_shares: Optional[Mapping[Tuple[K, K], EdgeShares]] = None,
) -> Tuple[Edge[K], ...]:
    """Every edge whose two ends both have shares, with its arrow."""
    edges: List[Edge[K]] = []
    for producer, consumers in next_ops.items():
        if producer not in shares:
            continue
        for consumer in consumers:
            if consumer not in shares:
                continue
            joint = edge_shares_of(producer, consumer, shares, edge_shares)
            c_starved = shares[consumer].starved
            edges.append(
                Edge(
                    producer,
                    consumer,
                    joint.blocked,
                    joint.self_limited,
                    c_starved,
                    classify_edge(joint.blocked, joint.self_limited, c_starved, margin),
                )
            )
    return tuple(edges)


def find_bottlenecks(
    shares: Mapping[K, OpShareBreakdown],
    edges: Sequence[Edge[K]],
    margin: float,
    next_ops: Mapping[K, Sequence[K]],
    edge_shares: Optional[Mapping[Tuple[K, K], EdgeShares]] = None,
) -> List[K]:
    """Ops with shares and no outgoing arrow. A tie points nowhere.

    An op a consumer holds back more than ``1 - margin`` of the time is never
    one: it is waiting on something downstream. Normally that shows as an
    outgoing arrow; this covers a consumer the snapshot has no shares for.
    Output backpressure from the op's own budget does not count here.
    """
    has_outgoing = set()
    for edge in edges:
        if edge.arrow is Arrow.DOWNSTREAM:
            has_outgoing.add(edge.producer)
        elif edge.arrow is Arrow.UPSTREAM:
            has_outgoing.add(edge.consumer)
    return [
        op
        for op in shares
        if op not in has_outgoing
        and blocked_share(op, shares, next_ops, edge_shares) <= 1 - margin
    ]


def severity(
    op: K,
    shares: Mapping[K, OpShareBreakdown],
    prev_ops: Mapping[K, Sequence[K]],
    next_ops: Mapping[K, Sequence[K]],
    edge_shares: Optional[Mapping[Tuple[K, K], EdgeShares]] = None,
) -> float:
    """How much neighbours wait on ``op``, net of the time something else
    held it back: blocked(producers -> op) + starved(consumers) -
    (blocked(op) + starved(op))."""
    waiting_on = sum(
        edge_shares_of(p, op, shares, edge_shares).blocked
        for p in prev_ops.get(op, ())
        if p in shares
    )
    waiting_on += sum(shares[c].starved for c in next_ops.get(op, ()) if c in shares)
    own = blocked_share(op, shares, next_ops, edge_shares) + shares[op].starved
    return waiting_on - own


def path_between(
    src: K, dst: K, next_ops: Mapping[K, Sequence[K]]
) -> Optional[List[K]]:
    """Ops strictly between ``src`` and a downstream ``dst``, or None if
    ``dst`` is not downstream of ``src``."""
    stack: List[Tuple[K, List[K]]] = [(src, [])]
    seen = set()
    while stack:
        op, between = stack.pop()
        for nxt in next_ops.get(op, ()):
            if nxt == dst:
                return between
            if nxt not in seen:
                seen.add(nxt)
                stack.append((nxt, between + [nxt]))
    return None


def analyze(
    shares: Mapping[K, OpShareBreakdown],
    prev_ops: Mapping[K, Sequence[K]],
    next_ops: Mapping[K, Sequence[K]],
    *,
    arrow_margin: float,
    min_severity: float = 0.0,
    edge_shares: Optional[Mapping[Tuple[K, K], EdgeShares]] = None,
) -> Snapshot[K]:
    edges = build_edges(shares, next_ops, arrow_margin, edge_shares)
    # Severity <= 0: no neighbour waits on the op more than it idles itself,
    # so upscaling it would not speed anything up. Below ``min_severity``, too
    # little waits on it to tell from noise.
    scored = (
        (op, severity(op, shares, prev_ops, next_ops, edge_shares))
        for op in find_bottlenecks(shares, edges, arrow_margin, next_ops, edge_shares)
    )
    ranked = sorted(
        ((op, sev) for op, sev in scored if sev > 0 and sev >= min_severity),
        key=lambda entry: entry[1],
        reverse=True,
    )
    return Snapshot(
        shares=dict(shares.items()),
        edges=edges,
        bottlenecks=tuple(ranked),
        edge_shares=dict((edge_shares or {}).items()),
    )


def upscale_blocker(
    *,
    op: K,
    snapshot: Snapshot[K],
    next_ops: Mapping[K, Sequence[K]],
    margin: float,
) -> Optional[str]:
    """Why upscaling the named bottleneck ``op`` would not help this tick,
    or None if it would: a more severe bottleneck upstream limits its input,
    or a producer feeding it is held back by its own output budget while
    ``op`` has idle slots. Either way new actors would wait for input."""
    named = dict(snapshot.bottlenecks)
    for other, sev in named.items():
        if (
            other is not op
            and sev > named[op]
            and path_between(other, op, next_ops) is not None
        ):
            return "a more severe bottleneck is upstream"
    for edge in snapshot.edges:
        if edge.consumer is op and edge.producer_self_limited >= margin:
            return f"input self-limited ({edge.producer_self_limited:.2f})"
    return None


# --- sampling --------------------------------------------------------------


class ShareWindow:
    """Slot-time totals over the last ``window_s`` seconds.

    Each sample covers the interval since the previous one, in slot-seconds,
    so shares are slot-time fractions rather than per-sample averages.
    """

    def __init__(self, window_s: float):
        assert window_s > 0
        self._window_s = window_s
        # (dt, slot_s, busy_s, obp_s, starved_s, unstaffed_s)
        self._samples: Deque[Tuple[float, float, float, float, float, float]] = deque()
        self._span_s = 0.0
        self._latest_actors = 0

    def record(
        self,
        *,
        dt: float,
        actors: int,
        slot_s: float,
        busy_s: float,
        obp_s: float,
        starved_s: float,
        unstaffed: bool = False,
    ) -> None:
        """One interval of ``dt`` seconds: ``slot_s`` slot-seconds of
        capacity, split into busy / obp / starved (the rest was idle with work
        queued). ``unstaffed``: no serving actor but input is queued; the
        interval counts as one fully busy slot."""
        self._latest_actors = actors
        if dt <= 0:
            return
        unstaffed_s = dt if unstaffed and slot_s <= 0 else 0.0
        self._samples.append(
            (
                dt,
                slot_s + unstaffed_s,
                busy_s + unstaffed_s,
                obp_s,
                starved_s,
                unstaffed_s,
            )
        )
        self._span_s += dt
        while self._samples and self._span_s - self._samples[0][0] >= self._window_s:
            self._span_s -= self._samples.popleft()[0]

    def shares(self) -> Optional[OpShareBreakdown]:
        """None until a full window has been seen, or if the op had neither a
        serving actor nor queued input in it."""
        if self._span_s < self._window_s:
            return None
        totals = [sum(sample[i] for sample in self._samples) for i in range(1, 6)]
        slot_s, busy_s, obp_s, starved_s, unstaffed_s = totals
        if slot_s <= 0:
            return None
        return OpShareBreakdown(
            busy=busy_s / slot_s,
            obp=obp_s / slot_s,
            starved=starved_s / slot_s,
            actors=self._latest_actors,
            unstaffed=unstaffed_s / slot_s,
        )


class EdgeWindow:
    """One edge's producer slot time over the last ``window_s`` seconds,
    with the parts output-backpressured while the consumer was saturated
    (``blocked``) or had an idle slot (``self_limited``)."""

    def __init__(self, window_s: float):
        assert window_s > 0
        self._window_s = window_s
        # (dt, producer_slot_s, blocked_s, self_limited_s)
        self._samples: Deque[Tuple[float, float, float, float]] = deque()
        self._span_s = 0.0

    def record(
        self, *, dt: float, slot_s: float, blocked_s: float, self_limited_s: float
    ) -> None:
        if dt <= 0:
            return
        self._samples.append((dt, slot_s, blocked_s, self_limited_s))
        self._span_s += dt
        while self._samples and self._span_s - self._samples[0][0] >= self._window_s:
            self._span_s -= self._samples.popleft()[0]

    def shares(self) -> Optional[EdgeShares]:
        """None until a full window has been seen, or if the producer had no
        slot time in it."""
        if self._span_s < self._window_s:
            return None
        slot_s = sum(sample[1] for sample in self._samples)
        if slot_s <= 0:
            return None
        return EdgeShares(
            blocked=sum(sample[2] for sample in self._samples) / slot_s,
            self_limited=sum(sample[3] for sample in self._samples) / slot_s,
        )


class BottleneckObserver:
    """Samples every actor-pool op each scheduling-loop step and reports the
    bottleneck snapshot once per sizer tick. Read-only: it never scales a pool."""

    def __init__(
        self,
        profiles: "OpProfiles",
        *,
        clock: Callable[[], float] = time.monotonic,
        window_s: float = SIZER_BOTTLENECK_WINDOW_S,
        arrow_margin: float = SIZER_BOTTLENECK_ARROW_MARGIN,
        min_severity: float = SIZER_BOTTLENECK_MIN_SEVERITY,
    ):
        self._profiles = profiles
        self._clock = clock
        self._window_s = window_s
        self._arrow_margin = arrow_margin
        self._min_severity = min_severity
        self._windows: Dict["EAPMO", ShareWindow] = {}
        self._edge_windows: Dict[
            Tuple["EAPMO", "EAPMO"],
            EdgeWindow,
        ] = {}
        # Last read of each op's cumulative busy-slot seconds.
        self._last_busy_s: Dict["EAPMO", float] = {}
        self._last_t: Optional[float] = None
        self._names = _display_names(profiles.actor_pool_ops)

    @property
    def window_s(self) -> float:
        return self._window_s

    def reset_windows(self) -> None:
        """Drop every window's samples, so each op needs a new full window
        before the snapshot reports it. The cumulative busy readings stay, so
        the next sample still measures from the last one."""
        self._windows.clear()
        self._edge_windows.clear()

    def observe(self, topology: "Topology") -> None:
        now = self._clock()
        dt = 0.0 if self._last_t is None else now - self._last_t
        self._last_t = now
        usages: Dict["EAPMO", "SlotUsage"] = {}
        queued: Dict["EAPMO", int] = {}
        for op in self._profiles.actor_pool_ops:
            if op.has_completed():
                for per_op in (self._windows, self._last_busy_s):
                    per_op.pop(op, None)
                for edge in [e for e in self._edge_windows if op in e]:
                    del self._edge_windows[edge]
                continue
            usages[op] = op.slot_usage()
            queued[op] = topology[op].total_enqueued_input_blocks()
        for op, usage in usages.items():
            self._record_op(op, usage, queued[op], dt)
        # Joint per-edge shares: at this moment, is the consumer saturated
        # (no idle slot, or input queued with no actor)? The producer's
        # output-backpressured slots count as blocked by it if so, else as
        # held back by the producer's own budget.
        saturated = {
            op: (
                usage.busy_slots + usage.obp_slots >= usage.slots
                if usage.slots > 0
                else queued[op] > 0
            )
            for op, usage in usages.items()
        }
        for producer, usage in usages.items():
            obp_s = usage.obp_slots * dt
            for consumer in self._profiles.next_ops.get(producer, ()):
                if consumer not in usages:
                    continue
                window = self._edge_windows.get((producer, consumer))
                if window is None:
                    window = self._edge_windows[(producer, consumer)] = EdgeWindow(
                        self._window_s
                    )
                blocked_s = obp_s if saturated[consumer] else 0.0
                window.record(
                    dt=dt,
                    slot_s=usage.slots * dt,
                    blocked_s=blocked_s,
                    self_limited_s=obp_s - blocked_s,
                )

    def _record_op(
        self,
        op: "EAPMO",
        usage: "SlotUsage",
        queued: int,
        dt: float,
    ) -> None:
        prev_busy_s = self._last_busy_s.get(op)
        self._last_busy_s[op] = usage.busy_slot_seconds
        busy_integral = (
            0.0
            if prev_busy_s is None
            else max(0.0, usage.busy_slot_seconds - prev_busy_s)
        )
        slot_s = usage.slots * dt
        obp_s = usage.obp_slots * dt
        # The integral counts output-backpressured actors' held tasks; that
        # time is obp, not busy.
        busy_s = min(
            max(0.0, busy_integral - usage.obp_busy_slots * dt),
            max(0.0, slot_s - obp_s),
        )
        # Idle with work queued is a dispatch gap, not starvation.
        starved_s = max(0.0, slot_s - obp_s - busy_s) if queued == 0 else 0.0
        window = self._windows.get(op)
        if window is None:
            window = self._windows[op] = ShareWindow(self._window_s)
        window.record(
            dt=dt,
            actors=usage.serving_actors,
            slot_s=slot_s,
            busy_s=busy_s,
            obp_s=obp_s,
            starved_s=starved_s,
            unstaffed=usage.serving_actors == 0 and queued > 0,
        )

    def snapshot(self) -> Snapshot["EAPMO"]:
        shares: Dict["EAPMO", OpShareBreakdown] = {}
        for op, window in self._windows.items():
            op_shares = window.shares()
            if op_shares is not None:
                shares[op] = op_shares
        edge_shares: Dict[Tuple["EAPMO", "EAPMO"], EdgeShares] = {}
        for edge, window in self._edge_windows.items():
            joint = window.shares()
            if joint is not None:
                edge_shares[edge] = joint
        return analyze(
            shares,
            self._profiles.prev_ops,
            self._profiles.next_ops,
            arrow_margin=self._arrow_margin,
            min_severity=self._min_severity,
            edge_shares=edge_shares,
        )

    def tick(self) -> Snapshot["EAPMO"]:
        """This tick's snapshot, logged once. Call once per sizer tick."""
        snapshot = self.snapshot()
        logger.debug(
            "BottleneckObserver: %s",
            format_snapshot(snapshot=snapshot, names=self._names),
        )
        return snapshot


def _display_names(
    ops: Sequence["EAPMO"],
) -> Dict[Hashable, str]:
    """Op name, or name plus id when two ops share a name (e.g. two Projects)."""
    counts: Dict[str, int] = {}
    for op in ops:
        counts[op.name] = counts.get(op.name, 0) + 1
    return {
        op: op.name if counts[op.name] == 1 else f"{op.name}[{op.id}]" for op in ops
    }


def format_snapshot(*, snapshot: Snapshot[Any], names: Mapping[Any, str]) -> str:
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
        f"blocked={e.producer_blocked:.2f}/self={e.producer_self_limited:.2f}"
        f"/starved={e.consumer_starved:.2f} [{e.arrow.value}]"
        for e in snapshot.edges
    )
    ops = ", ".join(
        f"{name(op)} busy={s.busy:.2f} obp={s.obp:.2f} starved={s.starved:.2f} "
        f"n={s.actors}" + (f" unstaffed={s.unstaffed:.2f}" if s.unstaffed > 0 else "")
        for op, s in snapshot.shares.items()
    )
    return f"bottlenecks=[{bottlenecks}] | edges=[{edges}] | ops=[{ops}]"
