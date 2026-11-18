"""Observation-only bottleneck model for the actor-only sizer.

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
this it would have no actor time, drop out of the model, and its producer --
fully output-backpressured on it -- would be named instead.

From those, each edge ``producer -> consumer`` between actor-pool operators
gets an arrow pointing toward the side that limits it (the arrow-based method
for serial production lines): if obp(producer) exceeds starved(consumer) by
more than a margin, the limit is at or past the consumer; if it falls short by
more than the margin, the limit is at or before the producer; otherwise the
edge is a tie. Operators with no outgoing arrow are bottlenecks, except one
that is itself output-backpressured nearly all the time (whatever limits it is
downstream, outside the model, e.g. a non-actor-pool consumer), or one with
severity <= 0 (nothing waits on it more than it waits itself, e.g. the last op
working through its tail after every upstream op has completed).

Each logged tick also says whether each bottleneck could use capacity
(eligibility: free room on a node it may use, or room other ops' actors there
would free), and, for the most severe bottleneck that can grow, how long each
donor's candidate victims on its nodes would take to drain (time-to-free:
outstanding output blocks over the rate the donor's consumer finishes them),
flagging circular drains whose outputs only clear through the recipient.

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
    AbstractSet,
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
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import ActorStatus, NodeIdStr
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
    FIT_EPS,
    PLACEMENT_RESOURCE_DIMS,
    max_placeable_actors,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    bounded_max_size,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.sizer.models import (
        TickCapacitySnapshot,
    )
    from ray.data._internal.experimental.execution.sizer.op_profile import (
        OpProfiles,
    )

logger = logging.getLogger(__name__)

K = TypeVar("K", bound=Hashable)


@dataclass(frozen=True)
class OpShares:
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


@dataclass(frozen=True)
class DonorVictims:
    """One donor's candidate victims on the recipient's allowed nodes."""

    count: int
    # Estimated seconds until a victim frees its slot; inf when the donor's
    # outputs are not being consumed at all.
    ttf_min_s: float
    ttf_p50_s: float
    # The recipient is downstream and every op between them is
    # output-backpressured: the victims' outputs only clear as fast as the
    # recipient itself runs.
    circular: bool


@dataclass(frozen=True)
class Assessment(Generic[K]):
    """What a tick's bottlenecks could do with capacity (logged only)."""

    # Per bottleneck: "yes: free" / "yes: victims" / "no: <reason>".
    eligibility: Mapping[K, str]
    # The most severe bottleneck able to grow, whose victims are listed.
    recipient: Optional[K]
    victims: Mapping[K, DonorVictims]


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


def find_bottlenecks(
    shares: Mapping[K, OpShares], edges: Sequence[Edge[K]], margin: float
) -> List[K]:
    """Ops with shares and no outgoing arrow. A tie points nowhere.

    An op output-backpressured more than ``1 - margin`` of the time is never
    one: it is waiting on something downstream. Normally that shows as an
    outgoing arrow; this covers a consumer the model has no shares for.
    """
    has_outgoing = set()
    for edge in edges:
        if edge.arrow is Arrow.DOWNSTREAM:
            has_outgoing.add(edge.producer)
        elif edge.arrow is Arrow.UPSTREAM:
            has_outgoing.add(edge.consumer)
    return [
        op
        for op, op_shares in shares.items()
        if op not in has_outgoing and op_shares.obp <= 1 - margin
    ]


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


def is_circular(
    donor: K,
    recipient: K,
    shares: Mapping[K, OpShares],
    next_ops: Mapping[K, Sequence[K]],
    margin: float,
) -> bool:
    """Whether ``donor``'s outputs clear only through ``recipient``: it is
    downstream and every op between them is output-backpressured."""
    between = path_between(donor, recipient, next_ops)
    if between is None:
        return False
    return all(op in shares and shares[op].obp >= 1 - margin for op in between)


def time_to_free(work_blocks: float, consume_rate: float) -> float:
    """Seconds to drain ``work_blocks`` at ``consume_rate`` blocks/s."""
    if work_blocks <= 0:
        return 0.0
    if consume_rate <= 0:
        return math.inf
    return work_blocks / consume_rate


def analyze(
    shares: Mapping[K, OpShares],
    prev_ops: Mapping[K, Sequence[K]],
    next_ops: Mapping[K, Sequence[K]],
    *,
    arrow_margin: float,
    slack_margin: float,
) -> Snapshot[K]:
    edges = build_edges(shares, next_ops, arrow_margin)
    # Severity <= 0: no neighbour waits on the op more than it idles itself,
    # so growing it would not speed anything up.
    scored = (
        (op, severity(op, shares, prev_ops, next_ops))
        for op in find_bottlenecks(shares, edges, arrow_margin)
    )
    ranked = sorted(
        ((op, sev) for op, sev in scored if sev > 0),
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
        dt: float,
        *,
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

    def shares(self) -> Optional[OpShares]:
        """None until a full window has been seen, or if the op had neither a
        serving actor nor queued input in it."""
        if self._span_s < self._window_s:
            return None
        totals = [sum(sample[i] for sample in self._samples) for i in range(1, 6)]
        slot_s, busy_s, obp_s, starved_s, unstaffed_s = totals
        if slot_s <= 0:
            return None
        return OpShares(
            busy=busy_s / slot_s,
            obp=obp_s / slot_s,
            starved=starved_s / slot_s,
            actors=self._latest_actors,
            unstaffed=unstaffed_s / slot_s,
        )


class _CounterRate:
    """Rate of a cumulative counter over the last ``window_s`` seconds."""

    def __init__(self, window_s: float):
        self._window_s = window_s
        self._points: Deque[Tuple[float, float]] = deque()

    def record(self, now: float, value: float) -> None:
        self._points.append((now, value))
        while len(self._points) > 2 and now - self._points[1][0] >= self._window_s:
            self._points.popleft()

    def rate(self) -> float:
        if len(self._points) < 2:
            return 0.0
        (t0, v0), (t1, v1) = self._points[0], self._points[-1]
        return (v1 - v0) / (t1 - t0) if t1 > t0 else 0.0


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
        # Last read of each op's cumulative busy-slot seconds.
        self._last_busy_s: Dict["ExperimentalActorPoolMapOperator", float] = {}
        # Input blocks each op's tasks finished, and output blocks taken from
        # each op: what clears a producer's outputs, for time-to-free.
        self._processed: Dict["ExperimentalActorPoolMapOperator", _CounterRate] = {}
        self._taken: Dict["ExperimentalActorPoolMapOperator", _CounterRate] = {}
        self._last_t: Optional[float] = None
        self._names = _display_names(profiles.actor_pool_ops)

    def observe(self, topology: "Topology") -> None:
        now = self._clock()
        dt = 0.0 if self._last_t is None else now - self._last_t
        self._last_t = now
        for op in self._profiles.actor_pool_ops:
            if op.has_completed():
                for per_op in (self._windows, self._last_busy_s):
                    per_op.pop(op, None)
                continue
            usage = op.slot_usage()
            queued = topology[op].total_enqueued_input_blocks()
            prev_busy_s = self._last_busy_s.get(op)
            self._last_busy_s[op] = usage.busy_slot_seconds
            busy_integral = (
                0.0
                if prev_busy_s is None
                else max(0.0, usage.busy_slot_seconds - prev_busy_s)
            )
            slot_s = usage.slots * dt
            obp_s = usage.obp_slots * dt
            # The integral counts output-backpressured actors' held tasks;
            # that time is obp, not busy.
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
                dt,
                actors=usage.serving_actors,
                slot_s=slot_s,
                busy_s=busy_s,
                obp_s=obp_s,
                starved_s=starved_s,
                unstaffed=usage.serving_actors == 0 and queued > 0,
            )
            for rates, value in (
                (self._processed, op.metrics.num_task_inputs_processed),
                (self._taken, op.metrics.num_outputs_taken),
            ):
                rates.setdefault(op, _CounterRate(self._window_s)).record(now, value)

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

    def assess(
        self,
        snapshot: Snapshot["ExperimentalActorPoolMapOperator"],
        capacity: "TickCapacitySnapshot",
    ) -> Assessment["ExperimentalActorPoolMapOperator"]:
        """Eligibility of each bottleneck, and the victims a transfer to the
        most severe one that can grow would draw from."""
        bottlenecks = [op for op, _ in snapshot.bottlenecks]
        freeable = self._freeable_by_node(capacity, exclude=set(bottlenecks))
        eligibility = {
            op: self._eligibility(op, capacity, freeable) for op in bottlenecks
        }
        recipient = next(
            (op for op in bottlenecks if not eligibility[op].startswith("no:")),
            None,
        )
        victims: Dict["ExperimentalActorPoolMapOperator", DonorVictims] = {}
        if recipient is not None and recipient in capacity.schedulable_nodes_by_op:
            nodes = capacity.schedulable_nodes_by_op[recipient]
            for donor in self._profiles.non_pg_ops:
                if donor in eligibility or donor not in snapshot.shares:
                    continue
                summary = self._donor_victims(donor, recipient, nodes, snapshot)
                if summary is not None:
                    victims[donor] = summary
        return Assessment(eligibility, recipient, victims)

    def _freeable_by_node(
        self,
        capacity: "TickCapacitySnapshot",
        *,
        exclude: AbstractSet["ExperimentalActorPoolMapOperator"],
    ) -> Dict[NodeIdStr, ExecutionResources]:
        """Per node, what every serving actor of a non-PG op outside
        ``exclude`` holds: the most victims there could free."""
        freeable: Dict[NodeIdStr, ExecutionResources] = {}
        for op in self._profiles.non_pg_ops:
            if op in exclude or op.has_completed():
                continue
            per_actor = capacity.per_actor_resources_by_op[op]
            for node, states in op.actor_pool.alive_actor_states_by_node().items():
                serving = sum(
                    1
                    for state in states.values()
                    if state.status in (ActorStatus.IDLE, ActorStatus.ACTIVE)
                )
                if serving:
                    freeable[node] = freeable.get(node, ExecutionResources.zero()).add(
                        per_actor.scale(serving)
                    )
        return freeable

    def _eligibility(
        self,
        op: "ExperimentalActorPoolMapOperator",
        capacity: "TickCapacitySnapshot",
        freeable: Mapping[NodeIdStr, ExecutionResources],
    ) -> str:
        pool = op.actor_pool
        if pool.serving_size() >= bounded_max_size(pool):
            return "no: at max size"
        if self._profiles.is_pg_managed(op):
            return "no: placement group (not assessed)"
        per_actor = capacity.per_actor_resources_by_op[op]
        nodes = [
            n
            for n in capacity.free_by_node
            if n in capacity.schedulable_nodes_by_op[op]
        ]
        if not nodes:
            return "no: no allowed node"
        if any(
            max_placeable_actors(capacity.free_by_node[n], per_actor) >= 1
            for n in nodes
        ):
            return "yes: free"
        best: Optional[Dict[str, float]] = None
        for node in nodes:
            room = capacity.free_by_node[node].add(
                freeable.get(node, ExecutionResources.zero())
            )
            if max_placeable_actors(room, per_actor) >= 1:
                return "yes: victims"
            short = {
                dim: getattr(per_actor, dim) - getattr(room, dim)
                for dim in PLACEMENT_RESOURCE_DIMS
                if getattr(per_actor, dim) > getattr(room, dim) + FIT_EPS
            }
            if best is None or len(short) < len(best):
                best = short
        return f"no: short {_format_resources(best or {})} after all victims"

    def _consume_rate(self, donor: "ExperimentalActorPoolMapOperator") -> float:
        """Blocks/s that clear ``donor``'s outputs: its actor-pool consumers'
        finished input blocks, or, with none, the blocks taken from it."""
        consumers = self._profiles.next_ops.get(donor, ())
        if consumers:
            return sum(
                self._processed[c].rate() for c in consumers if c in self._processed
            )
        rates = self._taken.get(donor)
        return rates.rate() if rates is not None else 0.0

    def _donor_victims(
        self,
        donor: "ExperimentalActorPoolMapOperator",
        recipient: "ExperimentalActorPoolMapOperator",
        nodes: AbstractSet[NodeIdStr],
        snapshot: Snapshot["ExperimentalActorPoolMapOperator"],
    ) -> Optional[DonorVictims]:
        """Time-to-free of ``donor``'s non-draining actors on ``nodes``.

        Work per victim = its unconsumed output blocks + its tasks in flight x
        the donor's outputs per task. A lower bound when the consumer has a
        backlog queued ahead of the victim's outputs.
        """
        view = placement_view.build_op_placement_view(donor, rank_victims=True)
        metrics = donor.metrics
        outputs_per_task = metrics.num_task_outputs_generated / max(
            1, metrics.num_tasks_finished
        )
        rate = self._consume_rate(donor)
        ttfs = sorted(
            time_to_free(unconsumed + in_flight * outputs_per_task, rate)
            for node in nodes
            for in_flight, unconsumed in view.victim_drain_cost_by_node.get(node, [])
        )
        if not ttfs:
            return None
        return DonorVictims(
            count=len(ttfs),
            ttf_min_s=ttfs[0],
            ttf_p50_s=ttfs[len(ttfs) // 2],
            circular=is_circular(
                donor,
                recipient,
                snapshot.shares,
                self._profiles.next_ops,
                self._arrow_margin,
            ),
        )

    def log_tick(self, capacity: "TickCapacitySnapshot") -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return
        snapshot = self.snapshot()
        logger.debug(
            "BottleneckObserver: %s",
            format_snapshot(snapshot, self._names, self.assess(snapshot, capacity)),
        )


def _format_resources(resources: Mapping[str, float]) -> str:
    parts = []
    for dim, value in resources.items():
        parts.append(
            f"{dim}={value / 2**30:.1f}GiB" if dim == "memory" else f"{dim}={value:g}"
        )
    return "{" + ", ".join(parts) + "}"


def _format_seconds(seconds: float) -> str:
    return "inf" if math.isinf(seconds) else f"{seconds:.0f}s"


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


def format_snapshot(
    snapshot: Snapshot[Any],
    names: Mapping[Any, str],
    assessment: Optional[Assessment[Any]] = None,
) -> str:
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
        + (f" unstaffed={s.unstaffed:.2f}" if s.unstaffed > 0 else "")
        + (f" slack={snapshot.slack[op]}" if op in snapshot.slack else "")
        for op, s in snapshot.shares.items()
    )
    line = f"bottlenecks=[{bottlenecks}] | edges=[{edges}] | ops=[{ops}]"
    if assessment is None or not assessment.eligibility:
        return line
    eligible = ", ".join(
        f"{name(op)}: {verdict}" for op, verdict in assessment.eligibility.items()
    )
    line += f" | eligible=[{eligible}]"
    if assessment.recipient is not None:
        victims = ", ".join(
            f"{name(op)} n={v.count} ttf min={_format_seconds(v.ttf_min_s)} "
            f"p50={_format_seconds(v.ttf_p50_s)}" + (" circular" if v.circular else "")
            for op, v in assessment.victims.items()
        )
        line += f" | victims for {name(assessment.recipient)}=[{victims}]"
    return line
