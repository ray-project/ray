"""Unit tests for ``LocalityBasedActorPlacement``: collocation demand and
ranking, the proportional spread, and downscale victim ordering. Pure logic,
no cluster."""
import sys
from collections import Counter

import pytest

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    OpPlacementView,
    max_placeable_actors,
)
from ray.data._internal.experimental.execution.sizer.locality_actor_placement import (
    LocalityBasedActorPlacement,
)


def _view(**overrides) -> OpPlacementView:
    base = dict(
        op_id="op",
        is_source=False,
        per_actor_usage=ExecutionResources(cpu=1),
        input_bytes_per_actor=None,
        actors_by_node={},
        input_queue_bytes_by_node={},
        upstream_actors_by_node={},
        downstream_actors_by_node={},
        victim_ids_by_node={},
        idle_actors_by_node={},
        pending_ids_by_node={},
    )
    base.update(overrides)
    return OpPlacementView(**base)


def _free(**by_node) -> dict:
    return {n: ExecutionResources(cpu=cpu) for n, cpu in by_node.items()}


def _victim_view() -> OpPlacementView:
    # Node A: 1 pending + [idle, busy]; B: [idle, busy] (queue 10);
    # C: [idle] (queue 100). idle = the 0-task prefix of victim_ids_by_node.
    return _view(
        pending_ids_by_node={"A": ["pA"]},
        victim_ids_by_node={"A": ["iA", "bA"], "B": ["iB", "bB"], "C": ["iC"]},
        idle_actors_by_node={"A": 1, "B": 1, "C": 1},
        input_queue_bytes_by_node={"B": 10, "C": 100},
    )


# --- collocation ---------------------------------------------------------------


@pytest.mark.parametrize(
    "view_kwargs,expected_collocated",
    [
        # No idle actor: a 300-byte queue converts to 3 actors of demand.
        pytest.param({}, 3, id="queue-converts-to-demand"),
        # An idle actor on the node absorbs the queue: no buffer demand.
        pytest.param(
            dict(
                actors_by_node={"A": 1},
                victim_ids_by_node={"A": ["a1"]},
                idle_actors_by_node={"A": 1},
            ),
            0,
            id="idle-actor-suppresses-buffer",
        ),
        # A pending actor gates the buffer like an idle one: it will absorb
        # the queue as soon as it lands.
        pytest.param(
            dict(actors_by_node={"A": 1}, pending_ids_by_node={"A": ["p1"]}),
            0,
            id="pending-actor-suppresses-buffer",
        ),
    ],
)
def test_locality_queue_buffer_gated_on_idle(view_kwargs, expected_collocated):
    """Queued input adds collocation demand only when no local actor is idle."""
    p = LocalityBasedActorPlacement()
    view = _view(
        input_queue_bytes_by_node={"A": 300}, input_bytes_per_actor=100, **view_kwargs
    )
    p.place_upscale(view, free=_free(A=10, B=10), count=8)
    assert p.last_upscale_breakdown["op"][0] == expected_collocated


def test_locality_live_rerank_interleaves():
    """Placing on a node decays its rank, so a big-queue node doesn't take the
    whole upscale: queue pressure per actor equalizes across nodes."""
    p = LocalityBasedActorPlacement()
    view = _view(
        upstream_actors_by_node={"A": 3, "B": 1},
        downstream_actors_by_node={"B": 1},
        actors_by_node={"A": 1},
        input_queue_bytes_by_node={"A": 400, "B": 100},
        input_bytes_per_actor=200,
    )
    # Walk (imbalance first, then queue pressure): A imb 2 -> imb tie at 1,
    # A wins on pressure 400/3 > 100 -> A exhausts imbalance, B's 1 remains
    # -> B -> imb tie at 0, A wins on pressure 100 > 50.
    # Demands: A = max(3,0)-1+2 = 4, B = max(1,1)-0+1 = 2.
    assert p.place_upscale(view, free=_free(A=10, B=10), count=4) == [
        "A",
        "A",
        "B",
        "A",
    ]
    assert p.last_upscale_breakdown["op"] == (4, 0)


def test_locality_imbalance_outranks_queue_pressure():
    """Unmatched producers (a durable rate-mismatch signal) beat a big queue
    (a noisy snapshot): balance the node first, then work on the buffer."""
    p = LocalityBasedActorPlacement()
    view = _view(
        upstream_actors_by_node={"A": 2, "B": 1},
        input_queue_bytes_by_node={"B": 1000},
        input_bytes_per_actor=100,
    )
    # Queue-first ranking would send both to B. Imbalance-first sends the
    # first to A (imb 2 > 1); at the imbalance tie, B's queue wins the second.
    assert p.place_upscale(view, free=_free(A=10, B=10), count=2) == ["A", "B"]


# --- spread --------------------------------------------------------------------


@pytest.mark.parametrize(
    "per_actor,free,count,expected",
    [
        # Proportional to free slots (not water-filling): 8x the capacity
        # gets 8x the actors.
        pytest.param(
            ExecutionResources(cpu=1),
            dict(A=64, B=8),
            9,
            Counter({"A": 8, "B": 1}),
            id="proportional-to-capacity",
        ),
        # Quotas are 4 * 2/6 = 1.33 each: floors give 1 apiece, the leftover
        # goes to the remainder tiebreak (fewest of ours, then node id) -> A.
        pytest.param(
            ExecutionResources(cpu=1),
            dict(A=2, B=2, C=2),
            4,
            Counter({"A": 2, "B": 1, "C": 1}),
            id="leftovers-by-largest-remainder",
        ),
        pytest.param(
            ExecutionResources(cpu=1),
            dict(A=1, B=1),
            10,
            Counter({"A": 1, "B": 1}),
            id="shortfall-returns-short",
        ),
        # Every node at zero capacity: place nothing (and don't divide by 0).
        pytest.param(
            ExecutionResources(cpu=1), dict(A=0, B=0), 3, Counter(), id="all-full"
        ),
        # A resource-less actor fits anywhere (inf): split evenly, and the
        # total-capacity clamp must NOT bound the placement to the node count.
        pytest.param(
            ExecutionResources(),
            dict(A=4, B=4, C=4),
            6,
            Counter({"A": 2, "B": 2, "C": 2}),
            id="resource-less-even-split",
        ),
        pytest.param(
            ExecutionResources(),
            dict(A=1, B=1),
            10,
            Counter({"A": 5, "B": 5}),
            id="resource-less-more-actors-than-nodes",
        ),
    ],
)
def test_locality_spread(per_actor, free, count, expected):
    p = LocalityBasedActorPlacement()
    placed = p.place_upscale(
        _view(per_actor_usage=per_actor), free=_free(**free), count=count
    )
    assert Counter(placed) == expected


@pytest.mark.parametrize(
    "free,per_actor,expected",
    [
        # 0.3 // 0.1 == 2.0 in floats; the count must still be 3.
        pytest.param(
            ExecutionResources(cpu=0.3),
            ExecutionResources(cpu=0.1),
            3,
            id="float-floor",
        ),
        pytest.param(
            ExecutionResources(cpu=0.29),
            ExecutionResources(cpu=0.1),
            2,
            id="no-overfit",
        ),
        # The tightest requested dimension decides; unrequested ones don't.
        pytest.param(
            ExecutionResources(cpu=8, gpu=1),
            ExecutionResources(cpu=1, gpu=0.5),
            2,
            id="tightest-dim",
        ),
    ],
)
def test_max_placeable_actors(free, per_actor, expected):
    assert max_placeable_actors(free, per_actor) == expected


# --- downscale victims ---------------------------------------------------------


@pytest.mark.parametrize(
    "count,favored,expected",
    [
        # Cost-class-major: pending/idle victims on EVERY node are taken
        # before a favored node's busy actor (node-major would take bA third).
        pytest.param(4, {"A"}, ["pA", "iA", "iB", "iC"], id="all-cheap-before-busy"),
        # Busy actors follow, favored node first.
        pytest.param(5, {"A"}, ["pA", "iA", "iB", "iC", "bA"], id="busy-favored-first"),
        pytest.param(2, {"A"}, ["pA", "iA"], id="favored-first-within-class"),
        # No favored nodes: the cheap class walks nodes by queue size.
        pytest.param(3, set(), ["pA", "iA", "iB"], id="unfavored-by-queue-size"),
    ],
)
def test_locality_victims(count, favored, expected):
    p = LocalityBasedActorPlacement()
    assert (
        p.pick_downscale_victims(_victim_view(), count=count, favored_nodes=favored)
        == expected
    )


def test_locality_victims_full_drain_returns_all_pendings_first():
    p = LocalityBasedActorPlacement()
    victims = p.pick_downscale_victims(_victim_view(), count=99, favored_nodes={"A"})
    assert victims[0] == "pA"
    assert sorted(victims) == ["bA", "bB", "iA", "iB", "iC", "pA"]


def test_locality_favored_nodes_ignore_queue_on_slow_path():
    """Each capacity-short op nominates its top-unmet nodes per reclaim timescale; the
    slow (drain-to-free) tier ranks on structure only -- C's big queue would
    win the imbalance tie if it counted, but the snapshot is stale by drain
    time, so the node-id tiebreak picks A."""
    p = LocalityBasedActorPlacement()
    view = _view(
        upstream_actors_by_node={"A": 2, "C": 2},
        input_queue_bytes_by_node={"C": 1000},
    )
    favored = p.compute_favored_nodes(
        capacity_short_views=[(view, 1)],
        all_nodes={"A", "B", "C"},
        fast_free_nodes={"B"},
    )
    assert favored == {"A", "B"}
    # Nothing unmet -> no favored nodes.
    assert (
        p.compute_favored_nodes(
            capacity_short_views=[(view, 0)],
            all_nodes={"A", "B"},
            fast_free_nodes={"B"},
        )
        == set()
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
