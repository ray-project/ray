import sys

import pytest

from ray.serve._private import scheduling_rules
from ray.serve._private.common import DeploymentID
from ray.serve._private.scheduling_rules import (
    RAY_NODE_ID_LABEL,
    ExcludeNodesConstraint,
    MinTopologyDomainsConstraint,
    SchedulingConstraint,
    SchedulingContext,
    label_value,
    rule_tiers,
)

DEPLOYMENT_ID = DeploymentID(name="d", app_name="app")


ZONE = "ray.io/availability-zone"

NODE_LABELS = {
    "a1": {ZONE: "a"},
    "a2": {ZONE: "a"},
    "b1": {ZONE: "b"},
    "u1": {},
}


def make_ctx(
    node_labels=None, replicas_per_node=None, num_replicas=0, candidate_nodes=None
) -> SchedulingContext:
    return SchedulingContext(
        deployment_id=DEPLOYMENT_ID,
        node_labels=node_labels or {},
        replicas_per_node=replicas_per_node or {},
        num_replicas=num_replicas,
        candidate_nodes=candidate_nodes,
    )


def test_base_constraint_allows_every_node():
    assert SchedulingConstraint().allowed_nodes({"n1", "n2"}, make_ctx()) == {
        "n1",
        "n2",
    }
    assert SchedulingConstraint().describe() == "SchedulingConstraint"


def test_exclude_nodes_follows_the_current_set():
    excluded = set()
    rule = ExcludeNodesConstraint(lambda: excluded)
    ctx = make_ctx()

    assert rule.allowed_nodes({"n1", "n2"}, ctx) == {"n1", "n2"}

    excluded.add("n1")
    assert rule.allowed_nodes({"n1", "n2"}, ctx) == {"n2"}

    excluded.clear()
    assert rule.allowed_nodes({"n1", "n2"}, ctx) == {"n1", "n2"}


def test_label_value():
    assert label_value("a1", ZONE, NODE_LABELS) == "a"
    assert label_value("u1", ZONE, NODE_LABELS) is None
    assert label_value("unknown", ZONE, NODE_LABELS) is None
    assert label_value("u1", RAY_NODE_ID_LABEL, NODE_LABELS) == "u1"


def test_floor_allows_only_uncovered_domains_while_unmet():
    rule = MinTopologyDomainsConstraint(ZONE, 2)
    ctx = make_ctx(NODE_LABELS, replicas_per_node={"a1": 1}, num_replicas=2)

    assert not rule.is_met(ctx)
    assert rule.covered_domains(ctx) == {"a"}
    assert rule.allowed_nodes(NODE_LABELS, ctx) == {"b1"}


def test_floor_allows_every_node_once_met():
    rule = MinTopologyDomainsConstraint(ZONE, 2)
    ctx = make_ctx(NODE_LABELS, replicas_per_node={"a1": 1, "b1": 1}, num_replicas=3)

    assert rule.is_met(ctx)
    assert rule.allowed_nodes(NODE_LABELS, ctx) == set(NODE_LABELS)


def test_floor_is_capped_at_the_replica_count():
    rule = MinTopologyDomainsConstraint(ZONE, 3)

    ctx = make_ctx(NODE_LABELS, num_replicas=1)
    assert rule.effective_floor(ctx) == 1

    ctx = make_ctx(NODE_LABELS, replicas_per_node={"a1": 1}, num_replicas=1)
    assert rule.is_met(ctx)


def test_floor_is_capped_at_the_domains_the_replica_can_use():
    rule = MinTopologyDomainsConstraint(ZONE, 3)

    ctx = make_ctx(NODE_LABELS, num_replicas=6)
    assert rule.effective_floor(ctx) == 2

    ctx = make_ctx(
        NODE_LABELS,
        replicas_per_node={"a1": 1},
        num_replicas=6,
        candidate_nodes={"a1", "a2"},
    )
    assert rule.effective_floor(ctx) == 1
    assert rule.is_met(ctx)


def test_domain_counts_follow_the_candidate_nodes(monkeypatch):
    ctx = make_ctx(NODE_LABELS, candidate_nodes={"a1", "a2", "u1"})
    assert ctx.num_domains(ZONE) == 1
    assert ctx.num_domains(RAY_NODE_ID_LABEL) == 3
    assert make_ctx(NODE_LABELS).num_domains(ZONE) == 2

    calls = []
    real_label_value = scheduling_rules.label_value
    monkeypatch.setattr(
        scheduling_rules,
        "label_value",
        lambda *args: calls.append(args) or real_label_value(*args),
    )
    rule = MinTopologyDomainsConstraint(ZONE, 3)
    ctx = make_ctx(NODE_LABELS, num_replicas=6)
    assert rule.effective_floor(ctx) == 2
    num_calls = len(calls)
    assert rule.effective_floor(ctx) == 2
    assert not rule.is_met(ctx)
    assert len(calls) == num_calls


def test_node_floor_counts_distinct_nodes():
    rule = MinTopologyDomainsConstraint(RAY_NODE_ID_LABEL, 3)
    ctx = make_ctx(NODE_LABELS, replicas_per_node={"a1": 2, "u1": 1}, num_replicas=4)

    assert rule.covered_domains(ctx) == {"a1", "u1"}
    assert rule.allowed_nodes(NODE_LABELS, ctx) == {"a2", "b1"}


def test_floor_describe():
    assert (
        MinTopologyDomainsConstraint(ZONE, 2).describe()
        == "topology_spread[ray.io/availability-zone>=2]"
    )


def test_rule_tiers_drop_optional_rules_last_listed_first():
    first = MinTopologyDomainsConstraint(ZONE, 2)
    second = MinTopologyDomainsConstraint(RAY_NODE_ID_LABEL, 3)
    required = ExcludeNodesConstraint(set)

    assert rule_tiers([first, second, required]) == [
        (required, first, second),
        (required, first),
        (required,),
    ]
    assert rule_tiers([required]) == [(required,)]
    assert rule_tiers([]) == [()]


def test_floor_refuses_a_stop_that_drops_below_it():
    rule = MinTopologyDomainsConstraint(ZONE, 2)
    ctx = make_ctx(NODE_LABELS, replicas_per_node={"a1": 2, "b1": 1}, num_replicas=2)

    assert not rule.may_stop("b1", ctx)
    assert rule.may_stop("a1", ctx)
    assert rule.may_stop("u1", ctx)


def test_floor_allows_stops_down_to_the_new_replica_count():
    rule = MinTopologyDomainsConstraint(ZONE, 2)
    ctx = make_ctx(NODE_LABELS, replicas_per_node={"a1": 1, "b1": 1}, num_replicas=1)

    assert rule.may_stop("b1", ctx)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
