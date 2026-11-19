import sys

import pytest

from ray.serve._private.common import DeploymentID
from ray.serve._private.scheduling_rules import (
    ExcludeNodesConstraint,
    SchedulingConstraint,
    SchedulingContext,
)

DEPLOYMENT_ID = DeploymentID(name="d", app_name="app")


def make_ctx(node_labels=None) -> SchedulingContext:
    return SchedulingContext(deployment_id=DEPLOYMENT_ID, node_labels=node_labels or {})


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
