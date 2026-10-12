import pytest

import ray._raylet
import ray.train
from ray.train._internal.autoscaling_coordinator_client import (
    build_train_resource_request,
)
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ResizeDecision,
)
from ray.train.v2._internal.execution.scaling_policy.fixed import FixedScalingPolicy

NODE_ID_KEY = ray._raylet.RAY_NODE_ID_KEY


class _StubCoordinatorClient:
    """Stands in for ``TrainAutoscalingCoordinatorClient``.

    ``cached`` is returned for plain reads and ``fresh`` for ``recompute=True``
    reads, so tests can make the two views disagree.
    """

    def __init__(self, cached, fresh=None):
        self.cached = cached
        self.fresh = cached if fresh is None else fresh
        self.num_queries = 0

    def get_reserved_resources(self, *, recompute=False):
        self.num_queries += 1
        return self.fresh if recompute else self.cached

    def maybe_send_resource_request(self, **kwargs):
        pass


def _policy_with_reservation(reserved_resources, fresh=None, **scaling_config_kwargs):
    policy = FixedScalingPolicy(ray.train.ScalingConfig(**scaling_config_kwargs))
    policy._coordinator_client = _StubCoordinatorClient(reserved_resources, fresh)
    return policy


def test_reserved_resources_become_one_node_pin_per_worker():
    policy = _policy_with_reservation(
        {"node-a": {"CPU": 2}, "node-b": {"CPU": 1}},
        num_workers=3,
        resources_per_worker={"CPU": 1},
    )

    decision = policy.make_decision_for_non_running_worker_group()

    assert isinstance(decision, ResizeDecision)
    assert decision.num_workers == 3
    assert decision.label_selectors == [
        {NODE_ID_KEY: "node-a"},
        {NODE_ID_KEY: "node-a"},
        {NODE_ID_KEY: "node-b"},
    ]


@pytest.mark.parametrize(
    "reserved_resources",
    [
        pytest.param({}, id="nothing_reserved"),
        pytest.param({"node-a": {"CPU": 1}}, id="covers_one_of_two"),
    ],
)
def test_waits_until_the_reservation_covers_every_worker(reserved_resources):
    """Pinning only some workers would leave the rest to schedule anywhere, so
    the policy waits (like elastic waits for ``min_workers``) until every
    worker is covered."""
    policy = _policy_with_reservation(
        reserved_resources,
        num_workers=2,
        resources_per_worker={"CPU": 1},
    )

    assert isinstance(policy.make_decision_for_non_running_worker_group(), NoopDecision)

    policy._coordinator_client.cached = policy._coordinator_client.fresh = {
        "node-a": {"CPU": 1},
        "node-b": {"CPU": 1},
    }
    decision = policy.make_decision_for_non_running_worker_group()

    assert decision.label_selectors == [
        {NODE_ID_KEY: "node-a"},
        {NODE_ID_KEY: "node-b"},
    ]


def test_pins_come_from_a_fresh_reservation():
    """The cached view can still include a node that just died. Pins to it would
    leave the placement group waiting for a node that no longer exists."""
    policy = _policy_with_reservation(
        {"node-a": {"CPU": 1}, "node-b": {"CPU": 1}},
        fresh={"node-a": {"CPU": 1}},
        num_workers=2,
        resources_per_worker={"CPU": 1},
    )

    assert isinstance(policy.make_decision_for_non_running_worker_group(), NoopDecision)


@pytest.mark.parametrize(
    "placement_strategy,reserved_resources,is_ready",
    [
        pytest.param(
            "STRICT_PACK", {"node-a": {"CPU": 2}}, True, id="strict_pack_one_node"
        ),
        pytest.param(
            "STRICT_PACK",
            {"node-a": {"CPU": 1}, "node-b": {"CPU": 1}},
            False,
            id="strict_pack_split_over_two_nodes",
        ),
        pytest.param(
            "STRICT_SPREAD",
            {"node-a": {"CPU": 1}, "node-b": {"CPU": 1}},
            True,
            id="strict_spread_distinct_nodes",
        ),
        pytest.param(
            "STRICT_SPREAD",
            {"node-a": {"CPU": 2}},
            False,
            id="strict_spread_stacked_on_one_node",
        ),
        # The non-strict strategies place no constraint on the node count.
        pytest.param("PACK", {"node-a": {"CPU": 2}}, True, id="pack_one_node"),
        pytest.param(
            "SPREAD",
            {"node-a": {"CPU": 1}, "node-b": {"CPU": 1}},
            True,
            id="spread_two_nodes",
        ),
    ],
)
def test_reservation_must_satisfy_the_requested_placement_strategy(
    placement_strategy, reserved_resources, is_ready
):
    """Pinning to a layout that contradicts the strategy would silently
    downgrade it -- e.g. STRICT_SPREAD workers landing on the same node."""
    policy = _policy_with_reservation(
        reserved_resources,
        num_workers=2,
        resources_per_worker={"CPU": 1},
        placement_strategy=placement_strategy,
    )

    decision = policy.make_decision_for_non_running_worker_group()

    assert isinstance(decision, ResizeDecision) == is_ready


@pytest.mark.parametrize(
    "scaling_config_kwargs",
    [
        # Zero-resource workers fit anywhere, so there is nothing to reserve
        # and nothing to wait for.
        pytest.param({"resources_per_worker": {"CPU": 0}}, id="zero_resource_workers"),
        # The coordinator never matches label selectors against node labels,
        # so its pins could violate the user's selector. The user's wins.
        pytest.param(
            {"resources_per_worker": {"CPU": 1}, "label_selector": {"a": "b"}},
            id="user_label_selector",
        ),
        # SlicePlacementGroup does its own reservation and ignores
        # `label_selector`, so pins would only add latency.
        pytest.param({"use_tpu": True, "resources_per_worker": {"TPU": 4}}, id="tpu"),
    ],
)
def test_no_pins_and_no_wait_when_pinning_does_not_apply(scaling_config_kwargs):
    policy = _policy_with_reservation({}, num_workers=1, **scaling_config_kwargs)

    decision = policy.make_decision_for_non_running_worker_group()

    assert isinstance(decision, ResizeDecision)
    assert decision.label_selectors is None
    assert policy._coordinator_client.num_queries == 0


def test_build_train_resource_request_excludes_trainer_bundle_v2():
    scaling_config = ray.train.ScalingConfig(
        num_workers=2,
        use_gpu=True,
        resources_per_worker={"CPU": 4, "GPU": 1},
    )

    resources, label_selectors = build_train_resource_request(scaling_config, 2)

    assert resources == [
        {"CPU": 4, "GPU": 1},
        {"CPU": 4, "GPU": 1},
    ]
    assert label_selectors is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
