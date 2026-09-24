import pytest

import ray._raylet
import ray.train
from ray.train._internal.autoscaling_coordinator_client import (
    build_train_resource_request,
)
from ray.train.v2._internal.constants import WORKER_GROUP_START_TIMEOUT_S_ENV_VAR
from ray.train.v2._internal.execution.scaling_policy.fixed import FixedScalingPolicy

NODE_ID_KEY = ray._raylet.RAY_NODE_ID_KEY


class _StubCoordinatorClient:
    """Stands in for ``TrainAutoscalingCoordinatorClient`` with a fixed answer."""

    def __init__(self, reserved_resources):
        self._reserved_resources = reserved_resources

    def get_reserved_resources(self, **kwargs):
        return self._reserved_resources


def _policy_with_reservation(reserved_resources, **scaling_config_kwargs):
    policy = FixedScalingPolicy(ray.train.ScalingConfig(**scaling_config_kwargs))
    policy._coordinator_client = _StubCoordinatorClient(reserved_resources)
    return policy


def test_reserved_resources_become_one_node_pin_per_worker():
    policy = _policy_with_reservation(
        {"node-a": {"CPU": 2}, "node-b": {"CPU": 1}},
        num_workers=3,
        resources_per_worker={"CPU": 1},
    )

    assert policy.get_reserved_bundle_label_selectors(3) == [
        {NODE_ID_KEY: "node-a"},
        {NODE_ID_KEY: "node-a"},
        {NODE_ID_KEY: "node-b"},
    ]


@pytest.mark.parametrize(
    "reserved_resources,resources_per_worker",
    [
        pytest.param({}, {"CPU": 1}, id="nothing_reserved"),
        pytest.param({"node-a": {"CPU": 1}}, {"CPU": 1}, id="covers_one_of_two"),
        pytest.param({"node-a": {"CPU": 5}}, {"CPU": 1}, id="covers_more_than_asked"),
        # Nothing to reserve for a zero-resource worker, so there is nothing to
        # pin to either -- and nothing to wait for.
        pytest.param({}, {"CPU": 0}, id="zero_resource_workers"),
    ],
)
def test_no_pins_unless_the_reservation_covers_every_worker(
    reserved_resources, resources_per_worker, monkeypatch
):
    """Pinning only some workers would leave the rest to schedule anywhere, so
    anything short of full coverage yields no pins at all."""
    monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "0")
    policy = _policy_with_reservation(
        reserved_resources,
        num_workers=2,
        resources_per_worker=resources_per_worker,
    )

    assert policy.get_reserved_bundle_label_selectors(2) is None


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
    placement_strategy, reserved_resources, is_ready, monkeypatch
):
    """Pinning to a layout that contradicts the strategy would silently
    downgrade it -- e.g. STRICT_SPREAD workers landing on the same node."""
    monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "0")
    policy = _policy_with_reservation(
        reserved_resources,
        num_workers=2,
        resources_per_worker={"CPU": 1},
        placement_strategy=placement_strategy,
    )

    selectors = policy.get_reserved_bundle_label_selectors(2)

    assert (selectors is not None) == is_ready


def test_reservation_wait_polls_until_ready(monkeypatch):
    """Like pg.wait(), keep querying until reserved nodes show up."""
    policy = _policy_with_reservation(
        {},
        num_workers=2,
        resources_per_worker={"CPU": 1},
    )
    answers = iter(
        [
            {},
            {"node-a": {"CPU": 1}, "node-b": {"CPU": 1}},
        ]
    )
    policy._coordinator_client.get_reserved_resources = lambda **kwargs: next(answers)
    monkeypatch.setattr(
        "ray.train.v2._internal.execution.scaling_policy.scaling_policy.time.sleep",
        lambda _: None,
    )

    assert policy.get_reserved_bundle_label_selectors(2) == [
        {NODE_ID_KEY: "node-a"},
        {NODE_ID_KEY: "node-b"},
    ]


def test_reservation_wait_returns_none_after_timeout(monkeypatch):
    """Give up after the start timeout rather than blocking the worker-group start."""
    monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "3")
    policy = _policy_with_reservation(
        {},
        num_workers=2,
        resources_per_worker={"CPU": 1},
    )
    fake_now = [0.0]
    monkeypatch.setattr(
        "ray.train.v2._internal.execution.scaling_policy.scaling_policy.time.monotonic",
        lambda: fake_now[0],
    )
    monkeypatch.setattr(
        "ray.train.v2._internal.execution.scaling_policy.scaling_policy.time.sleep",
        lambda seconds: fake_now.__setitem__(0, fake_now[0] + seconds),
    )

    assert policy.get_reserved_bundle_label_selectors(2) is None


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
