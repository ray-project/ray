import pytest

import ray.air
from ray.train._internal.autoscaling_coordinator_client import (
    _reserved_resources_to_bundle_label_selectors,
    build_train_resource_request,
)


def test_build_train_resource_request_includes_default_trainer_bundle_v1():
    scaling_config = ray.air.ScalingConfig(
        num_workers=2,
        use_gpu=True,
        resources_per_worker={"CPU": 4, "GPU": 1},
    )

    resources, label_selectors = build_train_resource_request(scaling_config, 2)

    assert resources == [
        {"CPU": 1},  # Trainer bundle for v1
        {"CPU": 4, "GPU": 1},
        {"CPU": 4, "GPU": 1},
    ]
    assert label_selectors is None


def test_build_train_resource_request_respects_zero_trainer_resources_v1():
    scaling_config = ray.air.ScalingConfig(
        num_workers=2,
        trainer_resources={"CPU": 0},
        resources_per_worker={"CPU": 1},
    )

    resources, label_selectors = build_train_resource_request(scaling_config, 2)

    assert resources == [{"CPU": 1}, {"CPU": 1}]
    assert label_selectors is None


def test_build_train_resource_request_prepends_trainer_label_selector_v1():
    scaling_config = ray.air.ScalingConfig(
        num_workers=2,
        resources_per_worker={"CPU": 1},
    )

    resources, label_selectors = build_train_resource_request(scaling_config, 2)

    assert resources == [{"CPU": 1}, {"CPU": 1}, {"CPU": 1}]
    assert label_selectors is None


@pytest.mark.parametrize(
    "reserved_resources,trainer_resources,resources_per_worker,expected_nodes",
    [
        pytest.param(
            {"n1": {"CPU": 3}}, {"CPU": 1}, {"CPU": 1}, ["n1", "n1"], id="single_node"
        ),
        pytest.param(
            # The coordinator scans nodes largest-first, so the trainer landed
            # on "z-big" -- and it keys ``reserved_resources`` in placement
            # order, so "z-big" comes first. Subtracting from the
            # alphabetically-first node instead would take 3 CPU off "a-small",
            # rounding it down to zero worker slots and reporting one worker
            # where two were reserved.
            {"z-big": {"CPU": 7}, "a-small": {"CPU": 4}},
            {"CPU": 3},
            {"CPU": 4},
            ["z-big", "a-small"],
            id="multi_node_placement_order_is_not_alphabetical",
        ),
    ],
)
def test_reserved_resources_to_bundle_label_selectors_subtracts_trainer_bundle(
    reserved_resources, trainer_resources, resources_per_worker, expected_nodes
):
    """The trainer bundle is merged into the node totals, so it has to come
    back off -- on the node that actually holds it -- before the remainder is
    divided into worker slots."""
    import ray._raylet

    node_id_key = ray._raylet.RAY_NODE_ID_KEY
    label_selectors = _reserved_resources_to_bundle_label_selectors(
        reserved_resources=reserved_resources,
        resources_per_worker=resources_per_worker,
        trainer_resources=trainer_resources,
    )

    assert label_selectors == [{node_id_key: node} for node in expected_nodes]


@pytest.mark.parametrize(
    "per_worker_gpu, num_workers",
    [(0.1, 5), (0.1, 10), (0.2, 5), (0.3, 3), (0.7, 3), (0.25, 4)],
)
def test_reserved_resources_to_bundle_label_selectors_fractional_resources(
    per_worker_gpu, num_workers
):
    """Fractional per-worker resources must not lose a slot to float error.

    The coordinator accumulates a node's reserved total one bundle at a time, so
    the total is not exactly ``num_workers * per_worker_gpu``. Flooring that with
    plain ``//`` drops a slot (0.1 GPU x 5 sums to a float that ``// 0.1`` reads as
    4), which makes a fully reserved worker group look permanently unready.
    """
    import ray._raylet

    # Mirror how the coordinator builds the per-node total: repeated addition.
    reserved_gpu = 0.0
    for _ in range(num_workers):
        reserved_gpu = reserved_gpu + per_worker_gpu

    label_selectors = _reserved_resources_to_bundle_label_selectors(
        reserved_resources={"node-a": {"GPU": reserved_gpu}},
        resources_per_worker={"GPU": per_worker_gpu},
        trainer_resources={},
    )

    assert label_selectors == [{ray._raylet.RAY_NODE_ID_KEY: "node-a"}] * num_workers


def test_reserved_resources_to_bundle_label_selectors_ignores_partial_slot():
    """Tolerating float drift must not invent a slot out of a real shortfall."""
    label_selectors = _reserved_resources_to_bundle_label_selectors(
        reserved_resources={"node-a": {"GPU": 0.9}},
        resources_per_worker={"GPU": 1},
        trainer_resources={},
    )

    assert label_selectors == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
