import os
import sys

import pytest

import ray
from ray._private.test_utils import placement_group_assert_no_leak
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_bundle_label_selector_with_repeated_labels(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, labels={"ray.io/accelerator-type": "A100"})
    node = cluster.add_node(num_cpus=4, labels={"ray.io/accelerator-type": "TPU"})
    ray.init(address=cluster.address)

    bundles = [{"CPU": 1}, {"CPU": 1}]
    label_selector = [{"ray.io/accelerator-type": "TPU"}] * 2

    placement_group = ray.util.placement_group(
        name="repeated_labels_pg",
        bundles=bundles,
        bundle_label_selector=label_selector,
    )
    ray.get(placement_group.ready())

    bundles_to_node_id = ray.util.placement_group_table()[placement_group.id.hex()][
        "bundles_to_node_id"
    ]
    assert bundles_to_node_id[0] == node.node_id
    assert bundles_to_node_id[1] == node.node_id

    placement_group_assert_no_leak([placement_group])


def test_unschedulable_bundle_label_selector(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, labels={"ray.io/accelerator-type": "A100"})
    cluster.add_node(num_cpus=1, labels={"ray.io/accelerator-type": "TPU"})
    ray.init(address=cluster.address)

    # request 2 CPUs total, but only 1 CPU available with label ray.io/accelerator-type=A100
    bundles = [{"CPU": 1}, {"CPU": 1}]
    label_selector = [{"ray.io/accelerator-type": "A100"}] * 2

    placement_group = ray.util.placement_group(
        name="unschedulable_labels_pg",
        bundles=bundles,
        bundle_label_selector=label_selector,
    )

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(placement_group.ready(), timeout=3)

    state = ray.util.placement_group_table()[placement_group.id.hex()]["stats"][
        "scheduling_state"
    ]
    assert state == "NO_RESOURCES"


def test_bundle_label_selectors_match_bundle_resources(ray_start_cluster):
    cluster = ray_start_cluster

    # Add nodes with unique labels and custom resources
    cluster.add_node(
        num_cpus=1, resources={"resource-0": 1}, labels={"region": "us-west4"}
    )
    cluster.add_node(
        num_cpus=1, resources={"resource-1": 1}, labels={"region": "us-east5"}
    )
    cluster.add_node(
        num_cpus=1, resources={"resource-2": 1}, labels={"region": "us-central2"}
    )
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)

    # Bundle label selectors to match the node labels above
    bundle_label_selectors = [
        {"region": "us-west4"},
        {"region": "us-east5"},
        {"region": "us-central2"},
    ]

    # Each bundle requests CPU and a unique custom resource
    bundles = [
        {"CPU": 1, "resource-0": 1},
        {"CPU": 1, "resource-1": 1},
        {"CPU": 1, "resource-2": 1},
    ]

    pg = ray.util.placement_group(
        name="label_selectors_match_resources",
        bundles=bundles,
        bundle_label_selector=bundle_label_selectors,
    )
    ray.get(pg.ready())

    @ray.remote
    def get_assigned_resources():
        return (
            ray.get_runtime_context().get_node_id(),
            ray.get_runtime_context().get_assigned_resources(),
        )

    node_id_to_label = {
        node["NodeID"]: node["Labels"]["region"] for node in ray.nodes()
    }

    # Launch one task per bundle to check resource mapping
    for i in range(len(bundles)):
        result = ray.get(
            get_assigned_resources.options(
                num_cpus=1,
                resources={f"resource-{i}": 1},
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=i
                ),
            ).remote()
        )
        node_id, assigned = result

        # Check node label matches expected
        assert node_id_to_label[node_id] == bundle_label_selectors[i]["region"]

        # Check resource assignment includes the expected custom resource
        assert f"resource-{i}" in assigned
        assert assigned[f"resource-{i}"] == 1.0

        # Check CPU was assigned
        assert "CPU" in assigned and assigned["CPU"] == 1.0


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
