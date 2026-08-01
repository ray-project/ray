import sys

import pytest

import ray


@ray.remote
class MyActor:
    def __init__(self):
        self.value = 0

    def value(self):
        return self.value

    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()


@ray.remote
def get_node_id():
    return ray.get_runtime_context().get_node_id()


@pytest.fixture
def cluster_with_labeled_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"worker1": 1},
        num_cpus=3,
        labels={"ray.io/accelerator-type": "A100", "region": "us-west4"},
    )
    ray.init(address=cluster.address)
    node_1 = ray.get(get_node_id.options(resources={"worker1": 1}).remote())

    cluster.add_node(
        resources={"worker2": 1},
        num_cpus=3,
        labels={"ray.io/accelerator-type": "B200", "market-type": "spot"},
    )
    node_2 = ray.get(get_node_id.options(resources={"worker2": 1}).remote())

    cluster.add_node(
        resources={"worker3": 1},
        num_cpus=3,
        labels={
            "ray.io/accelerator-type": "TPU",
            "market-type": "on-demand",
            "region": "us-east4",
        },
    )
    node_3 = ray.get(get_node_id.options(resources={"worker3": 1}).remote())

    cluster.wait_for_nodes()
    return node_1, node_2, node_3


def test_label_selector_equals(cluster_with_labeled_nodes):
    node_1, _, _ = cluster_with_labeled_nodes
    actor = MyActor.options(label_selector={"ray.io/accelerator-type": "A100"}).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_1


def test_label_selector_not_equals(cluster_with_labeled_nodes):
    _, node_2, _ = cluster_with_labeled_nodes
    actor = MyActor.options(
        label_selector={"ray.io/accelerator-type": "!A100", "market-type": "spot"}
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_2


def test_label_selector_in(cluster_with_labeled_nodes):
    node_1, _, _ = cluster_with_labeled_nodes
    actor = MyActor.options(
        label_selector={"region": "in(us-west4, us-central2)"}
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_1


def test_label_selector_not_in(cluster_with_labeled_nodes):
    _, node_2, _ = cluster_with_labeled_nodes
    actor = MyActor.options(
        label_selector={
            "ray.io/accelerator-type": "!in(A100)",
            "region": "!in(us-east4, us-west4)",
        }
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_2


def test_label_selector_multiple(cluster_with_labeled_nodes):
    _, _, node_3 = cluster_with_labeled_nodes
    actor = MyActor.options(
        label_selector={"ray.io/accelerator-type": "TPU", "region": "us-east4"}
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_3


def test_fallback_strategy(cluster_with_labeled_nodes):
    # Create a RayCluster with labelled nodes.
    gpu_node, _, _ = cluster_with_labeled_nodes

    # Define an unsatisfiable label selector.
    infeasible_label_selector = {"ray.io/accelerator-type": "does-not-exist"}

    # Create a fallback strategy with multiple accelerator options.
    accelerator_fallbacks = [
        {"label_selector": {"ray.io/accelerator-type": "A100"}},
        {"label_selector": {"ray.io/accelerator-type": "TPU"}},
    ]

    # Attempt to schedule the actor. The scheduler should fail to find a node with the
    # primary `label_selector` and fall back to the first available option, 'A100'.
    label_selector_actor = MyActor.options(
        label_selector=infeasible_label_selector,
        fallback_strategy=accelerator_fallbacks,
    ).remote()

    # Assert that the actor was scheduled on the expected node.
    assert ray.get(label_selector_actor.get_node_id.remote(), timeout=5) == gpu_node


def test_empty_selector_fallback_strategy(cluster_with_labeled_nodes):
    node_1, node_2, node_3 = cluster_with_labeled_nodes

    # Define an unsatisfiable label selector.
    infeasible_label_selector = {"ray.io/accelerator-type": "does-not-exist"}

    # Create a fallback strategy with multiple label selector fallbacks. The
    # first fallback option is unsatisfiable, so it falls back to the empty label
    # selector option. This fallback should match any node.
    accelerator_fallbacks = [
        {"label_selector": {"ray.io/accelerator-type": "also-does-not-exist"}},
        {"label_selector": {}},
    ]

    label_selector_actor = MyActor.options(
        label_selector=infeasible_label_selector,
        fallback_strategy=accelerator_fallbacks,
    ).remote()

    # Assert that the actor was scheduled on the expected node.
    assert ray.get(label_selector_actor.get_node_id.remote(), timeout=5) in {
        node_1,
        node_2,
        node_3,
    }


def test_infeasible_fallback_strategy(cluster_with_labeled_nodes):
    # Define an unsatisfiable label selector and fallback strategy.
    label_selector = {"ray.io/accelerator-type": "does-not-exist"}
    fallback_strategy = [
        {"label_selector": {"ray.io/accelerator-type": "does-not-exist-either"}},
        {"label_selector": {"ray.io/accelerator-type": "also-nonexistant"}},
    ]

    # Attempt to schedule the actor, but it should timeout since none of
    # the nodes match any label selector.
    label_selector_actor = MyActor.options(
        label_selector=label_selector, fallback_strategy=fallback_strategy
    ).remote()
    with pytest.raises(TimeoutError):
        ray.get(label_selector_actor.get_node_id.remote(), timeout=3)


def test_fallback_with_feasible_primary_selector(cluster_with_labeled_nodes):
    gpu_node, _, _ = cluster_with_labeled_nodes

    feasible_label_selector = {"ray.io/accelerator-type": "A100"}
    feasible_fallback_strategy = [
        {"label_selector": {"ray.io/accelerator-type": "B200"}},
    ]

    # Attempt to schedule the actor. The scheduler should use the
    # primary selector and ignore the fallback.
    label_selector_actor = MyActor.options(
        label_selector=feasible_label_selector,
        fallback_strategy=feasible_fallback_strategy,
    ).remote()

    # Assert that the actor was scheduled on the expected node and not the fallback.
    assert ray.get(label_selector_actor.get_node_id.remote(), timeout=5) == gpu_node


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
