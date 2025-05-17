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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
