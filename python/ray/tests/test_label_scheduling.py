import pytest
import ray
import sys


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


def test_scheduling_with_label_selector(ray_start_cluster):
    # Start cluster with node labels
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

    equals_actor = MyActor.options(
        label_selector={"ray.io/accelerator-type": "A100"},
    ).remote()
    assert ray.get(equals_actor.get_node_id.remote(), timeout=3) in (node_1)

    not_equals_actor = MyActor.options(
        label_selector={"ray.io/accelerator-type": "!A100", "market-type": "spot"},
    ).remote()
    assert ray.get(not_equals_actor.get_node_id.remote(), timeout=3) in (node_2)

    in_actor = MyActor.options(
        label_selector={"region": "in(us-west4, us-central2)"},
    ).remote()
    assert ray.get(in_actor.get_node_id.remote(), timeout=3) in (node_1)

    not_in_actor = MyActor.options(
        label_selector={
            "ray.io/accelerator-type": "!in(A100)",
            "region": "!in(us-east4, us-west4)",
        },
    ).remote()
    assert ray.get(not_in_actor.get_node_id.remote(), timeout=3) in (node_2)

    multiple_selectors = MyActor.options(
        label_selector={"ray.io/accelerator-type": "TPU", "region": "us-east4"},
    ).remote()
    assert ray.get(multiple_selectors.get_node_id.remote(), timeout=3) in (node_3)


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
