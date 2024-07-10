import pytest

import ray
from ray.anyscale.train._internal.execution.scaling_policy.autoscaling_requester import (  # noqa: E501
    TrainAutoscalingRequester,
)
from ray.cluster_utils import Cluster


@pytest.fixture
def cluster():
    """Initialize a Ray cluster with a 0 CPU head node and no workers."""
    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


def test_node_resources(cluster):
    autoscaling_requester = TrainAutoscalingRequester()

    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=8)
    node_to_remove = cluster.add_node(num_cpus=16)
    cluster.add_node(num_cpus=32)

    cluster.remove_node(node_to_remove)
    cluster.wait_for_nodes()

    node_resources = autoscaling_requester.node_resources()
    node_cpus = [resource.get("CPU", 0) for resource in node_resources]
    # The 0 here is the 0 CPU head node.
    assert sorted(node_cpus) == [0, 2, 4, 8, 32]


if __name__ == "__main__":
    pytest.main(["-v", __file__])
