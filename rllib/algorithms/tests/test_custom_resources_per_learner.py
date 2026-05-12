import gymnasium as gym
import pytest

import ray
import ray._common
from ray.cluster_utils import Cluster
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.testing.testing_learner import BaseTestingAlgorithmConfig

EXPECTED_PER_NODE_OBJECT_STORE_MEMORY = 10**8
HEAD_REDIS_PORT = 6379
HEAD_CPUS = 4
WORKER_CPUS = 4
PINNED_RESOURCE = "learner_pool"
DECOY_RESOURCE = "decoy_pool"


def test_round_trip():
    """Test that custom_resources_per_learner is set correctly."""
    cfg = AlgorithmConfig().learners(
        custom_resources_per_learner={"my_label": 0.001, "other": 1}
    )
    assert cfg.custom_resources_per_learner == {"my_label": 0.001, "other": 1}


@pytest.fixture
def cluster():
    """3-node fake cluster:

    - head:    HEAD_CPUS CPUs, no custom resources
    - pinned:  WORKER_CPUS CPUs, {PINNED_RESOURCE: 4}
    - decoy:   WORKER_CPUS CPUs, {DECOY_RESOURCE: 4} (must not host learners
               when requesting PINNED_RESOURCE)
    """
    assert (
        3 * EXPECTED_PER_NODE_OBJECT_STORE_MEMORY
        < ray._common.utils.get_system_memory() / 2
    ), "Not enough memory on this machine to run this workload."

    cluster = Cluster()
    head = cluster.add_node(
        redis_port=HEAD_REDIS_PORT,
        num_cpus=HEAD_CPUS,
        object_store_memory=EXPECTED_PER_NODE_OBJECT_STORE_MEMORY,
        include_dashboard=True,
    )
    pinned = cluster.add_node(
        num_cpus=WORKER_CPUS,
        resources={PINNED_RESOURCE: 4},
        object_store_memory=EXPECTED_PER_NODE_OBJECT_STORE_MEMORY,
        include_dashboard=False,
    )
    decoy = cluster.add_node(
        num_cpus=WORKER_CPUS,
        resources={DECOY_RESOURCE: 4},
        object_store_memory=EXPECTED_PER_NODE_OBJECT_STORE_MEMORY,
        include_dashboard=False,
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    yield {"cluster": cluster, "head": head, "pinned": pinned, "decoy": decoy}

    ray.shutdown()
    cluster.shutdown()


def test_learners_pinned_to_node_with_custom_resource(cluster):
    """4 learners requesting PINNED_RESOURCE must all land on the pinned node,
    even though head+decoy together have 8 free CPUs that would otherwise
    absorb them.
    """
    config = BaseTestingAlgorithmConfig().learners(
        num_learners=4,
        num_cpus_per_learner=1,
        custom_resources_per_learner={PINNED_RESOURCE: 1},
    )
    learner_group = config.build_learner_group(env=gym.make("CartPole-v1"))

    refs = learner_group.foreach_learner(
        lambda _: ray.get_runtime_context().get_node_id()
    )
    node_ids = [r.get() for r in refs]
    assert len(node_ids) == 4
    assert all(nid == cluster["pinned"].node_id for nid in node_ids)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
