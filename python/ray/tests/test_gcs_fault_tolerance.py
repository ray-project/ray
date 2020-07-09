import sys

import ray
import pytest
import time
from ray.test_utils import generate_internal_config_map


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


@ray.remote
def increase(x):
    return x + 1


def test_gcs_server_restart(ray_start_regular):
    actor1 = Increase.remote()
    result = ray.get(actor1.method.remote(1))
    assert result == 3

    ray.worker._global_node.kill_gcs_server()
    ray.worker._global_node.start_gcs_server()

    result = ray.get(actor1.method.remote(7))
    assert result == 9

    actor2 = Increase.remote()
    result = ray.get(actor2.method.remote(2))
    assert result == 4

    result = ray.get(increase.remote(1))
    assert result == 2


def test_gcs_server_restart_during_actor_creation(ray_start_regular):
    ids = []
    for i in range(0, 100):
        actor = Increase.remote()
        ids.append(actor.method.remote(1))

    ray.worker._global_node.kill_gcs_server()
    ray.worker._global_node.start_gcs_server()

    ready, unready = ray.wait(ids, 100, 240)
    print("Ready objects is {}.".format(ready))
    print("Unready objects is {}.".format(unready))
    assert len(unready) == 0


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [generate_internal_config_map(num_heartbeats_timeout=20)],
    indirect=True)
def test_node_failure_detector_when_gcs_server_restart(ray_start_cluster_head):
    """Checks that the node failure detector is correct when gcs server restart.

    We set the cluster to timeout nodes after 2 seconds of no timeouts. We
    then remove a node, wait for 1 second and start gcs server again to check
    that the alive node count is 2, then wait another 2 seconds to check that
    the one of the node is timed out.
    """
    cluster = ray_start_cluster_head
    worker = cluster.add_node()
    cluster.wait_for_nodes()

    cluster.head_node.kill_gcs_server()
    cluster.remove_node(worker, allow_graceful=False)

    time.sleep(1)
    cluster.head_node.start_gcs_server()

    nodes = ray.nodes()
    assert len(nodes) == 2
    assert nodes[0]["alive"] and nodes[1]["alive"]

    time.sleep(2.5)
    nodes = ray.nodes()
    assert len(nodes) == 2

    dead_count = 0
    for node in nodes:
        if not node["alive"]:
            dead_count += 1
    assert dead_count == 1


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
