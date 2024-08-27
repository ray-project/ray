import os
import sys
import time
import pytest

import ray
from ray.util.state import list_actors


def test_implicit_resource(ray_start_regular):
    @ray.remote(num_cpus=1, resources={ray._raylet.IMPLICIT_RESOURCE_PREFIX + "a": 1})
    class Actor:
        def ping(self):
            return ray.get_runtime_context().get_node_id()

    # The first actor is schedulable.
    a1 = Actor.remote()
    ray.get(a1.ping.remote())

    # The second actor will be pending since
    # only one such actor can run on a single node.
    a2 = Actor.remote()
    time.sleep(2)
    actors = list_actors(filters=[("actor_id", "=", a2._actor_id.hex())])
    assert actors[0]["state"] == "PENDING_CREATION"


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_implicit_resource_autoscaling(autoscaler_v2, shutdown_only):
    from ray.cluster_utils import AutoscalingCluster

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 8,
                },
                "node_config": {},
                "min_workers": 1,
                "max_workers": 100,
            },
        },
        autoscaler_v2=autoscaler_v2,
    )

    cluster.start()
    ray.init()

    @ray.remote(num_cpus=1, resources={ray._raylet.IMPLICIT_RESOURCE_PREFIX + "a": 0.5})
    class Actor:
        def ping(self):
            return ray.get_runtime_context().get_node_id()

    actors = [Actor.remote() for _ in range(2)]
    for actor in actors:
        ray.get(actor.ping.remote())

    # No new worker nodes should be started.
    assert len(ray.nodes()) == 2

    # 3 more worker nodes should be started.
    actors.extend([Actor.remote() for _ in range(5)])
    node_id_to_num_actors = {}
    for actor in actors:
        node_id = ray.get(actor.ping.remote())
        node_id_to_num_actors[node_id] = node_id_to_num_actors.get(node_id, 0) + 1
    assert len(ray.nodes()) == 5
    assert len(node_id_to_num_actors) == 4
    num_actors_per_node = list(node_id_to_num_actors.values())
    num_actors_per_node.sort()
    assert num_actors_per_node == [1, 2, 2, 2]

    cluster.shutdown()


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
