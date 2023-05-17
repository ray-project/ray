import ray
import sys
from ray.serve.scheduler import Scheduler
from ray.cluster_utils import AutoscalingCluster


def test_serve_scheduler():
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "worker": {"resources": {"CPU": 2}, "node_config": {}, "max_workers": 100}
        },
    )
    cluster.start()
    ray.init(address="auto")

    @ray.remote(num_cpus=1)
    class Deployment:
        pass

    scheduler = Scheduler(Deployment, spread_min_nodes=2)

    # Initially we only have the head node
    assert len(ray.nodes()) == 1

    scheduler.add_replica()
    # A worker node is added
    assert len(ray.nodes()) == 2

    scheduler.add_replica()
    # A worker node is added to satisfy min_nodes spread constraints
    assert len(ray.nodes()) == 3

    scheduler.add_replica()
    # min_nodes is already satisified so we don't need to add extra nodes
    assert len(ray.nodes()) == 3

    scheduler.remove_replica()
    assert len(ray.nodes()) == 3

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
