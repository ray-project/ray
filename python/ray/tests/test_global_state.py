import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import time

import ray


# TODO(rliaw): The proper way to do this is to have the pytest config setup.
@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(10)
def test_replenish_resources(ray_start_regular):
    cluster_resources = ray.cluster_resources()
    available_resources = ray.available_resources()
    assert cluster_resources == available_resources

    @ray.remote
    def cpu_task():
        pass

    ray.get(cpu_task.remote())
    resources_reset = False

    while not resources_reset:
        available_resources = ray.available_resources()
        resources_reset = (cluster_resources == available_resources)
    assert resources_reset


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(10)
def test_uses_resources(ray_start_regular):
    cluster_resources = ray.cluster_resources()

    @ray.remote
    def cpu_task():
        time.sleep(1)

    cpu_task.remote()
    resource_used = False

    while not resource_used:
        available_resources = ray.available_resources()
        resource_used = available_resources.get(
            "CPU", 0) == cluster_resources.get("CPU", 0) - 1

    assert resource_used


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(20)
def test_add_remove_cluster_resources(ray_start_cluster_head):
    """Tests that Global State API is consistent with actual cluster."""
    cluster = ray_start_cluster_head
    assert ray.cluster_resources()["CPU"] == 1
    nodes = []
    nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 2

    cluster.remove_node(nodes.pop())
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 1

    for i in range(5):
        nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 6


def test_global_state_actor_table(ray_start_regular):
    @ray.remote
    class Actor:
        def ready(self):
            pass

    # actor table should be empty at first
    assert len(ray.actors()) == 0

    # actor table should contain only one entry
    a = Actor.remote()
    ray.get(a.ready.remote())
    assert len(ray.actors()) == 1

    # actor table should contain only this entry
    # even when the actor goes out of scope
    del a

    def get_state():
        return list(ray.actors().values())[0]["State"]

    dead_state = ray.gcs_utils.ActorTableData.DEAD
    for _ in range(10):
        if get_state() == dead_state:
            break
        else:
            time.sleep(0.5)
    assert get_state() == dead_state


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
