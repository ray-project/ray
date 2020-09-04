import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import time

import ray
import ray.ray_constants
import ray.test_utils


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
@pytest.mark.timeout(120)
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


def test_global_state_actor_entry(ray_start_regular):
    @ray.remote
    class Actor:
        def ready(self):
            pass

    # actor table should be empty at first
    assert len(ray.actors()) == 0

    a = Actor.remote()
    b = Actor.remote()
    ray.get(a.ready.remote())
    ray.get(b.ready.remote())
    assert len(ray.actors()) == 2
    a_actor_id = a._actor_id.hex()
    b_actor_id = b._actor_id.hex()
    assert ray.actors(actor_id=a_actor_id)["ActorID"] == a_actor_id
    assert ray.actors(
        actor_id=a_actor_id)["State"] == ray.gcs_utils.ActorTableData.ALIVE
    assert ray.actors(actor_id=b_actor_id)["ActorID"] == b_actor_id
    assert ray.actors(
        actor_id=b_actor_id)["State"] == ray.gcs_utils.ActorTableData.ALIVE


@pytest.mark.parametrize("max_shapes", [0, 2, -1])
def test_load_report(shutdown_only, max_shapes):
    resource1 = "A"
    resource2 = "B"
    cluster = ray.init(
        num_cpus=1,
        resources={resource1: 1},
        _system_config={
            "max_resource_shapes_per_load_report": max_shapes,
        })
    redis = ray.services.create_redis_client(
        cluster["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    client = redis.pubsub(ignore_subscribe_messages=True)
    client.psubscribe(ray.gcs_utils.XRAY_HEARTBEAT_BATCH_PATTERN)

    @ray.remote
    def sleep():
        time.sleep(1000)

    sleep.remote()
    for _ in range(3):
        sleep.remote()
        sleep.options(resources={resource1: 1}).remote()
        sleep.options(resources={resource2: 1}).remote()

    class Checker:
        def __init__(self):
            self.report = None

        def check_load_report(self):
            try:
                message = client.get_message()
            except redis.exceptions.ConnectionError:
                pass
            if message is None:
                return False

            pattern = message["pattern"]
            data = message["data"]
            if pattern != ray.gcs_utils.XRAY_HEARTBEAT_BATCH_PATTERN:
                return False

            pub_message = ray.gcs_utils.PubSubMessage.FromString(data)
            heartbeat_data = pub_message.data
            heartbeat = ray.gcs_utils.HeartbeatBatchTableData.FromString(
                heartbeat_data)
            self.report = heartbeat.resource_load_by_shape.resource_demands
            if max_shapes == 0:
                return True
            elif max_shapes == 2:
                return len(self.report) >= 2
            else:
                return len(self.report) >= 3

    # Wait for load information to arrive.
    checker = Checker()
    ray.test_utils.wait_for_condition(checker.check_load_report)

    # Check that we respect the max shapes limit.
    if max_shapes != -1:
        assert len(checker.report) <= max_shapes

    if max_shapes > 0:
        # Check that we always include the 1-CPU resource shape.
        one_cpu_shape = {"CPU": 1}
        one_cpu_found = False
        for demand in checker.report:
            if demand.shape == one_cpu_shape:
                one_cpu_found = True
        assert one_cpu_found

        # Check that we differentiate between infeasible and ready tasks.
        for demand in checker.report:
            if resource2 in demand.shape:
                assert demand.num_infeasible_requests_queued > 0
                assert demand.num_ready_requests_queued == 0
            else:
                assert demand.num_ready_requests_queued > 0
                assert demand.num_infeasible_requests_queued == 0


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
