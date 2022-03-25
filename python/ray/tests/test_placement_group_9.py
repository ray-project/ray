import pytest
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils

from ray._private.test_utils import (
    get_other_nodes,
    generate_system_config_map,
    run_string_as_driver,
    wait_for_condition,
)


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


def test_automatic_cleanup_job(ray_start_cluster_enabled):
    # Make sure the placement groups created by a
    # job, actor, and task are cleaned when the job is done.
    cluster = ray_start_cluster_enabled
    num_nodes = 3
    num_cpu_per_node = 4
    # Create 3 nodes cluster.
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpu_per_node)
    cluster.wait_for_nodes()

    info = ray.init(address=cluster.address)
    available_cpus = ray.available_resources()["CPU"]
    assert available_cpus == num_nodes * num_cpu_per_node

    driver_code = f"""
import ray

ray.init(address="{info["address"]}")

def create_pg():
    pg = ray.util.placement_group(
            [{{"CPU": 1}} for _ in range(3)],
            strategy="STRICT_SPREAD")
    ray.get(pg.ready())
    return pg

@ray.remote(num_cpus=0)
def f():
    create_pg()

@ray.remote(num_cpus=0)
class A:
    def create_pg(self):
        create_pg()

ray.get(f.remote())
a = A.remote()
ray.get(a.create_pg.remote())
# Create 2 pgs to make sure multiple placement groups that belong
# to a single job will be properly cleaned.
create_pg()
create_pg()

ray.shutdown()
    """

    run_string_as_driver(driver_code)

    # Wait until the driver is reported as dead by GCS.
    def is_job_done():
        jobs = ray.state.jobs()
        for job in jobs:
            if job["IsDead"]:
                return True
        return False

    def assert_num_cpus(expected_num_cpus):
        if expected_num_cpus == 0:
            return "CPU" not in ray.available_resources()
        return ray.available_resources()["CPU"] == expected_num_cpus

    wait_for_condition(is_job_done)
    available_cpus = ray.available_resources()["CPU"]
    wait_for_condition(lambda: assert_num_cpus(num_nodes * num_cpu_per_node))


def test_automatic_cleanup_detached_actors(ray_start_cluster_enabled):
    # Make sure the placement groups created by a
    # detached actors are cleaned properly.
    cluster = ray_start_cluster_enabled
    num_nodes = 3
    num_cpu_per_node = 2
    # Create 3 nodes cluster.
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpu_per_node)
    cluster.wait_for_nodes()

    info = ray.init(address=cluster.address, namespace="default_test_namespace")
    available_cpus = ray.available_resources()["CPU"]
    assert available_cpus == num_nodes * num_cpu_per_node

    driver_code = f"""
import ray

ray.init(address="{info["address"]}", namespace="default_test_namespace")

def create_pg():
    pg = ray.util.placement_group(
            [{{"CPU": 1}} for _ in range(3)],
            strategy="STRICT_SPREAD")
    ray.get(pg.ready())
    return pg

# TODO(sang): Placement groups created by tasks launched by detached actor
# is not cleaned with the current protocol.
# @ray.remote(num_cpus=0)
# def f():
#     create_pg()

@ray.remote(num_cpus=0, max_restarts=1)
class A:
    def create_pg(self):
        create_pg()
    def create_child_pg(self):
        self.a = A.options(name="B").remote()
        ray.get(self.a.create_pg.remote())
    def kill_child_actor(self):
        ray.kill(self.a)
        try:
            ray.get(self.a.create_pg.remote())
        except Exception:
            pass

a = A.options(lifetime="detached", name="A").remote()
ray.get(a.create_pg.remote())
# TODO(sang): Currently, child tasks are cleaned when a detached actor
# is dead. We cannot test this scenario until it is fixed.
# ray.get(a.create_child_pg.remote())

ray.shutdown()
    """

    run_string_as_driver(driver_code)

    # Wait until the driver is reported as dead by GCS.
    def is_job_done():
        jobs = ray.state.jobs()
        for job in jobs:
            if job["IsDead"]:
                return True
        return False

    def assert_num_cpus(expected_num_cpus):
        if expected_num_cpus == 0:
            return "CPU" not in ray.available_resources()
        return ray.available_resources()["CPU"] == expected_num_cpus

    wait_for_condition(is_job_done)
    wait_for_condition(lambda: assert_num_cpus(num_nodes))

    # Make sure when a child actor spawned by a detached actor
    # is killed, the placement group is removed.
    a = ray.get_actor("A")
    # TODO(sang): child of detached actors
    # seem to be killed when jobs are done. We should fix this before
    # testing this scenario.
    # ray.get(a.kill_child_actor.remote())
    # assert assert_num_cpus(num_nodes)

    # Make sure placement groups are cleaned when detached actors are killed.
    ray.kill(a, no_restart=False)
    wait_for_condition(lambda: assert_num_cpus(num_nodes * num_cpu_per_node))
    # The detached actor a should've been restarted.
    # Recreate a placement group.
    ray.get(a.create_pg.remote())
    wait_for_condition(lambda: assert_num_cpus(num_nodes))
    # Kill it again and make sure the placement group
    # that is created is deleted again.
    ray.kill(a, no_restart=False)
    wait_for_condition(lambda: assert_num_cpus(num_nodes * num_cpu_per_node))


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            num_heartbeats_timeout=10, gcs_rpc_server_reconnect_timeout_s=60
        )
    ],
    indirect=True,
)
def test_create_placement_group_after_gcs_server_restart(
    ray_start_cluster_head_with_external_redis,
):
    cluster = ray_start_cluster_head_with_external_redis
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Create placement group 1 successfully.
    placement_group1 = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    ray.get(placement_group1.ready(), timeout=10)
    table = ray.util.placement_group_table(placement_group1)
    assert table["state"] == "CREATED"

    # Restart gcs server.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    # Create placement group 2 successfully.
    placement_group2 = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    ray.get(placement_group2.ready(), timeout=10)
    table = ray.util.placement_group_table(placement_group2)
    assert table["state"] == "CREATED"

    # Create placement group 3.
    # Status is `PENDING` because the cluster resource is insufficient.
    placement_group3 = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(placement_group3.ready(), timeout=2)
    table = ray.util.placement_group_table(placement_group3)
    assert table["state"] == "PENDING"


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            num_heartbeats_timeout=10, gcs_rpc_server_reconnect_timeout_s=60
        )
    ],
    indirect=True,
)
def test_create_actor_with_placement_group_after_gcs_server_restart(
    ray_start_cluster_head_with_external_redis,
):
    cluster = ray_start_cluster_head_with_external_redis
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Create a placement group.
    placement_group = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])

    # Create an actor that occupies resources after gcs server restart.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()
    actor_2 = Increase.options(
        placement_group=placement_group, placement_group_bundle_index=1
    ).remote()
    assert ray.get(actor_2.method.remote(1)) == 3


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            num_heartbeats_timeout=10, gcs_rpc_server_reconnect_timeout_s=60
        )
    ],
    indirect=True,
)
def test_bundle_recreated_when_raylet_fo_after_gcs_server_restart(
    ray_start_cluster_head_with_external_redis,
):
    cluster = ray_start_cluster_head_with_external_redis
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Create one placement group and make sure its creation successfully.
    placement_group = ray.util.placement_group([{"CPU": 2}])
    ray.get(placement_group.ready(), timeout=10)
    table = ray.util.placement_group_table(placement_group)
    assert table["state"] == "CREATED"

    # Restart gcs server.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    # Restart the raylet.
    cluster.remove_node(get_other_nodes(cluster, exclude_head=True)[-1])
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Schedule an actor and make sure its creaton successfully.
    actor = Increase.options(
        placement_group=placement_group, placement_group_bundle_index=0
    ).remote()

    assert ray.get(actor.method.remote(1), timeout=5) == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
