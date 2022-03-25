import pytest
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils
import ray._private.gcs_utils as gcs_utils

from ray.autoscaler._private.commands import debug_status
from ray._private.test_utils import (
    generate_system_config_map,
    kill_actor_and_wait_for_failure,
    run_string_as_driver,
    wait_for_condition,
    is_placement_group_removed,
    convert_actor_state,
)
from ray.exceptions import RaySystemError
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.client.ray_client_helpers import connect_to_client_or_not
import ray.experimental.internal_kv as internal_kv
from ray.ray_constants import DEBUG_AUTOSCALING_ERROR, DEBUG_AUTOSCALING_STATUS


def get_ray_status_output(address):
    gcs_client = gcs_utils.GcsClient(address=address)
    internal_kv._initialize_internal_kv(gcs_client)
    status = internal_kv._internal_kv_get(DEBUG_AUTOSCALING_STATUS)
    error = internal_kv._internal_kv_get(DEBUG_AUTOSCALING_ERROR)
    return {
        "demand": debug_status(status, error)
        .split("Demands:")[1]
        .strip("\n")
        .strip(" "),
        "usage": debug_status(status, error)
        .split("Demands:")[0]
        .split("Usage:")[1]
        .strip("\n")
        .strip(" "),
    }


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            num_heartbeats_timeout=10, gcs_rpc_server_reconnect_timeout_s=60
        )
    ],
    indirect=True,
)
def test_create_placement_group_during_gcs_server_restart(
    ray_start_cluster_head_with_external_redis,
):
    cluster = ray_start_cluster_head_with_external_redis
    cluster.add_node(num_cpus=200)
    cluster.wait_for_nodes()

    # Create placement groups during gcs server restart.
    placement_groups = []
    for i in range(0, 100):
        placement_group = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
        placement_groups.append(placement_group)

    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    for i in range(0, 100):
        ray.get(placement_groups[i].ready())


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            num_heartbeats_timeout=10, gcs_rpc_server_reconnect_timeout_s=60
        )
    ],
    indirect=True,
)
def test_placement_group_wait_api(ray_start_cluster_head_with_external_redis):
    cluster = ray_start_cluster_head_with_external_redis
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Create placement group 1 successfully.
    placement_group1 = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    assert placement_group1.wait(10)

    # Restart gcs server.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    # Create placement group 2 successfully.
    placement_group2 = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    assert placement_group2.wait(10)

    # Remove placement group 1.
    ray.util.remove_placement_group(placement_group1)

    # Wait for placement group 1 after it is removed.
    with pytest.raises(Exception):
        placement_group1.wait(10)


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_schedule_placement_groups_at_the_same_time(connect_to_client):
    ray.init(num_cpus=4)

    with connect_to_client_or_not(connect_to_client):
        pgs = [placement_group([{"CPU": 2}]) for _ in range(6)]

        wait_pgs = {pg.ready(): pg for pg in pgs}

        def is_all_placement_group_removed():
            ready, _ = ray.wait(list(wait_pgs.keys()), timeout=0.5)
            if ready:
                ready_pg = wait_pgs[ready[0]]
                remove_placement_group(ready_pg)
                del wait_pgs[ready[0]]

            if len(wait_pgs) == 0:
                return True
            return False

        wait_for_condition(is_all_placement_group_removed)

    ray.shutdown()


def test_detached_placement_group(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    for _ in range(2):
        cluster.add_node(num_cpus=3)
    cluster.wait_for_nodes()
    info = ray.init(address=cluster.address)

    # Make sure detached placement group will alive when job dead.
    driver_code = f"""
import ray

ray.init(address="{info["address"]}")

pg = ray.util.placement_group(
        [{{"CPU": 1}} for _ in range(2)],
        strategy="STRICT_SPREAD", lifetime="detached")
ray.get(pg.ready())

@ray.remote(num_cpus=1)
class Actor:
    def ready(self):
        return True

for bundle_index in range(2):
    actor = Actor.options(lifetime="detached", placement_group=pg,
                placement_group_bundle_index=bundle_index).remote()
    ray.get(actor.ready.remote())

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

    def assert_alive_num_pg(expected_num_pg):
        alive_num_pg = 0
        for _, placement_group_info in ray.util.placement_group_table().items():
            if placement_group_info["state"] == "CREATED":
                alive_num_pg += 1
        return alive_num_pg == expected_num_pg

    def assert_alive_num_actor(expected_num_actor):
        alive_num_actor = 0
        for actor_info in ray.state.actors().values():
            if actor_info["State"] == convert_actor_state(
                gcs_utils.ActorTableData.ALIVE
            ):
                alive_num_actor += 1
        return alive_num_actor == expected_num_actor

    wait_for_condition(is_job_done)

    assert assert_alive_num_pg(1)
    assert assert_alive_num_actor(2)

    # Make sure detached placement group will alive when its creator which
    # is detached actor dead.
    # Test actors first.
    @ray.remote(num_cpus=1)
    class NestedActor:
        def ready(self):
            return True

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            self.actors = []

        def ready(self):
            return True

        def schedule_nested_actor_with_detached_pg(self):
            # Create placement group which is detached.
            pg = ray.util.placement_group(
                [{"CPU": 1} for _ in range(2)],
                strategy="STRICT_SPREAD",
                lifetime="detached",
                name="detached_pg",
            )
            ray.get(pg.ready())
            # Schedule nested actor with the placement group.
            for bundle_index in range(2):
                actor = NestedActor.options(
                    placement_group=pg,
                    placement_group_bundle_index=bundle_index,
                    lifetime="detached",
                ).remote()
                ray.get(actor.ready.remote())
                self.actors.append(actor)

    a = Actor.options(lifetime="detached").remote()
    ray.get(a.ready.remote())
    # 1 parent actor and 2 children actor.
    ray.get(a.schedule_nested_actor_with_detached_pg.remote())

    # Kill an actor and wait until it is killed.
    kill_actor_and_wait_for_failure(a)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.ready.remote())

    # We should have 2 alive pgs and 4 alive actors.
    assert assert_alive_num_pg(2)
    assert assert_alive_num_actor(4)


def test_named_placement_group(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    for _ in range(2):
        cluster.add_node(num_cpus=3)
    cluster.wait_for_nodes()
    info = ray.init(address=cluster.address, namespace="default_test_namespace")
    global_placement_group_name = "named_placement_group"

    # Create a detached placement group with name.
    driver_code = f"""
import ray

ray.init(address="{info["address"]}", namespace="default_test_namespace")

pg = ray.util.placement_group(
        [{{"CPU": 1}} for _ in range(2)],
        strategy="STRICT_SPREAD",
        name="{global_placement_group_name}",
        lifetime="detached")
ray.get(pg.ready())

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

    wait_for_condition(is_job_done)

    @ray.remote(num_cpus=1)
    class Actor:
        def ping(self):
            return "pong"

    # Get the named placement group and schedule a actor.
    placement_group = ray.util.get_placement_group(global_placement_group_name)
    assert placement_group is not None
    assert placement_group.wait(5)
    actor = Actor.options(
        placement_group=placement_group, placement_group_bundle_index=0
    ).remote()

    ray.get(actor.ping.remote())

    # Create another placement group and make sure its creation will failed.
    error_creation_count = 0
    try:
        ray.util.placement_group(
            [{"CPU": 1} for _ in range(2)],
            strategy="STRICT_SPREAD",
            name=global_placement_group_name,
        )
    except RaySystemError:
        error_creation_count += 1
    assert error_creation_count == 1

    # Remove a named placement group and make sure the second creation
    # will successful.
    ray.util.remove_placement_group(placement_group)
    same_name_pg = ray.util.placement_group(
        [{"CPU": 1} for _ in range(2)],
        strategy="STRICT_SPREAD",
        name=global_placement_group_name,
    )
    assert same_name_pg.wait(10)

    # Get a named placement group with a name that doesn't exist
    # and make sure it will raise ValueError correctly.
    error_count = 0
    try:
        ray.util.get_placement_group("inexistent_pg")
    except ValueError:
        error_count = error_count + 1
    assert error_count == 1


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_synchronous_registration(
    ray_start_cluster_enabled, connect_to_client
):
    cluster = ray_start_cluster_enabled
    # One node which only has one CPU.
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        # Create a placement group that has two bundles and `STRICT_PACK`
        # strategy so its registration will successful but scheduling failed.
        placement_group = ray.util.placement_group(
            name="name",
            strategy="STRICT_PACK",
            bundles=[
                {
                    "CPU": 1,
                },
                {"CPU": 1},
            ],
        )
        # Make sure we can properly remove it immediately
        # as its registration is synchronous.
        ray.util.remove_placement_group(placement_group)

        wait_for_condition(lambda: is_placement_group_removed(placement_group))


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_gpu_set(ray_start_cluster_enabled, connect_to_client):
    cluster = ray_start_cluster_enabled
    # One node which only has one CPU.
    cluster.add_node(num_cpus=1, num_gpus=1)
    cluster.add_node(num_cpus=1, num_gpus=1)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name",
            strategy="PACK",
            bundles=[{"CPU": 1, "GPU": 1}, {"CPU": 1, "GPU": 1}],
        )

        @ray.remote(num_gpus=1)
        def get_gpus():
            return ray.get_gpu_ids()

        result = get_gpus.options(
            placement_group=placement_group, placement_group_bundle_index=0
        ).remote()
        result = ray.get(result)
        assert result == [0]

        result = get_gpus.options(
            placement_group=placement_group, placement_group_bundle_index=1
        ).remote()
        result = ray.get(result)
        assert result == [0]


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_gpu_assigned(ray_start_cluster_enabled, connect_to_client):
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_gpus=2)
    ray.init(address=cluster.address)
    gpu_ids_res = set()

    @ray.remote(num_gpus=1, num_cpus=0)
    def f():
        import os

        return os.environ["CUDA_VISIBLE_DEVICES"]

    with connect_to_client_or_not(connect_to_client):
        pg1 = ray.util.placement_group([{"GPU": 1}])
        pg2 = ray.util.placement_group([{"GPU": 1}])

        assert pg1.wait(10)
        assert pg2.wait(10)

        gpu_ids_res.add(ray.get(f.options(placement_group=pg1).remote()))
        gpu_ids_res.add(ray.get(f.options(placement_group=pg2).remote()))

        assert len(gpu_ids_res) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
