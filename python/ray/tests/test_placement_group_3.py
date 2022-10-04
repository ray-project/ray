import sys
import time

import pytest

import ray
import ray._private.gcs_utils as gcs_utils
import ray.cluster_utils
import ray.experimental.internal_kv as internal_kv
from ray._private.ray_constants import DEBUG_AUTOSCALING_ERROR, DEBUG_AUTOSCALING_STATUS
from ray._private.test_utils import (
    convert_actor_state,
    generate_system_config_map,
    is_placement_group_removed,
    kill_actor_and_wait_for_failure,
    run_string_as_driver,
    wait_for_condition,
)
from ray.autoscaler._private.commands import debug_status
from ray.exceptions import RaySystemError
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None


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


def test_placement_group_wait_api_timeout(shutdown_only):
    """Make sure the wait API timeout works

    https://github.com/ray-project/ray/issues/27287
    """
    ray.init(num_cpus=1)
    pg = ray.util.placement_group(bundles=[{"CPU": 2}])
    start = time.time()
    assert not pg.wait(5)
    assert 5 <= time.time() - start


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


def test_detached_placement_group(ray_start_cluster):
    cluster = ray_start_cluster
    for _ in range(2):
        cluster.add_node(num_cpus=3)
    cluster.wait_for_nodes()
    info = ray.init(address=cluster.address)

    # Make sure detached placement group will alive when job dead.
    driver_code = f"""
import ray
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

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
    actor = Actor.options(lifetime="detached",
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg,
                placement_group_bundle_index=bundle_index)).remote()
    ray.get(actor.ready.remote())

ray.shutdown()
    """

    run_string_as_driver(driver_code)

    # Wait until the driver is reported as dead by GCS.
    def is_job_done():
        jobs = ray._private.state.jobs()
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
        for actor_info in ray._private.state.actors().values():
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
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg, placement_group_bundle_index=bundle_index
                    ),
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


def test_named_placement_group(ray_start_cluster):
    cluster = ray_start_cluster
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
        jobs = ray._private.state.jobs()
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
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=placement_group, placement_group_bundle_index=0
        )
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
def test_placement_group_synchronous_registration(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
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
def test_placement_group_gpu_set(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
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
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=placement_group, placement_group_bundle_index=0
            )
        ).remote()
        result = ray.get(result)
        assert result == [0]

        result = get_gpus.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=placement_group, placement_group_bundle_index=1
            )
        ).remote()
        result = ray.get(result)
        assert result == [0]


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_gpu_assigned(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
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

        gpu_ids_res.add(
            ray.get(
                f.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg1
                    )
                ).remote()
            )
        )
        gpu_ids_res.add(
            ray.get(
                f.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg2
                    )
                ).remote()
            )
        )

        assert len(gpu_ids_res) == 2


def test_actor_scheduling_not_block_with_placement_group(ray_start_cluster):
    """Tests the scheduling of lots of actors will not be blocked
    when using placement groups.

    For more detailed information please refer to:
    https://github.com/ray-project/ray/issues/15801.
    """

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class A:
        def ready(self):
            pass

    actor_num = 1000
    pgs = [ray.util.placement_group([{"CPU": 1}]) for _ in range(actor_num)]
    actors = [
        A.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
        for pg in pgs
    ]
    refs = [actor.ready.remote() for actor in actors]

    expected_created_num = 1

    def is_actor_created_number_correct():
        ready, not_ready = ray.wait(refs, num_returns=len(refs), timeout=1)
        return len(ready) == expected_created_num

    def is_pg_created_number_correct():
        created_pgs = [
            pg
            for _, pg in ray.util.placement_group_table().items()
            if pg["state"] == "CREATED"
        ]
        return len(created_pgs) == expected_created_num

    wait_for_condition(is_pg_created_number_correct, timeout=3)
    wait_for_condition(is_actor_created_number_correct, timeout=30, retry_interval_ms=0)

    # NOTE: we don't need to test all the actors create successfully.
    for _ in range(20):
        expected_created_num += 1
        cluster.add_node(num_cpus=1)

        wait_for_condition(is_pg_created_number_correct, timeout=10)
        # Make sure the node add event will cause a waiting actor
        # to create successfully in time.
        wait_for_condition(
            is_actor_created_number_correct, timeout=30, retry_interval_ms=0
        )


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_gpu_unique_assigned(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    cluster.add_node(num_gpus=4, num_cpus=4)
    ray.init(address=cluster.address)
    gpu_ids_res = set()

    # Create placement group with 4 bundles using 1 GPU each.
    num_gpus = 4
    bundles = [{"GPU": 1, "CPU": 1} for _ in range(num_gpus)]
    pg = placement_group(bundles)
    ray.get(pg.ready())

    # Actor using 1 GPU that has a method to get
    #  $CUDA_VISIBLE_DEVICES env variable.
    @ray.remote(num_gpus=1, num_cpus=1)
    class Actor:
        def get_gpu(self):
            import os

            return os.environ["CUDA_VISIBLE_DEVICES"]

    # Create actors out of order.
    actors = []
    actors.append(
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=0
            )
        ).remote()
    )
    actors.append(
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=3
            )
        ).remote()
    )
    actors.append(
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=2
            )
        ).remote()
    )
    actors.append(
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=1
            )
        ).remote()
    )

    for actor in actors:
        gpu_ids = ray.get(actor.get_gpu.remote())
        assert len(gpu_ids) == 1
        gpu_ids_res.add(gpu_ids)

    assert len(gpu_ids_res) == 4


def test_placement_group_status_no_bundle_demand(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        pass

    pg = ray.util.placement_group([{"CPU": 1}])
    ray.get(pg.ready())
    ray.util.remove_placement_group(pg)
    wait_for_condition(lambda: is_placement_group_removed(pg))
    # Create a ready task after the placement group is removed.
    # This shouldn't be reported to the resource demand.
    r = pg.ready()  # noqa

    # Wait until the usage is updated, which is
    # when the demand is also updated.
    def is_usage_updated():
        demand_output = get_ray_status_output(cluster.address)
        return demand_output["usage"] != ""

    wait_for_condition(is_usage_updated)
    # The output shouldn't include the pg.ready task demand.
    demand_output = get_ray_status_output(cluster.address)
    assert demand_output["demand"] == "(no resource demands)"


def test_placement_group_status(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class A:
        def ready(self):
            pass

    pg = ray.util.placement_group([{"CPU": 1}])
    ray.get(pg.ready())

    # Wait until the usage is updated, which is
    # when the demand is also updated.
    def is_usage_updated():
        demand_output = get_ray_status_output(cluster.address)
        return demand_output["usage"] != ""

    wait_for_condition(is_usage_updated)
    demand_output = get_ray_status_output(cluster.address)
    cpu_usage = demand_output["usage"].split("\n")[0]
    expected = "0.0/4.0 CPU (0.0 used of 1.0 reserved in placement groups)"
    assert cpu_usage == expected

    # 2 CPU + 1 PG CPU == 3.0/4.0 CPU (1 used by pg)
    actors = [A.remote() for _ in range(2)]
    actors_in_pg = [
        A.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
        for _ in range(1)
    ]

    ray.get([actor.ready.remote() for actor in actors])
    ray.get([actor.ready.remote() for actor in actors_in_pg])
    # Wait long enough until the usage is propagated to GCS.
    time.sleep(5)
    demand_output = get_ray_status_output(cluster.address)
    cpu_usage = demand_output["usage"].split("\n")[0]
    expected = "3.0/4.0 CPU (1.0 used of 1.0 reserved in placement groups)"
    assert cpu_usage == expected


def test_placement_group_removal_leak_regression(ray_start_cluster):
    """Related issue:
    https://github.com/ray-project/ray/issues/19131
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=5)
    ray.init(address=cluster.address)

    TOTAL_CPUS = 8
    bundles = [{"CPU": 1, "GPU": 1}]
    bundles += [{"CPU": 1} for _ in range(TOTAL_CPUS - 1)]

    pg = placement_group(bundles, strategy="PACK")
    # Here, we simulate that the ready task is queued and
    # the new node is up. As soon as the new node is up,
    # the ready task is scheduled.
    # See https://github.com/ray-project/ray/pull/19138
    # for more details about the test.
    o = pg.ready()
    # Add an artificial delay until the new node is up.
    time.sleep(3)
    cluster.add_node(num_cpus=5, num_gpus=1)
    ray.get(o)
    bundle_resource_name = f"bundle_group_{pg.id.hex()}"
    expected_bundle_wildcard_val = TOTAL_CPUS * 1000

    # This should fail if there's a leakage
    # because the bundle resources are never returned properly.
    def check_bundle_leaks():
        bundle_resources = ray.available_resources()[bundle_resource_name]
        return expected_bundle_wildcard_val == bundle_resources

    wait_for_condition(check_bundle_leaks)


def test_placement_group_local_resource_view(monkeypatch, ray_start_cluster):
    """Please refer to https://github.com/ray-project/ray/pull/19911
    for more details.
    """
    with monkeypatch.context() as m:
        # Increase broadcasting interval so that node resource will arrive
        # at raylet after local resource all being allocated.
        m.setenv("RAY_raylet_report_resources_period_milliseconds", "2000")
        m.setenv("RAY_grpc_based_resource_broadcast", "true")
        cluster = ray_start_cluster

        cluster.add_node(num_cpus=16, object_store_memory=1e9)
        cluster.wait_for_nodes()
        # We need to init here so that we can make sure it's connecting to
        # the raylet where it only has cpu resources.
        # This is a hacky way to prevent scheduling hanging which will
        # schedule <CPU:1> job to the node with GPU and for <GPU:1, CPU:1> task
        # there is no node has this resource.
        ray.init(address="auto")
        cluster.add_node(num_cpus=16, num_gpus=1)
        cluster.wait_for_nodes()
        NUM_CPU_BUNDLES = 30

        @ray.remote(num_cpus=1)
        class Worker(object):
            def __init__(self, i):
                self.i = i

            def work(self):
                time.sleep(0.1)
                print("work ", self.i)

        @ray.remote(num_cpus=1, num_gpus=1)
        class Trainer(object):
            def __init__(self, i):
                self.i = i

            def train(self):
                time.sleep(0.2)
                print("train ", self.i)

        bundles = [{"CPU": 1, "GPU": 1}]
        bundles += [{"CPU": 1} for _ in range(NUM_CPU_BUNDLES)]
        pg = placement_group(bundles, strategy="PACK")
        ray.get(pg.ready())

        # Local resource will be allocated and here we are to ensure
        # local view is consistent and node resouce updates are discarded
        workers = [
            Worker.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
            ).remote(i)
            for i in range(NUM_CPU_BUNDLES)
        ]
        trainer = Trainer.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote(0)
        ray.get([workers[i].work.remote() for i in range(NUM_CPU_BUNDLES)])
        ray.get(trainer.train.remote())


def test_fractional_resources_handle_correct(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1000)
    ray.init(address=cluster.address)

    bundles = [{"CPU": 0.01} for _ in range(5)]
    pg = placement_group(bundles, strategy="SPREAD")

    ray.get(pg.ready(), timeout=10)


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
