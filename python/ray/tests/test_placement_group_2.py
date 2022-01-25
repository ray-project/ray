import pytest
import sys
import time

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils
import ray._private.gcs_utils as gcs_utils

from ray._private.test_utils import (
    get_other_nodes, generate_system_config_map,
    kill_actor_and_wait_for_failure, run_string_as_driver, wait_for_condition,
    get_error_message, placement_group_assert_no_leak, convert_actor_state)
from ray.util.placement_group import get_current_placement_group
from ray.util.client.ray_client_helpers import connect_to_client_or_not


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_check_bundle_index(ray_start_cluster, connect_to_client):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name", strategy="SPREAD", bundles=[{
                "CPU": 2
            }, {
                "CPU": 2
            }])

        with pytest.raises(ValueError, match="bundle index 3 is invalid"):
            Actor.options(
                placement_group=placement_group,
                placement_group_bundle_index=3).remote()

        with pytest.raises(ValueError, match="bundle index -2 is invalid"):
            Actor.options(
                placement_group=placement_group,
                placement_group_bundle_index=-2).remote()

        with pytest.raises(ValueError, match="bundle index must be -1"):
            Actor.options(placement_group_bundle_index=0).remote()

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_pending_placement_group_wait(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    [cluster.add_node(num_cpus=2) for _ in range(1)]
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    with connect_to_client_or_not(connect_to_client):
        # Wait on placement group that cannot be created.
        placement_group = ray.util.placement_group(
            name="name",
            strategy="SPREAD",
            bundles=[
                {
                    "CPU": 2
                },
                {
                    "CPU": 2
                },
                {
                    "GPU": 2
                },
            ])
        ready, unready = ray.wait([placement_group.ready()], timeout=0.1)
        assert len(unready) == 1
        assert len(ready) == 0
        table = ray.util.placement_group_table(placement_group)
        assert table["state"] == "PENDING"
        with pytest.raises(ray.exceptions.GetTimeoutError):
            ray.get(placement_group.ready(), timeout=0.1)


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_wait(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    [cluster.add_node(num_cpus=2) for _ in range(2)]
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    with connect_to_client_or_not(connect_to_client):
        # Wait on placement group that cannot be created.
        placement_group = ray.util.placement_group(
            name="name", strategy="SPREAD", bundles=[
                {
                    "CPU": 2
                },
                {
                    "CPU": 2
                },
            ])
        ready, unready = ray.wait([placement_group.ready()])
        assert len(unready) == 0
        assert len(ready) == 1
        table = ray.util.placement_group_table(placement_group)
        assert table["state"] == "CREATED"

        pg = ray.get(placement_group.ready())
        assert pg.bundle_specs == placement_group.bundle_specs
        assert pg.id.binary() == placement_group.id.binary()


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_schedule_placement_group_when_node_add(ray_start_cluster,
                                                connect_to_client):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        # Creating a placement group that cannot be satisfied yet.
        placement_group = ray.util.placement_group([{"GPU": 2}, {"CPU": 2}])

        def is_placement_group_created():
            table = ray.util.placement_group_table(placement_group)
            if "state" not in table:
                return False
            return table["state"] == "CREATED"

        # Add a node that has GPU.
        cluster.add_node(num_cpus=4, num_gpus=4)

        # Make sure the placement group is created.
        wait_for_condition(is_placement_group_created)


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_atomic_creation(ray_start_cluster, connect_to_client):
    # Setup cluster.
    cluster = ray_start_cluster
    bundle_cpu_size = 2
    bundle_per_node = 2
    num_nodes = 2

    [
        cluster.add_node(num_cpus=bundle_cpu_size * bundle_per_node)
        for _ in range(num_nodes)
    ]
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class NormalActor:
        def ping(self):
            pass

    @ray.remote(num_cpus=3)
    def bothering_task():
        time.sleep(6)
        return True

    with connect_to_client_or_not(connect_to_client):
        # Schedule tasks to fail initial placement group creation.
        tasks = [bothering_task.remote() for _ in range(2)]

        # Make sure the two common task has scheduled.
        def tasks_scheduled():
            return ray.available_resources()["CPU"] == 2.0

        wait_for_condition(tasks_scheduled)

        # Create an actor that will fail bundle scheduling.
        # It is important to use pack strategy to make test less flaky.
        pg = ray.util.placement_group(
            name="name",
            strategy="SPREAD",
            bundles=[{
                "CPU": bundle_cpu_size
            } for _ in range(num_nodes * bundle_per_node)])

        # Create a placement group actor.
        # This shouldn't be scheduled because atomic
        # placement group creation should've failed.
        pg_actor = NormalActor.options(
            placement_group=pg,
            placement_group_bundle_index=num_nodes * bundle_per_node -
            1).remote()

        # Wait on the placement group now. It should be unready
        # because normal actor takes resources that are required
        # for one of bundle creation.
        ready, unready = ray.wait([pg.ready()], timeout=0.5)
        assert len(ready) == 0
        assert len(unready) == 1
        # Wait until all tasks are done.
        assert all(ray.get(tasks))

        # Wait on the placement group creation. Since resources are now
        # available, it should be ready soon.
        ready, unready = ray.wait([pg.ready()])
        assert len(ready) == 1
        assert len(unready) == 0

        # Confirm that the placement group actor is created. It will
        # raise an exception if actor was scheduled before placement
        # group was created thus it checks atomicity.
        ray.get(pg_actor.ping.remote(), timeout=3.0)
        ray.kill(pg_actor)

        # Make sure atomic creation failure didn't impact resources.
        @ray.remote(num_cpus=bundle_cpu_size)
        def resource_check():
            return True

        # This should hang because every resources
        # are claimed by placement group.
        check_without_pg = [
            resource_check.remote() for _ in range(bundle_per_node * num_nodes)
        ]

        # This all should scheduled on each bundle.
        check_with_pg = [
            resource_check.options(
                placement_group=pg, placement_group_bundle_index=i).remote()
            for i in range(bundle_per_node * num_nodes)
        ]

        # Make sure these are hanging.
        ready, unready = ray.wait(check_without_pg, timeout=0)
        assert len(ready) == 0
        assert len(unready) == bundle_per_node * num_nodes

        # Make sure these are all scheduled.
        assert all(ray.get(check_with_pg))

        ray.util.remove_placement_group(pg)

        def pg_removed():
            return ray.util.placement_group_table(pg)["state"] == "REMOVED"

        wait_for_condition(pg_removed)

        # Make sure check without pgs are all
        # scheduled properly because resources are cleaned up.
        assert all(ray.get(check_without_pg))


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_mini_integration(ray_start_cluster, connect_to_client):
    # Create bundles as many as number of gpus in the cluster.
    # Do some random work and make sure all resources are properly recovered.

    cluster = ray_start_cluster

    num_nodes = 5
    per_bundle_gpus = 2
    gpu_per_node = 4
    total_gpus = num_nodes * per_bundle_gpus * gpu_per_node
    per_node_gpus = per_bundle_gpus * gpu_per_node

    bundles_per_pg = 2
    total_num_pg = total_gpus // (bundles_per_pg * per_bundle_gpus)

    [
        cluster.add_node(num_cpus=2, num_gpus=per_bundle_gpus * gpu_per_node)
        for _ in range(num_nodes)
    ]
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):

        @ray.remote(num_cpus=0, num_gpus=1)
        def random_tasks():
            import time
            import random
            sleep_time = random.uniform(0.1, 0.2)
            time.sleep(sleep_time)
            return True

        pgs = []
        pg_tasks = []
        # total bundle gpu usage = bundles_per_pg*total_num_pg*per_bundle_gpus
        # Note this is half of total
        for index in range(total_num_pg):
            pgs.append(
                ray.util.placement_group(
                    name=f"name{index}",
                    strategy="PACK",
                    bundles=[{
                        "GPU": per_bundle_gpus
                    } for _ in range(bundles_per_pg)]))

        # Schedule tasks.
        for i in range(total_num_pg):
            pg = pgs[i]
            pg_tasks.append([
                random_tasks.options(
                    placement_group=pg,
                    placement_group_bundle_index=bundle_index).remote()
                for bundle_index in range(bundles_per_pg)
            ])

        # Make sure tasks are done and we remove placement groups.
        num_removed_pg = 0
        pg_indexes = [2, 3, 1, 7, 8, 9, 0, 6, 4, 5]
        while num_removed_pg < total_num_pg:
            index = pg_indexes[num_removed_pg]
            pg = pgs[index]
            assert all(ray.get(pg_tasks[index]))
            ray.util.remove_placement_group(pg)
            num_removed_pg += 1

        @ray.remote(num_cpus=2, num_gpus=per_node_gpus)
        class A:
            def ping(self):
                return True

        # Make sure all resources are properly returned by scheduling
        # actors that take up all existing resources.
        actors = [A.remote() for _ in range(num_nodes)]
        assert all(ray.get([a.ping.remote() for a in actors]))


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_capture_child_actors(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    total_num_actors = 4
    for _ in range(2):
        cluster.add_node(num_cpus=total_num_actors)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        pg = ray.util.placement_group(
            [{
                "CPU": 2
            }, {
                "CPU": 2
            }], strategy="STRICT_PACK")
        ray.get(pg.ready())

        # If get_current_placement_group is used when the current worker/driver
        # doesn't belong to any of placement group, it should return None.
        assert get_current_placement_group() is None

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

            def schedule_nested_actor(self):
                # Make sure we can capture the current placement group.
                assert get_current_placement_group() is not None
                # Actors should be implicitly captured.
                actor = NestedActor.remote()
                ray.get(actor.ready.remote())
                self.actors.append(actor)

            def schedule_nested_actor_outside_pg(self):
                # Don't use placement group.
                actor = NestedActor.options(placement_group=None).remote()
                ray.get(actor.ready.remote())
                self.actors.append(actor)

        a = Actor.options(
            placement_group=pg,
            placement_group_capture_child_tasks=True).remote()
        ray.get(a.ready.remote())
        # 1 top level actor + 3 children.
        for _ in range(total_num_actors - 1):
            ray.get(a.schedule_nested_actor.remote())
        # Make sure all the actors are scheduled on the same node.
        # (why? The placement group has STRICT_PACK strategy).
        node_id_set = set()
        for actor_info in ray.state.actors().values():
            if actor_info["State"] == convert_actor_state(
                    gcs_utils.ActorTableData.ALIVE):
                node_id = actor_info["Address"]["NodeID"]
                node_id_set.add(node_id)

        # Since all node id should be identical, set should be equal to 1.
        assert len(node_id_set) == 1

        # Kill an actor and wait until it is killed.
        kill_actor_and_wait_for_failure(a)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(a.ready.remote())

        # Now create an actor, but do not capture the current tasks
        a = Actor.options(placement_group=pg).remote()
        ray.get(a.ready.remote())
        # 1 top level actor + 3 children.
        for _ in range(total_num_actors - 1):
            ray.get(a.schedule_nested_actor.remote())
        # Make sure all the actors are not scheduled on the same node.
        # It is because the child tasks are not scheduled on the same
        # placement group.
        node_id_set = set()
        for actor_info in ray.state.actors().values():
            if actor_info["State"] == convert_actor_state(
                    gcs_utils.ActorTableData.ALIVE):
                node_id = actor_info["Address"]["NodeID"]
                node_id_set.add(node_id)

        assert len(node_id_set) == 2

        # Kill an actor and wait until it is killed.
        kill_actor_and_wait_for_failure(a)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(a.ready.remote())

        # Lastly, make sure when None is specified, actors are not scheduled
        # on the same placement group.
        a = Actor.options(placement_group=pg).remote()
        ray.get(a.ready.remote())
        # 1 top level actor + 3 children.
        for _ in range(total_num_actors - 1):
            ray.get(a.schedule_nested_actor_outside_pg.remote())
        # Make sure all the actors are not scheduled on the same node.
        # It is because the child tasks are not scheduled on the same
        # placement group.
        node_id_set = set()
        for actor_info in ray.state.actors().values():
            if actor_info["State"] == convert_actor_state(
                    gcs_utils.ActorTableData.ALIVE):
                node_id = actor_info["Address"]["NodeID"]
                node_id_set.add(node_id)

        assert len(node_id_set) == 2


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_capture_child_tasks(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    total_num_tasks = 4
    for _ in range(2):
        cluster.add_node(num_cpus=total_num_tasks, num_gpus=total_num_tasks)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        pg = ray.util.placement_group(
            [{
                "CPU": 2,
                "GPU": 2,
            }, {
                "CPU": 2,
                "GPU": 2,
            }],
            strategy="STRICT_PACK")
        ray.get(pg.ready())

        # If get_current_placement_group is used when the current worker/driver
        # doesn't belong to any of placement group, it should return None.
        assert get_current_placement_group() is None

        # Test if tasks capture child tasks.
        @ray.remote
        def task():
            return get_current_placement_group()

        @ray.remote
        def create_nested_task(child_cpu, child_gpu, set_none=False):
            assert get_current_placement_group() is not None
            kwargs = {
                "num_cpus": child_cpu,
                "num_gpus": child_gpu,
            }
            if set_none:
                kwargs["placement_group"] = None
            return ray.get([task.options(**kwargs).remote() for _ in range(3)])

        t = create_nested_task.options(
            num_cpus=1,
            num_gpus=0,
            placement_group=pg,
            placement_group_capture_child_tasks=True).remote(1, 0)
        pgs = ray.get(t)
        # Every task should have current placement group because they
        # should be implicitly captured by default.
        assert None not in pgs

        t1 = create_nested_task.options(
            num_cpus=1,
            num_gpus=0,
            placement_group=pg,
            placement_group_capture_child_tasks=True).remote(1, 0, True)
        pgs = ray.get(t1)
        # Every task should have no placement group since it's set to None.
        # should be implicitly captured by default.
        assert set(pgs) == {None}

        # Test if tasks don't capture child tasks when the option is off.
        t2 = create_nested_task.options(
            num_cpus=0, num_gpus=1, placement_group=pg).remote(0, 1)
        pgs = ray.get(t2)
        # All placement groups should be None since we don't capture child
        # tasks.
        assert not all(pgs)


def test_ready_warning_suppressed(ray_start_regular, error_pubsub):
    p = error_pubsub
    # Create an infeasible pg.
    pg = ray.util.placement_group([{"CPU": 2}] * 2, strategy="STRICT_PACK")
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg.ready(), timeout=0.5)

    errors = get_error_message(
        p, 1, ray.ray_constants.INFEASIBLE_TASK_ERROR, timeout=0.1)
    assert len(errors) == 0


def test_automatic_cleanup_job(ray_start_cluster):
    # Make sure the placement groups created by a
    # job, actor, and task are cleaned when the job is done.
    cluster = ray_start_cluster
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


def test_automatic_cleanup_detached_actors(ray_start_cluster):
    # Make sure the placement groups created by a
    # detached actors are cleaned properly.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpu_per_node = 2
    # Create 3 nodes cluster.
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpu_per_node)
    cluster.wait_for_nodes()

    info = ray.init(
        address=cluster.address, namespace="default_test_namespace")
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
    "ray_start_cluster_head_with_external_redis", [
        generate_system_config_map(
            num_heartbeats_timeout=10, gcs_rpc_server_reconnect_timeout_s=60)
    ],
    indirect=True)
def test_create_placement_group_after_gcs_server_restart(
        ray_start_cluster_head_with_external_redis):
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
    "ray_start_cluster_head_with_external_redis", [
        generate_system_config_map(
            num_heartbeats_timeout=10, gcs_rpc_server_reconnect_timeout_s=60)
    ],
    indirect=True)
def test_create_actor_with_placement_group_after_gcs_server_restart(
        ray_start_cluster_head_with_external_redis):
    cluster = ray_start_cluster_head_with_external_redis
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Create a placement group.
    placement_group = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])

    # Create an actor that occupies resources after gcs server restart.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()
    actor_2 = Increase.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()
    assert ray.get(actor_2.method.remote(1)) == 3


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis", [
        generate_system_config_map(
            num_heartbeats_timeout=10, gcs_rpc_server_reconnect_timeout_s=60)
    ],
    indirect=True)
def test_bundle_recreated_when_raylet_fo_after_gcs_server_restart(
        ray_start_cluster_head_with_external_redis):
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
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()

    assert ray.get(actor.method.remote(1), timeout=5) == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
