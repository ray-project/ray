import pytest
import os
import sys
import time

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
from ray.test_utils import (generate_system_config_map, get_other_nodes,
                            kill_actor_and_wait_for_failure,
                            run_string_as_driver, wait_for_condition,
                            get_error_message)
import ray.cluster_utils
from ray.exceptions import RaySystemError
from ray._raylet import PlacementGroupID
from ray.util.placement_group import (PlacementGroup, placement_group,
                                      remove_placement_group,
                                      get_current_placement_group)


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


def test_placement_group_pack(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="PACK",
        bundles=[
            {
                "CPU": 2,
                "GPU": 0  # Test 0 resource spec doesn't break tests.
            },
            {
                "CPU": 2
            }
        ])
    ray.get(placement_group.ready())
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    ray.get(actor_1.value.remote())
    ray.get(actor_2.value.remote())

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 == node_of_actor_2


def test_placement_group_strict_pack(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name", strategy="STRICT_PACK", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    ray.get(placement_group.ready())
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    ray.get(actor_1.value.remote())
    ray.get(actor_2.value.remote())

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 == node_of_actor_2


def test_placement_group_spread(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name", strategy="SPREAD", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    ray.get(placement_group.ready())
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    ray.get(actor_1.value.remote())
    ray.get(actor_2.value.remote())

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are located in separate nodes.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 != node_of_actor_2


def test_placement_group_strict_spread(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_SPREAD",
        bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }, {
            "CPU": 2
        }])
    ray.get(placement_group.ready())
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()
    actor_3 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=2).remote()

    ray.get(actor_1.value.remote())
    ray.get(actor_2.value.remote())
    ray.get(actor_3.value.remote())

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are located in separate nodes.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())
    actor_info_3 = actor_infos.get(actor_3._actor_id.hex())

    assert actor_info_1 and actor_info_2 and actor_info_3

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    node_of_actor_3 = actor_info_3["Address"]["NodeID"]
    assert node_of_actor_1 != node_of_actor_2
    assert node_of_actor_1 != node_of_actor_3
    assert node_of_actor_2 != node_of_actor_3


def test_placement_group_actor_resource_ids(ray_start_cluster):
    @ray.remote(num_cpus=1)
    class F:
        def f(self):
            return ray.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    g1 = ray.util.placement_group([{"CPU": 2}])
    a1 = F.options(placement_group=g1).remote()
    resources = ray.get(a1.f.remote())
    assert len(resources) == 1, resources
    assert "CPU_group_" in list(resources.keys())[0], resources


def test_placement_group_task_resource_ids(ray_start_cluster):
    @ray.remote(num_cpus=1)
    def f():
        return ray.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    g1 = ray.util.placement_group([{"CPU": 2}])
    o1 = f.options(placement_group=g1).remote()
    resources = ray.get(o1)
    assert len(resources) == 1, resources
    assert "CPU_group_" in list(resources.keys())[0], resources
    assert "CPU_group_0_" not in list(resources.keys())[0], resources

    # Now retry with a bundle index constraint.
    o1 = f.options(placement_group=g1, placement_group_bundle_index=0).remote()
    resources = ray.get(o1)
    assert len(resources) == 2, resources
    keys = list(resources.keys())
    assert "CPU_group_" in keys[0], resources
    assert "CPU_group_" in keys[1], resources
    assert "CPU_group_0_" in keys[0] or "CPU_group_0_" in keys[1], resources


def test_placement_group_hang(ray_start_cluster):
    @ray.remote(num_cpus=1)
    def f():
        return ray.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    # Warm workers up, so that this triggers the hang rice.
    ray.get(f.remote())

    g1 = ray.util.placement_group([{"CPU": 2}])
    # This will start out infeasible. The placement group will then be created
    # and it transitions to feasible.
    o1 = f.options(placement_group=g1).remote()

    resources = ray.get(o1)
    assert len(resources) == 1, resources
    assert "CPU_group_" in list(resources.keys())[0], resources


def test_remove_placement_group(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)
    # First try to remove a placement group that doesn't
    # exist. This should not do anything.
    random_group_id = PlacementGroupID.from_random()
    random_placement_group = PlacementGroup(random_group_id)
    for _ in range(3):
        ray.util.remove_placement_group(random_placement_group)

    # Creating a placement group as soon as it is
    # created should work.
    placement_group = ray.util.placement_group([{"CPU": 2}, {"CPU": 2}])
    assert placement_group.wait(10)
    ray.util.remove_placement_group(placement_group)

    def is_placement_group_removed():
        table = ray.util.placement_group_table(placement_group)
        if "state" not in table:
            return False
        return table["state"] == "REMOVED"

    wait_for_condition(is_placement_group_removed)

    # # Now let's create a placement group.
    placement_group = ray.util.placement_group([{"CPU": 2}, {"CPU": 2}])
    assert placement_group.wait(10)

    # Create an actor that occupies resources.
    @ray.remote(num_cpus=2)
    class A:
        def f(self):
            return 3

    # Currently, there's no way to prevent
    # tasks to be retried for removed placement group.
    # Set max_retrie=0 for testing.
    # TODO(sang): Handle this edge case.
    @ray.remote(num_cpus=2, max_retries=0)
    def long_running_task():
        print(os.getpid())
        import time
        time.sleep(50)

    # Schedule a long running task and actor.
    task_ref = long_running_task.options(
        placement_group=placement_group).remote()
    a = A.options(placement_group=placement_group).remote()
    assert ray.get(a.f.remote()) == 3

    ray.util.remove_placement_group(placement_group)
    # Subsequent remove request shouldn't do anything.
    for _ in range(3):
        ray.util.remove_placement_group(placement_group)

    # Make sure placement group resources are
    # released and we can schedule this task.
    @ray.remote(num_cpus=4)
    def f():
        return 3

    assert ray.get(f.remote()) == 3
    # Since the placement group is removed,
    # the actor should've been killed.
    # That means this request should fail.
    with pytest.raises(ray.exceptions.RayActorError, match="actor died"):
        ray.get(a.f.remote(), timeout=3.0)
    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(task_ref)


def test_remove_pending_placement_group(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)
    # Create a placement group that cannot be scheduled now.
    placement_group = ray.util.placement_group([{"GPU": 2}, {"CPU": 2}])
    ray.util.remove_placement_group(placement_group)

    # TODO(sang): Add state check here.
    @ray.remote(num_cpus=4)
    def f():
        return 3

    # Make sure this task is still schedulable.
    assert ray.get(f.remote()) == 3


def test_placement_group_table(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    # Originally placement group creation should be pending because
    # there are no resources.
    name = "name"
    strategy = "PACK"
    bundles = [{"CPU": 2, "GPU": 1}, {"CPU": 2}]
    placement_group = ray.util.placement_group(
        name=name, strategy=strategy, bundles=bundles)
    result = ray.util.placement_group_table(placement_group)
    assert result["name"] == name
    assert result["strategy"] == strategy
    for i in range(len(bundles)):
        assert bundles[i] == result["bundles"][i]
    assert result["state"] == "PENDING"

    # Now the placement group should be scheduled.
    cluster.add_node(num_cpus=5, num_gpus=1)

    cluster.wait_for_nodes()
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    ray.get(actor_1.value.remote())

    result = ray.util.placement_group_table(placement_group)
    assert result["state"] == "CREATED"

    # Add tow more placement group for placement group table test.
    second_strategy = "SPREAD"
    ray.util.placement_group(
        name="second_placement_group",
        strategy=second_strategy,
        bundles=bundles)
    ray.util.placement_group(
        name="third_placement_group",
        strategy=second_strategy,
        bundles=bundles)

    placement_group_table = ray.util.placement_group_table()
    assert len(placement_group_table) == 3

    true_name_set = {"name", "second_placement_group", "third_placement_group"}
    get_name_set = set()

    for _, placement_group_data in placement_group_table.items():
        get_name_set.add(placement_group_data["name"])

    assert true_name_set == get_name_set


def test_cuda_visible_devices(ray_start_cluster):
    @ray.remote(num_gpus=1)
    def f():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_gpus=1)
    ray.init(address=cluster.address)

    g1 = ray.util.placement_group([{"CPU": 1, "GPU": 1}])
    o1 = f.options(placement_group=g1).remote()

    devices = ray.get(o1)
    assert devices == "0", devices


def test_placement_group_reschedule_when_node_dead(ray_start_cluster):
    @ray.remote(num_cpus=1)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Make sure both head and worker node are alive.
    nodes = ray.nodes()
    assert len(nodes) == 3
    assert nodes[0]["alive"] and nodes[1]["alive"] and nodes[2]["alive"]

    placement_group = ray.util.placement_group(
        name="name",
        strategy="SPREAD",
        bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0,
        lifetime="detached").remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1,
        lifetime="detached").remote()
    actor_3 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=2,
        lifetime="detached").remote()
    ray.get(actor_1.value.remote())
    ray.get(actor_2.value.remote())
    ray.get(actor_3.value.remote())

    cluster.remove_node(get_other_nodes(cluster, exclude_head=True)[-1])
    cluster.wait_for_nodes()

    actor_4 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0,
        lifetime="detached").remote()
    actor_5 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1,
        lifetime="detached").remote()
    actor_6 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=2,
        lifetime="detached").remote()
    ray.get(actor_4.value.remote())
    ray.get(actor_5.value.remote())
    ray.get(actor_6.value.remote())
    ray.shutdown()


def test_check_bundle_index(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name", strategy="SPREAD", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])

    error_count = 0
    try:
        Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=3).remote()
    except ValueError:
        error_count = error_count + 1
    assert error_count == 1

    try:
        Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=-2).remote()
    except ValueError:
        error_count = error_count + 1
    assert error_count == 2

    try:
        Actor.options(placement_group_bundle_index=0).remote()
    except ValueError:
        error_count = error_count + 1
    assert error_count == 3


def test_pending_placement_group_wait(ray_start_cluster):
    cluster = ray_start_cluster
    [cluster.add_node(num_cpus=2) for _ in range(1)]
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

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


def test_placement_group_wait(ray_start_cluster):
    cluster = ray_start_cluster
    [cluster.add_node(num_cpus=2) for _ in range(2)]
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

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


def test_schedule_placement_group_when_node_add(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

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


def test_atomic_creation(ray_start_cluster):
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
        placement_group_bundle_index=num_nodes * bundle_per_node - 1).remote()

    # Wait on the placement group now. It should be unready
    # because normal actor takes resources that are required
    # for one of bundle creation.
    ready, unready = ray.wait([pg.ready()], timeout=0.5)
    assert len(ready) == 0
    assert len(unready) == 1
    # Wait until all tasks are done.
    assert all(ray.get(tasks))

    # Wait on the placement group creation. Since resources are now available,
    # it should be ready soon.
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


def test_mini_integration(ray_start_cluster):
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

    @ray.remote(num_cpus=0, num_gpus=1)
    def random_tasks():
        import time
        import random
        sleep_time = random.uniform(0.1, 0.2)
        time.sleep(sleep_time)
        return True

    pgs = []
    pg_tasks = []
    # total bundle gpu usage = bundles_per_pg * total_num_pg * per_bundle_gpus
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


def test_capture_child_actors(ray_start_cluster):
    cluster = ray_start_cluster
    total_num_actors = 4
    for _ in range(2):
        cluster.add_node(num_cpus=total_num_actors)
    ray.init(address=cluster.address)

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

    a = Actor.options(placement_group=pg).remote()
    ray.get(a.ready.remote())
    # 1 top level actor + 3 children.
    for _ in range(total_num_actors - 1):
        ray.get(a.schedule_nested_actor.remote())
    # Make sure all the actors are scheduled on the same node.
    # (why? The placement group has STRICT_PACK strategy).
    node_id_set = set()
    for actor_info in ray.actors().values():
        node_id = actor_info["Address"]["NodeID"]
        node_id_set.add(node_id)

    # Since all node id should be identical, set should be equal to 1.
    assert len(node_id_set) == 1

    # Kill an actor and wait until it is killed.
    kill_actor_and_wait_for_failure(a)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.ready.remote())

    # Now create an actor, but do not capture the current tasks
    a = Actor.options(
        placement_group=pg,
        placement_group_capture_child_tasks=False).remote()
    ray.get(a.ready.remote())
    # 1 top level actor + 3 children.
    for _ in range(total_num_actors - 1):
        ray.get(a.schedule_nested_actor.remote())
    # Make sure all the actors are not scheduled on the same node.
    # It is because the child tasks are not scheduled on the same
    # placement group.
    node_id_set = set()
    for actor_info in ray.actors().values():
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
    for actor_info in ray.actors().values():
        node_id = actor_info["Address"]["NodeID"]
        node_id_set.add(node_id)

    assert len(node_id_set) == 2


def test_capture_child_tasks(ray_start_cluster):
    cluster = ray_start_cluster
    total_num_tasks = 4
    for _ in range(2):
        cluster.add_node(num_cpus=total_num_tasks, num_gpus=total_num_tasks)
    ray.init(address=cluster.address)

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
    def create_nested_task(child_cpu, child_gpu):
        assert get_current_placement_group() is not None
        return ray.get([
            task.options(num_cpus=child_cpu, num_gpus=child_gpu).remote()
            for _ in range(3)
        ])

    t = create_nested_task.options(
        num_cpus=1, num_gpus=0, placement_group=pg).remote(1, 0)
    pgs = ray.get(t)
    # Every task should have current placement group because they
    # should be implicitly captured by default.
    assert None not in pgs

    # Test if tasks don't capture child tasks when the option is off.
    t2 = create_nested_task.options(
        num_cpus=0,
        num_gpus=1,
        placement_group=pg,
        placement_group_capture_child_tasks=False).remote(0, 1)
    pgs = ray.get(t2)
    # All placement group should be None because we don't capture child tasks.
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

ray.init(address="{info["redis_address"]}")

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
        jobs = ray.jobs()
        for job in jobs:
            if "StopTime" in job:
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

    info = ray.init(address=cluster.address)
    available_cpus = ray.available_resources()["CPU"]
    assert available_cpus == num_nodes * num_cpu_per_node

    driver_code = f"""
import ray

ray.init(address="{info["redis_address"]}")

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
        jobs = ray.jobs()
        for job in jobs:
            if "StopTime" in job:
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
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_create_placement_group_after_gcs_server_restart(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
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
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_create_actor_with_placement_group_after_gcs_server_restart(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
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
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_create_placement_group_during_gcs_server_restart(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
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
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_placement_group_wait_api(ray_start_cluster_head):
    cluster = ray_start_cluster_head
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


def test_schedule_placement_groups_at_the_same_time():
    ray.init(num_cpus=4)

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

ray.init(address="{info["redis_address"]}")

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
        jobs = ray.jobs()
        for job in jobs:
            if "StopTime" in job:
                return True
        return False

    def assert_alive_num_pg(expected_num_pg):
        alive_num_pg = 0
        for _, placement_group_info in ray.util.placement_group_table().items(
        ):
            if placement_group_info["state"] == "CREATED":
                alive_num_pg += 1
        return alive_num_pg == expected_num_pg

    def assert_alive_num_actor(expected_num_actor):
        alive_num_actor = 0
        for actor_info in ray.actors().values():
            if actor_info["State"] == ray.gcs_utils.ActorTableData.ALIVE:
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
                [{
                    "CPU": 1
                } for _ in range(2)],
                strategy="STRICT_SPREAD",
                lifetime="detached",
                name="detached_pg")
            ray.get(pg.ready())
            # Schedule nested actor with the placement group.
            for bundle_index in range(2):
                actor = NestedActor.options(
                    placement_group=pg,
                    placement_group_bundle_index=bundle_index,
                    lifetime="detached").remote()
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
    info = ray.init(address=cluster.address)
    global_placement_group_name = "named_placement_group"

    # Create a detached placement group with name.
    driver_code = f"""
import ray

ray.init(address="{info["redis_address"]}")

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
        jobs = ray.jobs()
        for job in jobs:
            if "StopTime" in job:
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
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()

    ray.get(actor.ping.remote())

    # Create another placement group and make sure its creation will failed.
    error_creation_count = 0
    try:
        ray.util.placement_group(
            [{
                "CPU": 1
            } for _ in range(2)],
            strategy="STRICT_SPREAD",
            name=global_placement_group_name)
    except RaySystemError:
        error_creation_count += 1
    assert error_creation_count == 1

    # Remove a named placement group and make sure the second creation
    # will successful.
    ray.util.remove_placement_group(placement_group)
    same_name_pg = ray.util.placement_group(
        [{
            "CPU": 1
        } for _ in range(2)],
        strategy="STRICT_SPREAD",
        name=global_placement_group_name)
    assert same_name_pg.wait(10)

    # Get a named placement group with a name that doesn't exist
    # and make sure it will raise ValueError correctly.
    error_count = 0
    try:
        ray.util.get_placement_group("inexistent_pg")
    except ValueError:
        error_count = error_count + 1
    assert error_count == 1


def test_placement_group_synchronous_registration(ray_start_cluster):
    cluster = ray_start_cluster
    # One node which only has one CPU.
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Create a placement group that has two bundles and `STRICT_PACK` strategy,
    # so, its registration will successful but scheduling failed.
    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_PACK",
        bundles=[{
            "CPU": 1,
        }, {
            "CPU": 1
        }])
    # Make sure we can properly remove it immediately
    # as its registration is synchronous.
    ray.util.remove_placement_group(placement_group)

    def is_placement_group_removed():
        table = ray.util.placement_group_table(placement_group)
        if "state" not in table:
            return False
        return table["state"] == "REMOVED"

    wait_for_condition(is_placement_group_removed)


def test_placement_group_gpu_set(ray_start_cluster):
    cluster = ray_start_cluster
    # One node which only has one CPU.
    cluster.add_node(num_cpus=1, num_gpus=1)
    cluster.add_node(num_cpus=1, num_gpus=1)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="PACK",
        bundles=[{
            "CPU": 1,
            "GPU": 1
        }, {
            "CPU": 1,
            "GPU": 1
        }])

    @ray.remote(num_gpus=1)
    def get_gpus():
        return ray.get_gpu_ids()

    result = get_gpus.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    result = ray.get(result)
    assert result == [0]

    result = get_gpus.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()
    result = ray.get(result)
    assert result == [0]


def test_placement_group_gpu_assigned(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_gpus=2)
    ray.init(address=cluster.address)
    gpu_ids_res = set()

    @ray.remote(num_gpus=1, num_cpus=0)
    def f():
        import os
        return os.environ["CUDA_VISIBLE_DEVICES"]

    pg1 = ray.util.placement_group([{"GPU": 1}])
    pg2 = ray.util.placement_group([{"GPU": 1}])

    assert pg1.wait(10)
    assert pg2.wait(10)

    gpu_ids_res.add(ray.get(f.options(placement_group=pg1).remote()))
    gpu_ids_res.add(ray.get(f.options(placement_group=pg2).remote()))

    assert len(gpu_ids_res) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
