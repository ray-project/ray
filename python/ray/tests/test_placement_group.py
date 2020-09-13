import pytest
import os
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
from ray.test_utils import get_other_nodes, wait_for_condition
import ray.cluster_utils
from ray._raylet import PlacementGroupID
from ray.util.placement_group import PlacementGroup


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
        name="name", strategy="PACK", bundles=[{
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
    random_placement_group = PlacementGroup(random_group_id, [{"CPU": 1}])
    for _ in range(3):
        ray.util.remove_placement_group(random_placement_group)

    # Creating a placement group as soon as it is
    # created should work.
    placement_group = ray.util.placement_group([{"CPU": 2}, {"CPU": 2}])
    ray.util.remove_placement_group(placement_group)

    def is_placement_group_removed():
        table = ray.util.placement_group_table(placement_group)
        if "state" not in table:
            return False
        return table["state"] == "REMOVED"

    wait_for_condition(is_placement_group_removed)

    # # Now let's create a placement group.
    placement_group = ray.util.placement_group([{"CPU": 2}, {"CPU": 2}])

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
    assert pg.bundles == placement_group.bundles
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
        import time
        time.sleep(1)
        return True

    # Schedule tasks to fail initial placement group creation.
    tasks = [bothering_task.remote() for _ in range(2)]
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
    ready, unready = ray.wait([pg.ready()], timeout=0)
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
    for _ in range(total_num_pg):
        pgs.append(
            ray.util.placement_group(
                name="name",
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
