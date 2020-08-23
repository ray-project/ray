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
from ray.experimental.placement_group import PlacementGroup


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

    placement_group = ray.experimental.placement_group(
        name="name", strategy="PACK", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))

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

    placement_group = ray.experimental.placement_group(
        name="name", strategy="STRICT_PACK", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))

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

    placement_group = ray.experimental.placement_group(
        name="name", strategy="SPREAD", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))

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

    placement_group = ray.experimental.placement_group(
        name="name",
        strategy="STRICT_SPREAD",
        bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()
    actor_3 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=2).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))
    print(ray.get(actor_3.value.remote()))

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

    g1 = ray.experimental.placement_group([{"CPU": 2}])
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

    g1 = ray.experimental.placement_group([{"CPU": 2}])
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

    g1 = ray.experimental.placement_group([{"CPU": 2}])
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
    random_placement_group = PlacementGroup(random_group_id, -1)
    for _ in range(3):
        ray.experimental.remove_placement_group(random_placement_group)

    # Creating a placement group as soon as it is
    # created should work.
    placement_group = ray.experimental.placement_group([{
        "CPU": 2
    }, {
        "CPU": 2
    }])
    ray.experimental.remove_placement_group(placement_group)

    def is_placement_group_removed():
        table = ray.experimental.placement_group_table(placement_group)
        if "state" not in table:
            return False
        return table["state"] == "REMOVED"

    wait_for_condition(is_placement_group_removed)

    # # Now let's create a placement group.
    placement_group = ray.experimental.placement_group([{
        "CPU": 2
    }, {
        "CPU": 2
    }])

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

    ray.experimental.remove_placement_group(placement_group)
    # Subsequent remove request shouldn't do anything.
    for _ in range(3):
        ray.experimental.remove_placement_group(placement_group)

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
    with pytest.raises(ray.exceptions.RayWorkerError):
        ray.get(task_ref)


def test_remove_pending_placement_group(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)
    # Create a placement group that cannot be scheduled now.
    placement_group = ray.experimental.placement_group([{
        "GPU": 2
    }, {
        "CPU": 2
    }])
    ray.experimental.remove_placement_group(placement_group)
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
    placement_group = ray.experimental.placement_group(
        name=name, strategy=strategy, bundles=bundles)
    result = ray.experimental.placement_group_table(placement_group)
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

    result = ray.experimental.placement_group_table(placement_group)
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

    g1 = ray.experimental.placement_group([{"CPU": 1, "GPU": 1}])
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

    placement_group = ray.experimental.placement_group(
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
        detached=True).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1,
        detached=True).remote()
    actor_3 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=2,
        detached=True).remote()
    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))
    print(ray.get(actor_3.value.remote()))

    cluster.remove_node(get_other_nodes(cluster, exclude_head=True)[-1])
    cluster.wait_for_nodes()

    actor_4 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0,
        detached=True).remote()
    actor_5 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1,
        detached=True).remote()
    actor_6 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=2,
        detached=True).remote()
    print(ray.get(actor_4.value.remote()))
    print(ray.get(actor_5.value.remote()))
    print(ray.get(actor_6.value.remote()))
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

    placement_group = ray.experimental.placement_group(
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


def test_schedule_placement_group_when_node_add(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    # Creating a placement group that cannot be satisfied yet.
    placement_group = ray.experimental.placement_group([{
        "GPU": 2
    }, {
        "CPU": 2
    }])

    def is_placement_group_created():
        table = ray.experimental.placement_group_table(placement_group)
        if "state" not in table:
            return False
        return table["state"] == "CREATED"

    # Add a node that has GPU.
    cluster.add_node(num_cpus=4, num_gpus=4)

    # Make sure the placement group is created.
    wait_for_condition(is_placement_group_created)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
