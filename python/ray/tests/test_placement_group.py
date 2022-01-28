import pytest
import os
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils
from ray._private.test_utils import (get_other_nodes, wait_for_condition,
                                     is_placement_group_removed,
                                     placement_group_assert_no_leak)
from ray._raylet import PlacementGroupID
from ray.util.placement_group import PlacementGroup
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

MOCK_WORKER_STARTUP_SLOWLY_PLUGIN_CLASS_PATH = "ray.tests.test_placement_group.MockWorkerStartupSlowlyPlugin"  # noqa


class MockWorkerStartupSlowlyPlugin(RuntimeEnvPlugin):
    def validate(runtime_env_dict: dict) -> str:
        return "success"

    @staticmethod
    def create(uri: str, runtime_env_dict: dict,
               ctx: RuntimeEnvContext) -> float:
        import time
        time.sleep(15)
        return 0


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_placement_ready(ray_start_regular, connect_to_client):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def v(self):
            return 10

    # bundle is placement group reserved resources and can't be used in bundles
    with pytest.raises(Exception):
        ray.util.placement_group(bundles=[{"bundle": 1}])
    # This test is to test the case that even there all resource in the
    # bundle got allocated, we are still able to return from ready[I
    # since ready use 0 CPU
    with connect_to_client_or_not(connect_to_client):
        pg = ray.util.placement_group(bundles=[{"CPU": 1}])
        ray.get(pg.ready())
        a = Actor.options(num_cpus=1, placement_group=pg).remote()
        ray.get(a.v.remote())
        ray.get(pg.ready())

        placement_group_assert_no_leak([pg])


def test_placement_group_invalid_resource_request(shutdown_only):
    """
    Make sure exceptions are raised if
    requested resources don't fit any bundles.
    """
    ray.init(resources={"a": 1})
    pg = ray.util.placement_group(bundles=[{"a": 1}])

    #
    # Test an actor with 0 cpu.
    #
    @ray.remote
    class A:
        def ready(self):
            pass

    # The actor cannot be scheduled with the default because
    # it requires 1 cpu for the placement, but the pg doesn't have it.
    with pytest.raises(ValueError):
        a = A.options(placement_group=pg).remote()
    # Shouldn't work with 1 CPU because pg doesn't contain CPUs.
    with pytest.raises(ValueError):
        a = A.options(num_cpus=1, placement_group=pg).remote()
    # 0 CPU should work.
    a = A.options(num_cpus=0, placement_group=pg).remote()
    ray.get(a.ready.remote())
    del a

    #
    # Test an actor with non-0 resources.
    #
    @ray.remote(resources={"a": 1})
    class B:
        def ready(self):
            pass

    # When resources are given to the placement group,
    # it automatically adds 1 CPU to resources, so it should fail.
    with pytest.raises(ValueError):
        b = B.options(placement_group=pg).remote()
    # If 0 cpu is given, it should work.
    b = B.options(num_cpus=0, placement_group=pg).remote()
    ray.get(b.ready.remote())
    del b
    # If resources are requested too much, it shouldn't work.
    with pytest.raises(ValueError):
        # The actor cannot be scheduled with no resource specified.
        # Note that the default actor has 0 cpu.
        B.options(num_cpus=0, resources={"a": 2}, placement_group=pg).remote()

    #
    # Test a function with 1 CPU.
    #
    @ray.remote
    def f():
        pass

    # 1 CPU shouldn't work because the pg doesn't have CPU bundles.
    with pytest.raises(ValueError):
        f.options(placement_group=pg).remote()
    # 0 CPU should work.
    ray.get(f.options(placement_group=pg, num_cpus=0).remote())

    #
    # Test a function with 0 CPU.
    #
    @ray.remote(num_cpus=0)
    def g():
        pass

    # 0 CPU should work.
    ray.get(g.options(placement_group=pg).remote())

    placement_group_assert_no_leak([pg])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_pack(ray_start_cluster, connect_to_client):
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

    with connect_to_client_or_not(connect_to_client):
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
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are collocated in one node.
        actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
        actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

        assert actor_info_1 and actor_info_2

        node_of_actor_1 = actor_info_1["Address"]["NodeID"]
        node_of_actor_2 = actor_info_2["Address"]["NodeID"]
        assert node_of_actor_1 == node_of_actor_2
        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_strict_pack(ray_start_cluster, connect_to_client):
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

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name",
            strategy="STRICT_PACK",
            bundles=[
                {
                    "memory": 50 * 1024 *
                    1024,  # Test memory resource spec doesn't break tests.
                    "CPU": 2
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
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are collocated in one node.
        actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
        actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

        assert actor_info_1 and actor_info_2

        node_of_actor_1 = actor_info_1["Address"]["NodeID"]
        node_of_actor_2 = actor_info_2["Address"]["NodeID"]
        assert node_of_actor_1 == node_of_actor_2

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_spread(ray_start_cluster, connect_to_client):
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

    with connect_to_client_or_not(connect_to_client):
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
        actor_infos = ray.state.actors()

        # Make sure all actors in counter_list are located in separate nodes.
        actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
        actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

        assert actor_info_1 and actor_info_2

        node_of_actor_1 = actor_info_1["Address"]["NodeID"]
        node_of_actor_2 = actor_info_2["Address"]["NodeID"]
        assert node_of_actor_1 != node_of_actor_2

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_strict_spread(ray_start_cluster, connect_to_client):
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

    with connect_to_client_or_not(connect_to_client):
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
        actor_infos = ray.state.actors()

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

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_actor_resource_ids(ray_start_cluster,
                                            connect_to_client):
    @ray.remote(num_cpus=1)
    class F:
        def f(self):
            return ray.worker.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        g1 = ray.util.placement_group([{"CPU": 2}])
        a1 = F.options(placement_group=g1).remote()
        resources = ray.get(a1.f.remote())
        assert len(resources) == 1, resources
        assert "CPU_group_" in list(resources.keys())[0], resources
        placement_group_assert_no_leak([g1])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_task_resource_ids(ray_start_cluster,
                                           connect_to_client):
    @ray.remote(num_cpus=1)
    def f():
        return ray.worker.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        g1 = ray.util.placement_group([{"CPU": 2}])
        o1 = f.options(placement_group=g1).remote()
        resources = ray.get(o1)
        assert len(resources) == 1, resources
        assert "CPU_group_" in list(resources.keys())[0], resources
        assert "CPU_group_0_" not in list(resources.keys())[0], resources

        # Now retry with a bundle index constraint.
        o1 = f.options(
            placement_group=g1, placement_group_bundle_index=0).remote()
        resources = ray.get(o1)
        assert len(resources) == 2, resources
        keys = list(resources.keys())
        assert "CPU_group_" in keys[0], resources
        assert "CPU_group_" in keys[1], resources
        assert ("CPU_group_0_" in keys[0]
                or "CPU_group_0_" in keys[1]), resources

        placement_group_assert_no_leak([g1])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_hang(ray_start_cluster, connect_to_client):
    @ray.remote(num_cpus=1)
    def f():
        return ray.worker.get_resource_ids()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        # Warm workers up, so that this triggers the hang rice.
        ray.get(f.remote())

        g1 = ray.util.placement_group([{"CPU": 2}])
        # This will start out infeasible. The placement group will then be
        # created and it transitions to feasible.
        o1 = f.options(placement_group=g1).remote()

        resources = ray.get(o1)
        assert len(resources) == 1, resources
        assert "CPU_group_" in list(resources.keys())[0], resources

        placement_group_assert_no_leak([g1])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_remove_placement_group(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    @ray.remote
    def warmup():
        pass

    # warm up the cluster.
    ray.get([warmup.remote() for _ in range(4)])

    with connect_to_client_or_not(connect_to_client):
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

        wait_for_condition(lambda: is_placement_group_removed(placement_group))

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


def test_remove_placement_group_worker_startup_slowly(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group([{"CPU": 2}, {"CPU": 2}])
    assert placement_group.wait(10)

    @ray.remote(num_cpus=2)
    class A:
        def f(self):
            return 3

    @ray.remote(num_cpus=2, max_retries=0)
    def long_running_task():
        print(os.getpid())
        import time
        time.sleep(60)

    # Schedule a long-running task that uses
    # runtime env to mock worker start up slowly.
    task_ref = long_running_task.options(
        placement_group=placement_group,
        runtime_env={
            "plugins": {
                MOCK_WORKER_STARTUP_SLOWLY_PLUGIN_CLASS_PATH: {}
            }
        }).remote()
    a = A.options(placement_group=placement_group).remote()
    assert ray.get(a.f.remote()) == 3

    ray.util.remove_placement_group(placement_group)

    # Make sure the actor has been killed
    # because of the removal of the pg.
    # TODO(@clay4444): Make it throw a `ActorPlacementGroupRemoved`.
    with pytest.raises(ray.exceptions.RayActorError, match="actor died"):
        ray.get(a.f.remote(), timeout=3.0)

    # The long-running task should still be in the state
    # of leasing-worker bacause of the worker startup delay.
    with pytest.raises(ray.exceptions.TaskPlacementGroupRemoved):
        ray.get(task_ref)


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_remove_pending_placement_group(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        # Create a placement group that cannot be scheduled now.
        placement_group = ray.util.placement_group([{"GPU": 2}, {"CPU": 2}])
        ray.util.remove_placement_group(placement_group)

        # TODO(sang): Add state check here.
        @ray.remote(num_cpus=4)
        def f():
            return 3

        # Make sure this task is still schedulable.
        assert ray.get(f.remote()) == 3

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_table(ray_start_cluster, connect_to_client):
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
    pgs_created = []

    with connect_to_client_or_not(connect_to_client):
        # Originally placement group creation should be pending because
        # there are no resources.
        name = "name"
        strategy = "PACK"
        bundles = [{"CPU": 2, "GPU": 1}, {"CPU": 2}]
        placement_group = ray.util.placement_group(
            name=name, strategy=strategy, bundles=bundles)
        pgs_created.append(placement_group)
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
        pgs_created.append(
            ray.util.placement_group(
                name="second_placement_group",
                strategy=second_strategy,
                bundles=bundles))
        pgs_created.append(
            ray.util.placement_group(
                name="third_placement_group",
                strategy=second_strategy,
                bundles=bundles))

        placement_group_table = ray.util.placement_group_table()
        assert len(placement_group_table) == 3

        true_name_set = {
            "name", "second_placement_group", "third_placement_group"
        }
        get_name_set = set()

        for _, placement_group_data in placement_group_table.items():
            get_name_set.add(placement_group_data["name"])

        assert true_name_set == get_name_set

        placement_group_assert_no_leak(pgs_created)


def test_placement_group_stats(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4, num_gpus=1)
    ray.init(address=cluster.address)

    # Test createable pgs.
    pg = ray.util.placement_group(bundles=[{"CPU": 4, "GPU": 1}])
    ray.get(pg.ready())
    stats = ray.util.placement_group_table(pg)["stats"]
    assert stats["scheduling_attempt"] == 1
    assert stats["scheduling_state"] == "FINISHED"
    assert stats["end_to_end_creation_latency_ms"] != 0

    # Create a pending pg.
    pg2 = ray.util.placement_group(bundles=[{"CPU": 4, "GPU": 1}])

    def assert_scheduling_state():
        stats = ray.util.placement_group_table(pg2)["stats"]
        if stats["scheduling_attempt"] != 1:
            return False
        if stats["scheduling_state"] != "NO_RESOURCES":
            return False
        if stats["end_to_end_creation_latency_ms"] != 0:
            return False
        return True

    wait_for_condition(assert_scheduling_state)

    # Remove the first pg, and the second
    # pg should be schedulable now.
    ray.util.remove_placement_group(pg)

    def assert_scheduling_state():
        stats = ray.util.placement_group_table(pg2)["stats"]
        if stats["scheduling_state"] != "FINISHED":
            return False
        if stats["end_to_end_creation_latency_ms"] == 0:
            return False
        return True

    wait_for_condition(assert_scheduling_state)

    # Infeasible pg.
    pg3 = ray.util.placement_group(bundles=[{"CPU": 4, "a": 1}])
    # TODO This is supposed to be infeasible, but it is printed
    # as NO_RESOURCES. Fix the issue.
    # def assert_scheduling_state():
    #     stats = ray.util.placement_group_table(pg3)["stats"]
    #     print(stats)
    #     if stats["scheduling_state"] != "INFEASIBLE":
    #         return False
    #     return True
    # wait_for_condition(assert_scheduling_state)

    ray.util.remove_placement_group(pg3)

    def assert_scheduling_state():
        stats = ray.util.placement_group_table(pg3)["stats"]
        if stats["scheduling_state"] != "REMOVED":
            return False
        return True

    wait_for_condition(assert_scheduling_state)

    placement_group_assert_no_leak([pg2])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_cuda_visible_devices(ray_start_cluster, connect_to_client):
    @ray.remote(num_gpus=1)
    def f():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_gpus=1)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        g1 = ray.util.placement_group([{"CPU": 1, "GPU": 1}])
        o1 = f.options(placement_group=g1).remote()

        devices = ray.get(o1)
        assert devices == "0", devices
        placement_group_assert_no_leak([g1])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_reschedule_when_node_dead(ray_start_cluster,
                                                   connect_to_client):
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
    ray.init(address=cluster.address, namespace="default_test_namespace")

    # Make sure both head and worker node are alive.
    nodes = ray.nodes()
    assert len(nodes) == 3
    assert nodes[0]["alive"] and nodes[1]["alive"] and nodes[2]["alive"]

    with connect_to_client_or_not(connect_to_client):
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
        placement_group_assert_no_leak([placement_group])
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
