import asyncio
import sys
import time
from functools import reduce
from itertools import chain

import pytest

import ray
from ray._private.test_utils import placement_group_assert_no_leak
from ray.tests.test_placement_group import are_pairwise_unique
from ray.util.state import list_actors, list_placement_groups
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.test_utils import wait_for_condition
from click.testing import CliRunner
import ray.scripts.scripts as scripts


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_no_resource(ray_start_cluster, connect_to_client):
    @ray.remote(num_cpus=1)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    @ray.remote(num_cpus=0)
    class PhantomActor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        for _ in range(10):
            pg1 = ray.util.placement_group(
                name="pg1",
                bundles=[
                    {"CPU": 2},
                ],
            )
            pg2 = ray.util.placement_group(
                name="pg2",
                bundles=[
                    {"CPU": 2},
                ],
            )
            ray.get(pg1.ready())
            ray.get(pg2.ready())
            actor_11 = Actor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg1, placement_group_bundle_index=0
                )
            ).remote()
            actor_12 = Actor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg1, placement_group_bundle_index=0
                )
            ).remote()
            actor_13 = PhantomActor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg1, placement_group_bundle_index=0
                )
            ).remote()
            actor_21 = Actor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg2, placement_group_bundle_index=0
                )
            ).remote()
            actor_22 = Actor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg2, placement_group_bundle_index=0
                )
            ).remote()
            actor_23 = PhantomActor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg2, placement_group_bundle_index=0
                )
            ).remote()

            first_node = [actor_11, actor_12, actor_13]
            second_node = [actor_21, actor_22, actor_23]

            for actor in chain(first_node, second_node):
                ray.get(actor.value.remote())

            # Get all actors.
            actor_infos = ray._private.state.actors()

            first_node_ids = [
                actor_infos.get(actor._actor_id.hex())["Address"]["NodeID"]
                for actor in first_node
            ]
            second_node_ids = [
                actor_infos.get(actor._actor_id.hex())["Address"]["NodeID"]
                for actor in second_node
            ]

            def check_eq(ip1, ip2):
                assert ip1 == ip2
                return ip1

            assert reduce(check_eq, first_node_ids) != reduce(check_eq, second_node_ids)

            placement_group_assert_no_leak([pg1, pg2])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_pg_no_resource_bundle_index(ray_start_cluster, connect_to_client):
    @ray.remote(num_cpus=0)
    class Actor:
        def node_id(self):
            return ray.get_runtime_context().get_node_id()

    cluster = ray_start_cluster
    num_nodes = 4
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        pg = ray.util.placement_group(
            bundles=[{"CPU": 1} for _ in range(num_nodes)],
        )
        ray.get(pg.ready())
        first_bundle_node_id = ray.util.placement_group_table(pg)["bundles_to_node_id"][
            0
        ]

        # Iterate 10 times to make sure it is not flaky.
        for _ in range(10):
            actor = Actor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=0
                )
            ).remote()

            assert first_bundle_node_id == ray.get(actor.node_id.remote())

        placement_group_assert_no_leak([pg])


# Make sure the task observability API outputs don't contain
# pg related data.
# TODO(sang): Currently, when a task hangs because the bundle
# index doesn't have enough resources, it is not displayed. Fix it.
def test_task_using_pg_observability(ray_start_cluster):
    @ray.remote(num_cpus=1)
    class Actor:
        def get_assigned_resources(self):
            return ray.get_runtime_context().get_assigned_resources()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    pg = ray.util.placement_group(
        bundles=[{"CPU": 1} for _ in range(num_nodes)],
    )

    # Make sure get_assigned_id doesn't contain formatted resources.
    bundle_index = 0
    actor1 = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=bundle_index
        )
    ).remote()
    r = ray.get(actor1.get_assigned_resources.remote())
    assert "bundle_group" not in r
    assert f"bundle_group_{bundle_index}" not in r

    # Make sure ray status doesn't contain formatted resources.
    actor2 = Actor.options(  # noqa
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=0
        )
    ).remote()

    def check_demands():
        runner = CliRunner()
        result = runner.invoke(scripts.status)
        if "No cluster status." in result.stdout:
            return False

        expected_demand_str = (
            "{'CPU': 1.0}: 1+ pending tasks/actors " "(1+ using placement groups)"
        )
        assert expected_demand_str in result.stdout, result.stdout
        return True

    wait_for_condition(check_demands)


@pytest.mark.parametrize("connect_to_client", [False, True])
@pytest.mark.parametrize("scheduling_strategy", ["SPREAD", "STRICT_SPREAD", "PACK"])
def test_placement_group_bin_packing_priority(
    ray_start_cluster, connect_to_client, scheduling_strategy
):
    @ray.remote
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    def index_to_actor(pg, index):
        if index < 2:
            return Actor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=index
                ),
                num_cpus=1,
            ).remote()
        else:
            return Actor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=index
                ),
                num_gpus=1,
            ).remote()

    def add_nodes_to_cluster(cluster):
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=2)
        cluster.add_node(num_gpus=1)

    default_bundles = [
        {"CPU": 1},
        {"CPU": 2},
        {"CPU": 1, "GPU": 1},
    ]

    default_num_nodes = len(default_bundles)
    cluster = ray_start_cluster
    add_nodes_to_cluster(cluster)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name",
            strategy=scheduling_strategy,
            bundles=default_bundles,
        )
        ray.get(placement_group.ready())

        actors = [index_to_actor(placement_group, i) for i in range(default_num_nodes)]

        [ray.get(actor.value.remote()) for actor in actors]

        # Get all actors.
        actor_infos = ray._private.state.actors()

        # Make sure all actors in counter_list are located in separate nodes.
        actor_info_objs = [actor_infos.get(actor._actor_id.hex()) for actor in actors]
        assert are_pairwise_unique(
            [info_obj["Address"]["NodeID"] for info_obj in actor_info_objs]
        )


@pytest.mark.parametrize("multi_bundle", [True, False])
@pytest.mark.parametrize("even_pack", [True, False])
@pytest.mark.parametrize("scheduling_strategy", ["SPREAD", "STRICT_PACK", "PACK"])
def test_placement_group_max_cpu_frac(
    ray_start_cluster, multi_bundle, even_pack, scheduling_strategy
):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    if multi_bundle:
        bundles = [{"CPU": 1}] * 3
    else:
        bundles = [{"CPU": 3}]

    # Input validation - max_cpu_fraction_per_node must be between 0 and 1.
    with pytest.raises(ValueError):
        ray.util.placement_group(bundles, _max_cpu_fraction_per_node=-1)
    with pytest.raises(ValueError):
        ray.util.placement_group(bundles, _max_cpu_fraction_per_node=2)

    pg = ray.util.placement_group(
        bundles, strategy=scheduling_strategy, _max_cpu_fraction_per_node=0.5
    )

    # Placement group will never be scheduled since it would violate the max CPU
    # fraction reservation.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg.ready(), timeout=5)

    # Add new node with enough CPU cores to scheduled placement group bundle while
    # adhering to the max CPU fraction constraint.
    if even_pack:
        num_cpus = 6
    else:
        num_cpus = 8
    cluster.add_node(num_cpus=num_cpus)
    cluster.wait_for_nodes()
    # The placement group should be schedulable so this shouldn't raise.
    ray.get(pg.ready(), timeout=5)


def test_placement_group_max_cpu_frac_multiple_pgs(ray_start_cluster):
    """
    Make sure when there's more than 1 pg, they respect the fraction.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=8)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # This pg should be scheduable.
    pg = ray.util.placement_group([{"CPU": 4}], _max_cpu_fraction_per_node=0.5)
    ray.get(pg.ready())

    # When we schedule another placement group, it shouldn't be scheduled.
    pg2 = ray.util.placement_group([{"CPU": 4}], _max_cpu_fraction_per_node=0.5)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg2.ready(), timeout=5)

    # When you add a new node, it is finally schedulable.
    cluster.add_node(num_cpus=8)
    ray.get(pg2.ready())


def test_placement_group_max_cpu_frac_edge_cases(ray_start_cluster):
    """
    _max_cpu_fraction_per_node <= 0  ---> should raise error (always)
    _max_cpu_fraction_per_node = 0.999 --->
        should exclude 1 CPU (this is already the case)
    _max_cpu_fraction_per_node = 0.001 --->
        should exclude 3 CPUs (not currently the case, we'll exclude all 4 CPUs).

    Related: https://github.com/ray-project/ray/issues/26635
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    """
    0 or 1 is not allowed.
    """
    with pytest.raises(ValueError):
        ray.util.placement_group([{"CPU": 1}], _max_cpu_fraction_per_node=0)

    """
    Make sure when _max_cpu_fraction_per_node = 0.999, 1 CPU is always excluded.
    """
    pg = ray.util.placement_group(
        [{"CPU": 1} for _ in range(4)], _max_cpu_fraction_per_node=0.999
    )
    # Since 1 CPU is excluded, we cannot schedule this pg.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg.ready(), timeout=5)
    ray.util.remove_placement_group(pg)

    # Since 1 CPU is excluded, we can schedule 1 num_cpus actor after creating
    # CPU: 1 * 3 bundle placement groups.
    @ray.remote(num_cpus=1)
    class A:
        def ready(self):
            pass

    # Try actor creation -> pg creation.
    a = A.remote()
    ray.get(a.ready.remote())
    pg = ray.util.placement_group(
        [{"CPU": 1} for _ in range(3)], _max_cpu_fraction_per_node=0.999
    )
    ray.get(pg.ready())

    ray.kill(a)
    ray.util.remove_placement_group(pg)

    # Make sure the opposite order also works. pg creation -> actor creation.
    pg = ray.util.placement_group(
        [{"CPU": 1} for _ in range(3)], _max_cpu_fraction_per_node=0.999
    )
    a = A.remote()
    ray.get(a.ready.remote())
    ray.get(pg.ready())

    ray.kill(a)
    ray.util.remove_placement_group(pg)

    """
    _max_cpu_fraction_per_node = 0.001 --->
        should exclude 3 CPUs (not currently the case, we'll exclude all 4 CPUs).
    """
    # We can schedule up to 1 pg.
    pg = ray.util.placement_group([{"CPU": 1}], _max_cpu_fraction_per_node=0.001)
    ray.get(pg.ready())
    # Cannot schedule any more PG.
    pg2 = ray.util.placement_group([{"CPU": 1}], _max_cpu_fraction_per_node=0.001)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg2.ready(), timeout=5)

    # Since 3 CPUs are excluded, we can schedule actors.
    actors = [A.remote() for _ in range(3)]
    ray.get([a.ready.remote() for a in actors])

    # Once pg 1 is removed, pg 2 can be created since there's 1 CPU that can be
    # used for this pg.
    ray.util.remove_placement_group(pg)
    ray.get(pg2.ready())


@pytest.mark.parametrize(
    "scheduling_strategy", ["SPREAD", "STRICT_SPREAD", "PACK", "STRICT_PACK"]
)
def test_placement_group_parallel_submission(ray_start_cluster, scheduling_strategy):
    @ray.remote(resources={"custom_resource": 1})
    def task(input):
        return input

    @ray.remote(num_cpus=0)
    def manage_tasks(input):
        pg = ray.util.placement_group(
            [{"custom_resource": 1, "CPU": 1}], strategy=scheduling_strategy
        )
        ray.get(pg.ready())
        pg_strategy = ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
            placement_group=pg
        )

        obj_ref = task.options(scheduling_strategy=pg_strategy).remote(input)
        ray.get(obj_ref)

        ray.util.remove_placement_group(pg)
        return "OK"

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"custom_resource": 20})
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Test all tasks will not hang
    ray.get([manage_tasks.remote(i) for i in range(20)], timeout=50)


MyPlugin = "MyPlugin"
MY_PLUGIN_CLASS_PATH = "ray.tests.test_placement_group_5.HangPlugin"
PLUGIN_TIMEOUT = 10


class HangPlugin(RuntimeEnvPlugin):
    name = MyPlugin

    async def create(
        self,
        uri,
        runtime_env,
        ctx,
        logger,  # noqa: F821
    ) -> float:
        await asyncio.sleep(PLUGIN_TIMEOUT)

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return 1


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + MY_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
def test_placement_group_leaks(set_runtime_env_plugins, shutdown_only):
    """Handles https://github.com/ray-project/ray/pull/42942

    Handle an edge case where if a task is scheduled & worker is not
    started before pg is removed, it leaks.
    """
    ray.init(num_cpus=1, _system_config={"prestart_worker_first_driver": False})

    @ray.remote
    class Actor:
        pass

    @ray.remote
    def f():
        pass

    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    actor = Actor.options(  # noqa
        num_cpus=1,
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
        ),
        runtime_env={MyPlugin: {"name": "f2"}},
    ).remote()

    # The race condition is triggered
    # if scheduling succeeds, but a worker is not started.
    # So we should make sure to wait until actor is scheduled.
    # Since there's no API to get that timing, we just wait sufficient time.
    time.sleep(PLUGIN_TIMEOUT // 2)

    # Verify pg resources are created.
    def verify_pg_resources_created():
        r_keys = ray.available_resources().keys()
        return any("group" in k for k in r_keys)

    wait_for_condition(verify_pg_resources_created)

    ray.util.remove_placement_group(pg)
    wait_for_condition(lambda: list_placement_groups()[0].state == "REMOVED")

    # Verify pg resources are cleaned up.
    def verify_pg_resources_cleaned():
        r_keys = ray.available_resources().keys()
        return all("group" not in k for k in r_keys)

    wait_for_condition(verify_pg_resources_cleaned, timeout=30)

    # Verify an actor is killed properly.

    def verify_actor_killed():
        state = list_actors()[0].state
        return state == "DEAD"

    wait_for_condition(verify_actor_killed)


def test_placement_group_strict_pack_soft_target_node_id(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=8, resources={"head": 1})
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=2, resources={"worker1": 1})
    worker2_node = cluster.add_node(num_cpus=4, resources={"worker2": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker1_node_id = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    worker2_node_id = ray.get(get_node_id.options(resources={"worker2": 1}).remote())

    # soft_target_node_id only works with STRICT_PACK
    with pytest.raises(ValueError):
        pg = ray.util.placement_group(
            bundles=[{"CPU": 2}, {"CPU": 2}],
            strategy="PACK",
            _soft_target_node_id=ray.NodeID.from_random().hex(),
        )

    # Invalid target node id
    with pytest.raises(ValueError):
        pg = ray.util.placement_group(
            bundles=[{"CPU": 2}, {"CPU": 2}], strategy="PACK", _soft_target_node_id="a"
        )

    # No target node.
    pg = ray.util.placement_group(
        bundles=[{"CPU": 2}, {"CPU": 2}], strategy="STRICT_PACK"
    )
    wait_for_condition(lambda: ray.available_resources()["CPU"] == 10)
    assert (
        ray.get(
            get_node_id.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
            ).remote()
        )
        == head_node_id
    )
    ray.util.remove_placement_group(pg)
    wait_for_condition(lambda: ray.available_resources()["CPU"] == 14)

    # Target node doesn't have enough available resources.
    pg = ray.util.placement_group(
        bundles=[{"CPU": 2}, {"CPU": 2}],
        strategy="STRICT_PACK",
        _soft_target_node_id=worker1_node_id,
    )
    wait_for_condition(lambda: ray.available_resources()["CPU"] == 10)
    assert (
        ray.get(
            get_node_id.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
            ).remote()
        )
        == head_node_id
    )
    ray.util.remove_placement_group(pg)
    wait_for_condition(lambda: ray.available_resources()["CPU"] == 14)

    # Target node doesn't exist.
    pg = ray.util.placement_group(
        bundles=[{"CPU": 2}, {"CPU": 2}],
        strategy="STRICT_PACK",
        _soft_target_node_id=ray.NodeID.from_random().hex(),
    )
    wait_for_condition(lambda: ray.available_resources()["CPU"] == 10)
    assert (
        ray.get(
            get_node_id.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
            ).remote()
        )
        == head_node_id
    )
    ray.util.remove_placement_group(pg)
    wait_for_condition(lambda: ray.available_resources()["CPU"] == 14)

    # Target node has enough available resources.
    pg = ray.util.placement_group(
        bundles=[{"CPU": 2}, {"CPU": 2}],
        strategy="STRICT_PACK",
        _soft_target_node_id=worker2_node_id,
    )
    wait_for_condition(lambda: ray.available_resources()["CPU"] == 10)
    assert (
        ray.get(
            get_node_id.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
            ).remote()
        )
        == worker2_node_id
    )

    # After target node dies, the pg can be recovered elsewhere.
    cluster.remove_node(worker2_node)
    cluster.wait_for_nodes()
    assert (
        ray.get(
            get_node_id.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
            ).remote()
        )
        == head_node_id
    )


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
