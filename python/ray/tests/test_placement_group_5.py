import asyncio
import sys
import time
from functools import reduce
from itertools import chain

from click.testing import CliRunner
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import placement_group_assert_no_leak
from ray.tests.test_placement_group import are_pairwise_unique
from ray.util.state import list_actors, list_placement_groups
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.test_utils import fetch_prometheus_metrics
import ray.scripts.scripts as scripts


def test_placement_group_no_resource(ray_start_cluster):
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


def test_pg_no_resource_bundle_index(ray_start_cluster):
    @ray.remote(num_cpus=0)
    class Actor:
        def node_id(self):
            return ray.get_runtime_context().get_node_id()

    cluster = ray_start_cluster
    num_nodes = 4
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    pg = ray.util.placement_group(
        bundles=[{"CPU": 1} for _ in range(num_nodes)],
    )
    ray.get(pg.ready())
    first_bundle_node_id = ray.util.placement_group_table(pg)["bundles_to_node_id"][0]

    # Iterate 5 times to make sure it is not flaky.
    for _ in range(5):
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
            "{'CPU': 1.0}: 1+ pending tasks/actors (1+ using placement groups)"
        )
        assert expected_demand_str in result.stdout, result.stdout
        return True

    wait_for_condition(check_demands)


@pytest.mark.parametrize("scheduling_strategy", ["SPREAD", "STRICT_SPREAD", "PACK"])
def test_placement_group_bin_packing_priority(ray_start_cluster, scheduling_strategy):
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


def test_placement_group_parallel_submission(ray_start_cluster):
    NUM_PARALLEL_PGS = 5
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"custom_resource": NUM_PARALLEL_PGS})
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote(resources={"custom_resource": 1})
    def task(input):
        return "ok"

    @ray.remote(num_cpus=0)
    class Submitter:
        def submit(self, strategy: str):
            pg = ray.util.placement_group(
                [{"custom_resource": 1, "CPU": 1}], strategy=strategy
            )
            try:
                ray.get(pg.ready())
                pg_strategy = (
                    ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                        placement_group=pg
                    )
                )
                return ray.get(
                    task.options(scheduling_strategy=pg_strategy).remote(input)
                )
            finally:
                ray.util.remove_placement_group(pg)

    # For each strategy, submit multiple placement groups in parallel and check that they
    # will all eventually be placed and their tasks executed.
    submitters = [Submitter.remote() for _ in range(NUM_PARALLEL_PGS)]
    for strategy in ["SPREAD", "STRICT_SPREAD", "PACK", "STRICT_PACK"]:
        print("Testing strategy:", strategy)
        assert (
            ray.get([s.submit.remote(strategy) for s in submitters], timeout=30)
            == ["ok"] * NUM_PARALLEL_PGS
        )


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


def test_remove_placement_group_with_pending_worker_lease_waiting_for_pg_resource(
    shutdown_only,
):
    """
    Test removing a pg with a pending worker lease request acquiring the pg resources.
    details: https://github.com/ray-project/ray/issues/51124
    Specific test steps:
      1. Create a placement group with only 1 bundle.
      2. Create two actors using the aforementioned pg. At this point,
         the latter actor lease request will definitely be pending in local task manager dispatch queue due to
         unavailable pg bundle resources.
      3. Remove the pg while the latter actor lease request is pending.
      4. Verify that the pending actor lease request is cancelled and the pg
         is removed successfully.
    """
    context = ray.init(num_cpus=1)
    prom_address = f"{context.address_info['node_ip_address']}:{context.address_info['metrics_export_port']}"

    pg = ray.util.placement_group(
        [{"CPU": 1}],
    )

    @ray.remote(
        num_cpus=1,
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=0
        ),
    )
    class Actor:
        def ping(self):
            pass

    actor1 = Actor.remote()
    # Actor1 is scheduled and used all the PG resources.
    ray.get(actor1.ping.remote())

    actor2 = Actor.remote()

    def wait_for_actor2_added_to_dispatch_queue():
        metrics = fetch_prometheus_metrics([prom_address])
        samples = metrics.get("ray_scheduler_tasks", None)
        if samples is None:
            return False
        for sample in samples:
            if sample.labels["State"] == "Dispatched" and sample.value == 1:
                # actor2 is in the local task manager dispatch queue
                return True
        return False

    wait_for_condition(wait_for_actor2_added_to_dispatch_queue, timeout=30)

    ray.util.remove_placement_group(pg)

    def check_pg_removed():
        pgs = list_placement_groups()
        assert len(pgs) == 1
        assert "REMOVED" == pgs[0].state
        return True

    wait_for_condition(check_pg_removed)

    # Actors should be dead due to the pg removal.
    def check_actor_dead():
        actors = list_actors()
        assert len(actors) == 2
        assert [actors[0].state, actors[1].state] == ["DEAD", "DEAD"]
        return True

    wait_for_condition(check_actor_dead)

    # Actor2 should be cancelled due to the pg removal.
    with pytest.raises(ray.exceptions.ActorUnschedulableError):
        ray.get(actor2.ping.remote())

    # Check that the raylet is still running.
    @ray.remote
    def task():
        return 1

    assert ray.get(task.remote()) == 1


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
