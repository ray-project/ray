import pytest
import sys
import time

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils

from ray._private.test_utils import (
    wait_for_condition,
    placement_group_assert_no_leak,
)
from ray.util.client.ray_client_helpers import connect_to_client_or_not


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_check_bundle_index(ray_start_cluster_enabled, connect_to_client):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name", strategy="SPREAD", bundles=[{"CPU": 2}, {"CPU": 2}]
        )

        with pytest.raises(ValueError, match="bundle index 3 is invalid"):
            Actor.options(
                placement_group=placement_group, placement_group_bundle_index=3
            ).remote()

        with pytest.raises(ValueError, match="bundle index -2 is invalid"):
            Actor.options(
                placement_group=placement_group, placement_group_bundle_index=-2
            ).remote()

        with pytest.raises(ValueError, match="bundle index must be -1"):
            Actor.options(placement_group_bundle_index=0).remote()

        placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_pending_placement_group_wait(ray_start_cluster_enabled, connect_to_client):
    cluster = ray_start_cluster_enabled
    [cluster.add_node(num_cpus=2) for _ in range(1)]
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    with connect_to_client_or_not(connect_to_client):
        # Wait on placement group that cannot be created.
        placement_group = ray.util.placement_group(
            name="name",
            strategy="SPREAD",
            bundles=[
                {"CPU": 2},
                {"CPU": 2},
                {"GPU": 2},
            ],
        )
        ready, unready = ray.wait([placement_group.ready()], timeout=0.1)
        assert len(unready) == 1
        assert len(ready) == 0
        table = ray.util.placement_group_table(placement_group)
        assert table["state"] == "PENDING"
        with pytest.raises(ray.exceptions.GetTimeoutError):
            ray.get(placement_group.ready(), timeout=0.1)


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_wait(ray_start_cluster_enabled, connect_to_client):
    cluster = ray_start_cluster_enabled
    [cluster.add_node(num_cpus=2) for _ in range(2)]
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    with connect_to_client_or_not(connect_to_client):
        # Wait on placement group that cannot be created.
        placement_group = ray.util.placement_group(
            name="name",
            strategy="SPREAD",
            bundles=[
                {"CPU": 2},
                {"CPU": 2},
            ],
        )
        ready, unready = ray.wait([placement_group.ready()])
        assert len(unready) == 0
        assert len(ready) == 1
        table = ray.util.placement_group_table(placement_group)
        assert table["state"] == "CREATED"

        pg = ray.get(placement_group.ready())
        assert pg.bundle_specs == placement_group.bundle_specs
        assert pg.id.binary() == placement_group.id.binary()


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_schedule_placement_group_when_node_add(
    ray_start_cluster_enabled, connect_to_client
):
    cluster = ray_start_cluster_enabled
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
            bundles=[
                {"CPU": bundle_cpu_size} for _ in range(num_nodes * bundle_per_node)
            ],
        )

        # Create a placement group actor.
        # This shouldn't be scheduled because atomic
        # placement group creation should've failed.
        pg_actor = NormalActor.options(
            placement_group=pg,
            placement_group_bundle_index=num_nodes * bundle_per_node - 1,
        ).remote()

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
                placement_group=pg, placement_group_bundle_index=i
            ).remote()
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
