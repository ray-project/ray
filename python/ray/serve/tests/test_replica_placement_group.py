import asyncio
import os
import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import GANG_PG_NAME_PREFIX, DeploymentStatus
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.utils import get_all_live_placement_group_names
from ray.serve.config import GangSchedulingConfig
from ray.serve.context import _get_global_client
from ray.util.placement_group import PlacementGroup, get_current_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def _get_pg_strategy(pg: PlacementGroup) -> str:
    return ray.util.placement_group_table(pg)["strategy"]


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
@pytest.mark.asyncio
async def test_basic(serve_instance):
    """Test the basic workflow: multiple replicas with their own PGs."""

    @serve.deployment(
        num_replicas=2,
        placement_group_bundles=[{"CPU": 1}, {"CPU": 0.1}],
    )
    class D:
        def get_pg(self) -> PlacementGroup:
            return get_current_placement_group()

    h = serve.run(D.bind(), name="pg_test")

    # Verify that each replica has its own placement group with the correct config.
    assert len(get_all_live_placement_group_names()) == 2
    unique_pgs = set(await asyncio.gather(*[h.get_pg.remote() for _ in range(20)]))
    assert len(unique_pgs) == 2
    for pg in unique_pgs:
        assert _get_pg_strategy(pg) == "PACK"
        assert pg.bundle_specs == [{"CPU": 1}, {"CPU": 0.1}]

    # Verify that all placement groups are deleted when the deployment is deleted.
    serve.delete("pg_test")
    assert len(get_all_live_placement_group_names()) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
def test_upgrade_and_change_pg(serve_instance):
    """Test re-deploying a deployment with different PG bundles and strategy."""

    @serve.deployment(
        num_replicas=1,
        placement_group_bundles=[{"CPU": 1}, {"CPU": 0.1}],
        placement_group_strategy="STRICT_PACK",
    )
    class D:
        def get_pg(self) -> PlacementGroup:
            return get_current_placement_group()

    h = serve.run(D.bind(), name="pg_test")

    # Check that the original replica is created with the expected PG config.
    assert len(get_all_live_placement_group_names()) == 1
    original_pg = h.get_pg.remote().result()
    assert original_pg.bundle_specs == [{"CPU": 1}, {"CPU": 0.1}]
    assert _get_pg_strategy(original_pg) == "STRICT_PACK"

    # Re-deploy with a new PG config.
    D = D.options(
        placement_group_bundles=[{"CPU": 2}, {"CPU": 0.2}],
        placement_group_strategy="SPREAD",
    )

    h = serve.run(D.bind(), name="pg_test")
    assert len(get_all_live_placement_group_names()) == 1
    new_pg = h.get_pg.remote().result()
    assert new_pg.bundle_specs == [{"CPU": 2}, {"CPU": 0.2}]
    assert _get_pg_strategy(new_pg) == "SPREAD"

    # Verify that all placement groups are deleted when the deployment is deleted.
    serve.delete("pg_test")
    assert len(get_all_live_placement_group_names()) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
@pytest.mark.asyncio
async def test_pg_removed_on_replica_graceful_shutdown(serve_instance):
    """Verify that PGs are removed when a replica shuts down gracefully."""

    @serve.deployment(
        placement_group_bundles=[{"CPU": 1}],
    )
    class D:
        def get_pg(self) -> PlacementGroup:
            return get_current_placement_group()

    h = serve.run(D.options(num_replicas=2).bind(), name="pg_test")

    # Two replicas to start, each should have their own placement group.
    assert len(get_all_live_placement_group_names()) == 2
    original_unique_pgs = set(
        await asyncio.gather(*[h.get_pg.remote() for _ in range(20)])
    )
    assert len(original_unique_pgs) == 2

    # Re-deploy the application with a single replica.
    # The existing PGs should be removed and a new one should be created for the
    # new replica.
    h = serve.run(D.options(num_replicas=1).bind(), name="pg_test")
    assert len(get_all_live_placement_group_names()) == 1
    new_unique_pgs = set(await asyncio.gather(*[h.get_pg.remote() for _ in range(20)]))
    assert len(new_unique_pgs) == 1
    assert not new_unique_pgs.issubset(original_unique_pgs)

    # Verify that all placement groups are deleted when the deployment is deleted.
    serve.delete("pg_test")
    assert len(get_all_live_placement_group_names()) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
def test_pg_removed_on_replica_crash(serve_instance):
    """Verify that PGs are removed when a replica crashes unexpectedly."""

    @serve.deployment(
        placement_group_bundles=[{"CPU": 1}],
        health_check_period_s=0.1,
    )
    class D:
        def die(self):
            os._exit(1)

        def get_pg(self) -> PlacementGroup:
            return get_current_placement_group()

    h = serve.run(D.bind(), name="pg_test")

    # Get the placement group for the original replica.
    assert len(get_all_live_placement_group_names()) == 1
    pg = h.get_pg.remote().result()

    # Kill the replica forcefully.
    with pytest.raises(ray.exceptions.RayActorError):
        h.die.remote().result()

    def new_replica_scheduled():
        try:
            h.get_pg.remote().result()
        except ray.exceptions.RayActorError:
            return False

        return True

    # The original placement group should be deleted and a new replica should
    # be scheduled with its own new placement group.
    wait_for_condition(new_replica_scheduled)
    new_pg = h.get_pg.remote().result()
    assert pg != new_pg
    assert len(get_all_live_placement_group_names()) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
def test_pg_removed_after_controller_crash(serve_instance):
    """Verify that PGs are removed normally after recovering from a controller crash.

    If the placement group was not properly recovered in the replica recovery process,
    it would be leaked here.
    """

    @serve.deployment(
        placement_group_bundles=[{"CPU": 1}],
    )
    class D:
        pass

    serve.run(D.bind(), name="pg_test")
    assert len(get_all_live_placement_group_names()) == 1

    ray.kill(_get_global_client()._controller, no_restart=False)

    serve.delete("pg_test")
    assert len(get_all_live_placement_group_names()) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
def test_leaked_pg_removed_on_controller_recovery(serve_instance):
    """Verify that leaked PGs are removed on controller recovery.

    A placement group can be "leaked" if the replica is killed while the controller is
    down or the controller crashes between creating a placement group and its replica.

    In these cases, the controller should detect the leak on recovery and delete the
    leaked placement group(s).
    """

    @serve.deployment(
        placement_group_bundles=[{"CPU": 1}],
        health_check_period_s=0.1,
    )
    class D:
        def die(self):
            os._exit(1)

        def get_pg(self) -> PlacementGroup:
            return get_current_placement_group()

    h = serve.run(D.bind(), name="pg_test")

    prev_pg = h.get_pg.remote().result()
    assert len(get_all_live_placement_group_names()) == 1

    # Kill the controller and the replica immediately after.
    # This will cause the controller to *not* detect the replica on recovery, but it
    # should still detect the leaked placement group and clean it up.
    ray.kill(_get_global_client()._controller, no_restart=False)
    with pytest.raises(ray.exceptions.RayActorError):
        h.die.remote().result()

    def leaked_pg_cleaned_up():
        try:
            new_pg = h.get_pg.remote().result()
        except ray.exceptions.RayActorError:
            return False

        return len(get_all_live_placement_group_names()) == 1 and new_pg != prev_pg

    # Verify that a new replica is placed with a new placement group and the old
    # placement group has been removed.
    wait_for_condition(leaked_pg_cleaned_up)

    serve.delete("pg_test")
    assert len(get_all_live_placement_group_names()) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
def test_leaked_gang_pg_removed_on_controller_recovery(serve_instance):
    """Verify that leaked gang PGs are removed while in-use gang PGs are kept.

    Deploys two gang-scheduled apps (``survivor`` and ``victim``) and a
    non-gang app with a per-replica placement group (``per_replica``).  After
    killing the controller and the victim's + per_replica's actors:

    * The victim's gang PG has no alive actors -> detected as leaked -> removed.
    * The per-replica PG has no alive actor -> detected as leaked -> removed.
    * The survivor's gang PG still has alive actors -> preserved.
    * All apps recover successfully.
    """

    @serve.deployment(
        num_replicas=2,
        ray_actor_options={"num_cpus": 0.1},
        gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        health_check_period_s=1,
    )
    class Survivor:
        def __call__(self):
            return "survivor_ok"

    @serve.deployment(
        num_replicas=2,
        ray_actor_options={"num_cpus": 0.1},
        gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        health_check_period_s=1,
    )
    class Victim:
        def __call__(self):
            return "victim_ok"

    @serve.deployment(
        placement_group_bundles=[{"CPU": 0.1}],
        ray_actor_options={"num_cpus": 0.1},
        health_check_period_s=1,
    )
    class PerReplica:
        def get_pg(self) -> PlacementGroup:
            return get_current_placement_group()

        def __call__(self):
            return "per_replica_ok"

    h_surv = serve.run(Survivor.bind(), name="survivor_app", route_prefix="/surv")
    h_vict = serve.run(Victim.bind(), name="victim_app", route_prefix="/vict")
    h_pr = serve.run(
        PerReplica.bind(), name="per_replica_app", route_prefix="/per_replica"
    )

    assert h_surv.remote().result() == "survivor_ok"
    assert h_vict.remote().result() == "victim_ok"
    assert h_pr.remote().result() == "per_replica_ok"

    prev_per_replica_pg = h_pr.get_pg.remote().result()

    # There should be exactly 2 gang PGs (one per gang app) + 1 per-replica PG.
    all_pg_names = get_all_live_placement_group_names()
    gang_pgs = [n for n in all_pg_names if n.startswith(GANG_PG_NAME_PREFIX)]
    non_gang_pgs = [n for n in all_pg_names if not n.startswith(GANG_PG_NAME_PREFIX)]
    assert len(gang_pgs) == 2
    assert len(non_gang_pgs) == 1

    survivor_pg = [n for n in gang_pgs if "survivor_app" in n]
    victim_pg = [n for n in gang_pgs if "victim_app" in n]
    assert len(survivor_pg) == 1
    assert len(victim_pg) == 1
    original_survivor_pg_name = survivor_pg[0]

    # Kill the controller
    ray.kill(_get_global_client()._controller, no_restart=False)

    # Kill the victim actors and per-replica actors while the controller is restarting
    actors_to_kill = [
        a
        for a in ray.util.list_named_actors(all_namespaces=True)
        if a["namespace"] == SERVE_NAMESPACE
        and ("victim_app" in a["name"] or "per_replica_app" in a["name"])
    ]
    for actor_info in actors_to_kill:
        try:
            handle = ray.get_actor(actor_info["name"], namespace=SERVE_NAMESPACE)
            ray.kill(handle, no_restart=True)
        except Exception:
            continue

    def recovery_complete():
        try:
            # Survivor should still work.
            result = h_surv.remote().result()
            assert result == "survivor_ok"
        except Exception:
            return False

        current_gang_pgs = [
            n
            for n in get_all_live_placement_group_names()
            if n.startswith(GANG_PG_NAME_PREFIX)
        ]

        # The survivor's *original* PG must still exist (not removed).
        survivor_pgs = [n for n in current_gang_pgs if "survivor_app" in n]
        if original_survivor_pg_name not in survivor_pgs:
            return False

        # Victim should have recovered with a new PG (old one was removed).
        victim_pgs = [n for n in current_gang_pgs if "victim_app" in n]
        if len(victim_pgs) < 1:
            return False

        # Per-replica app should have recovered with a new PG.
        try:
            new_per_replica_pg = h_pr.get_pg.remote().result()
        except Exception:
            return False
        if new_per_replica_pg == prev_per_replica_pg:
            return False

        return True

    wait_for_condition(recovery_complete, timeout=60)

    # Verify all apps recovered and are serving.
    assert h_vict.remote().result() == "victim_ok"
    assert h_pr.remote().result() == "per_replica_ok"

    # Verify all apps are RUNNING and deployments are HEALTHY.
    status = serve.status()
    for app_name in ("survivor_app", "victim_app", "per_replica_app"):
        app_status = status.applications[app_name]
        assert app_status.status == "RUNNING"
        for dep_name, dep_status in app_status.deployments.items():
            assert dep_status.status == DeploymentStatus.HEALTHY

    serve.delete("survivor_app")
    serve.delete("victim_app")
    serve.delete("per_replica_app")


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
def test_replica_actor_infeasible(serve_instance):
    """Test that we get a validation error if the replica doesn't fit in the bundle."""

    class Infeasible:
        pass

    with pytest.raises(ValueError):
        serve.deployment(placement_group_bundles=[{"CPU": 0.1}])(Infeasible)

    with pytest.raises(ValueError):
        serve.deployment(Infeasible).options(placement_group_bundles=[{"CPU": 0.1}])


@pytest.mark.skipif(sys.platform == "win32", reason="Timing out on Windows.")
def test_coschedule_actors_and_tasks(serve_instance):
    """Test that actor/tasks are placed in the replica's placement group by default."""

    @ray.remote(num_cpus=1)
    class TestActor:
        def get_pg(self) -> PlacementGroup:
            return get_current_placement_group()

    @ray.remote
    def get_pg():
        return get_current_placement_group()

    @serve.deployment(
        # Bundles have space for one additional 1-CPU actor.
        placement_group_bundles=[{"CPU": 1}, {"CPU": 1}],
    )
    class Parent:
        def run_test(self):
            # First actor should be scheduled in the placement group without issue.
            a1 = TestActor.remote()
            assert ray.get(a1.get_pg.remote()) == get_current_placement_group()

            # Second actor can't be placed because there are no more resources in the
            # placement group (the first actor is occupying the second bundle).
            a2 = TestActor.remote()
            ready, _ = ray.wait([a2.get_pg.remote()], timeout=0.1)
            assert len(ready) == 0
            ray.kill(a2)

            # Second actor can be successfully scheduled outside the placement group.
            a3 = TestActor.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=None
                )
            ).remote()
            assert ray.get(a3.get_pg.remote()) is None

            # A zero-CPU task can be scheduled in the placement group.
            assert (
                ray.get(get_pg.options(num_cpus=0).remote())
                == get_current_placement_group()
            )

            # A two-CPU task cannot fit in the placement group.
            with pytest.raises(ValueError):
                get_pg.options(num_cpus=2).remote()

            # A two-CPU task can be scheduled outside the placement group.
            assert (
                ray.get(
                    get_pg.options(
                        num_cpus=2,
                        scheduling_strategy=PlacementGroupSchedulingStrategy(
                            placement_group=None
                        ),
                    ).remote()
                )
                is None
            )

    h = serve.run(Parent.bind())
    h.run_test.remote().result()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
