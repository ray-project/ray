import pytest
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.cluster_utils
import ray._private.gcs_utils as gcs_utils

from ray._private.test_utils import (
    kill_actor_and_wait_for_failure,
    get_error_message,
    convert_actor_state,
)
from ray.util.placement_group import get_current_placement_group
from ray.util.client.ray_client_helpers import connect_to_client_or_not


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_capture_child_actors(ray_start_cluster_enabled, connect_to_client):
    cluster = ray_start_cluster_enabled
    total_num_actors = 4
    for _ in range(2):
        cluster.add_node(num_cpus=total_num_actors)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        pg = ray.util.placement_group([{"CPU": 2}, {"CPU": 2}], strategy="STRICT_PACK")
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
            placement_group=pg, placement_group_capture_child_tasks=True
        ).remote()
        ray.get(a.ready.remote())
        # 1 top level actor + 3 children.
        for _ in range(total_num_actors - 1):
            ray.get(a.schedule_nested_actor.remote())
        # Make sure all the actors are scheduled on the same node.
        # (why? The placement group has STRICT_PACK strategy).
        node_id_set = set()
        for actor_info in ray.state.actors().values():
            if actor_info["State"] == convert_actor_state(
                gcs_utils.ActorTableData.ALIVE
            ):
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
                gcs_utils.ActorTableData.ALIVE
            ):
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
                gcs_utils.ActorTableData.ALIVE
            ):
                node_id = actor_info["Address"]["NodeID"]
                node_id_set.add(node_id)

        assert len(node_id_set) == 2


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_capture_child_tasks(ray_start_cluster_enabled, connect_to_client):
    cluster = ray_start_cluster_enabled
    total_num_tasks = 4
    for _ in range(2):
        cluster.add_node(num_cpus=total_num_tasks, num_gpus=total_num_tasks)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        pg = ray.util.placement_group(
            [
                {
                    "CPU": 2,
                    "GPU": 2,
                },
                {
                    "CPU": 2,
                    "GPU": 2,
                },
            ],
            strategy="STRICT_PACK",
        )
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
            placement_group_capture_child_tasks=True,
        ).remote(1, 0)
        pgs = ray.get(t)
        # Every task should have current placement group because they
        # should be implicitly captured by default.
        assert None not in pgs

        t1 = create_nested_task.options(
            num_cpus=1,
            num_gpus=0,
            placement_group=pg,
            placement_group_capture_child_tasks=True,
        ).remote(1, 0, True)
        pgs = ray.get(t1)
        # Every task should have no placement group since it's set to None.
        # should be implicitly captured by default.
        assert set(pgs) == {None}

        # Test if tasks don't capture child tasks when the option is off.
        t2 = create_nested_task.options(
            num_cpus=0, num_gpus=1, placement_group=pg
        ).remote(0, 1)
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
        p, 1, ray.ray_constants.INFEASIBLE_TASK_ERROR, timeout=0.1
    )
    assert len(errors) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
