import os
import gc
import sys

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.core.generated import gcs_pb2
from ray.core.generated import common_pb2


def test_actor_reconstruction_triggered_by_lineage_reconstruction(ray_start_cluster):
    # Test the sequence of events:
    # actor goes out of scope and killed
    # -> lineage reconstruction triggered by object lost
    # -> actor is restarted
    # -> actor goes out of scope again after lineage reconstruction is done
    # -> actor is permanently dead when there is no reference.
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker1 = cluster.add_node(resources={"worker": 1})

    @ray.remote(
        num_cpus=1, resources={"worker": 1}, max_restarts=-1, max_task_retries=-1
    )
    class Actor:
        def ping(self):
            return [1] * 1024 * 1024

    actor = Actor.remote()
    actor_id = actor._actor_id

    obj1 = actor.ping.remote()
    obj2 = actor.ping.remote()

    # Make the actor out of scope
    actor = None

    def verify1():
        gc.collect()
        actor_info = ray._private.state.state.global_state_accessor.get_actor_info(
            actor_id
        )
        assert actor_info is not None
        actor_info = gcs_pb2.ActorTableData.FromString(actor_info)
        assert actor_info.state == gcs_pb2.ActorTableData.ActorState.DEAD
        assert (
            actor_info.death_cause.actor_died_error_context.reason
            == common_pb2.ActorDiedErrorContext.Reason.OUT_OF_SCOPE
        )
        assert actor_info.num_restarts_due_to_lineage_reconstruction == 0
        return True

    wait_for_condition(lambda: verify1())

    # objs will be lost and recovered
    # during the process, actor will be reconstructured
    # and dead again after lineage reconstruction finishes
    cluster.remove_node(worker1)
    cluster.add_node(resources={"worker": 1})

    assert ray.get(obj1) == [1] * 1024 * 1024
    assert ray.get(obj2) == [1] * 1024 * 1024

    def verify2():
        actor_info = ray._private.state.state.global_state_accessor.get_actor_info(
            actor_id
        )
        assert actor_info is not None
        actor_info = gcs_pb2.ActorTableData.FromString(actor_info)
        assert actor_info.state == gcs_pb2.ActorTableData.ActorState.DEAD
        assert (
            actor_info.death_cause.actor_died_error_context.reason
            == common_pb2.ActorDiedErrorContext.Reason.OUT_OF_SCOPE
        )
        # 1 restart recovers two objects
        assert actor_info.num_restarts_due_to_lineage_reconstruction == 1
        return True

    wait_for_condition(lambda: verify2())

    # actor can be permanently dead since no lineage reconstruction will happen
    del obj1
    del obj2

    def verify3():
        actor_info = ray._private.state.state.global_state_accessor.get_actor_info(
            actor_id
        )
        assert actor_info is not None
        actor_info = gcs_pb2.ActorTableData.FromString(actor_info)
        assert actor_info.state == gcs_pb2.ActorTableData.ActorState.DEAD
        assert (
            actor_info.death_cause.actor_died_error_context.reason
            == common_pb2.ActorDiedErrorContext.Reason.REF_DELETED
        )
        assert actor_info.num_restarts_due_to_lineage_reconstruction == 1
        return True

    wait_for_condition(lambda: verify3())


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
