import gc
import os
import signal
import sys

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.core.generated import common_pb2, gcs_pb2


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_actor_reconstruction_triggered_by_lineage_reconstruction(
    monkeypatch, ray_start_cluster, deterministic_failure
):
    # Test the sequence of events:
    # actor goes out of scope and killed
    # -> lineage reconstruction triggered by object lost
    # -> actor is restarted
    # -> actor goes out of scope again after lineage reconstruction is done
    # -> actor is permanently dead when there is no reference.
    # This test also injects network failure to make sure relevant rpcs are retried.
    chaos_failure = "100:0" if deterministic_failure == "request" else "0:100"
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        f"ray::rpc::ActorInfoGcsService.grpc_client.RestartActorForLineageReconstruction=1:{chaos_failure},"
        f"ray::rpc::ActorInfoGcsService.grpc_client.ReportActorOutOfScope=1:{chaos_failure}",
    )
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

        def pid(self):
            return os.getpid()

    actor = Actor.remote()
    actor_id = actor._actor_id

    obj1 = actor.ping.remote()
    os.kill(ray.get(actor.pid.remote()), signal.SIGKILL)

    # obj2 should be ready after actor is restarted
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
    sys.exit(pytest.main(["-sv", __file__]))
