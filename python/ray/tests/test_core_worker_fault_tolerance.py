import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.core.generated.common_pb2 import ActorDiedErrorContext


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_get_object_status_rpc_retry_and_idempotency(
    monkeypatch, shutdown_only, deterministic_failure
):
    """Test that GetObjectStatus RPC retries work correctly.
    Verify that the RPC is idempotent when network failures occur.
    Cross_worker_access_task triggers GetObjectStatus because it does
    not own objects and needs to request it from the driver.
    """

    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "CoreWorkerService.grpc_client.GetObjectStatus=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )

    ray.init()

    @ray.remote
    def test_task(i):
        return i * 2

    @ray.remote
    def cross_worker_access_task(objects):
        data = ray.get(objects)
        return data

    object_refs = [test_task.remote(i) for i in range(5)]
    result_object_ref = cross_worker_access_task.remote(object_refs)
    final_result = ray.get(result_object_ref)
    assert final_result == [0, 2, 4, 6, 8]


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_wait_for_actor_ref_deleted_rpc_retry_and_idempotency(
    monkeypatch, shutdown_only, deterministic_failure
):
    """Test that WaitForActorRefDeleted RPC retries work correctly.
    Verify that the RPC is idempotent when network failures occur.
    The GCS actor manager will trigger this RPC during actor initialization
    to monitor when the actor handles have gone out of scope and the actor should be destroyed.Collapse commentComment on line R46jjyao commented on Oct 6, 2025 jjyaoon Oct 6, 2025CollaboratorMore actions
    """

    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "CoreWorkerService.grpc_client.WaitForActorRefDeleted=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )

    ray.init()

    @ray.remote
    class SimpleActor:
        def ping(self):
            return "pong"

    actor = SimpleActor.options(name="test_actor").remote()
    actor_id = actor._actor_id.hex()

    result = ray.get(actor.ping.remote())
    assert result == "pong"

    del actor

    def check_actor_dead():
        actor_info = ray._private.state.actors(actor_id=actor_id)
        if not actor_info or actor_info.get("State") != "DEAD":
            return False

        death_cause = actor_info.get("DeathCause")
        actor_died_error = death_cause.actor_died_error_context
        return actor_died_error.reason == ActorDiedErrorContext.OUT_OF_SCOPE

    wait_for_condition(check_actor_dead, timeout=30)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
