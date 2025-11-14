import sys

import numpy as np
import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.core.generated import common_pb2, gcs_pb2
from ray.exceptions import GetTimeoutError, TaskCancelledError
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@pytest.mark.parametrize(
    "allow_out_of_order_execution",
    [True, False],
)
@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_push_actor_task_failure(
    monkeypatch,
    ray_start_cluster,
    allow_out_of_order_execution: bool,
    deterministic_failure: str,
):
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_rpc_failure",
            "CoreWorkerService.grpc_client.PushTask=2:"
            + ("100:0" if deterministic_failure == "request" else "0:100"),
        )
        m.setenv("RAY_actor_scheduling_queue_max_reorder_wait_seconds", "0")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address)

        @ray.remote(
            max_task_retries=-1,
            allow_out_of_order_execution=allow_out_of_order_execution,
        )
        class RetryActor:
            def echo(self, value):
                return value

        refs = []
        actor = RetryActor.remote()
        for i in range(10):
            refs.append(actor.echo.remote(i))
        assert ray.get(refs) == list(range(10))


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_update_object_location_batch_failure(
    monkeypatch, ray_start_cluster, deterministic_failure
):
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_rpc_failure",
            "CoreWorkerService.grpc_client.UpdateObjectLocationBatch=1:"
            + ("100:0" if deterministic_failure == "request" else "0:100"),
        )
        cluster = ray_start_cluster
        head_node_id = cluster.add_node(
            num_cpus=0,
        ).node_id
        ray.init(address=cluster.address)
        worker_node_id = cluster.add_node(num_cpus=1).node_id

        @ray.remote(num_cpus=1)
        def create_large_object():
            return np.zeros(100 * 1024 * 1024, dtype=np.uint8)

        @ray.remote(num_cpus=0)
        def consume_large_object(obj):
            return sys.getsizeof(obj)

        obj_ref = create_large_object.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=worker_node_id, soft=False
            )
        ).remote()
        consume_ref = consume_large_object.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        ).remote(obj_ref)
        assert ray.get(consume_ref, timeout=10) > 0


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
    to monitor when the actor handles have gone out of scope and the actor should be destroyed.
    """

    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "CoreWorkerService.grpc_client.WaitForActorRefDeleted=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )

    ray.init()

    @ray.remote(max_restarts=1)
    class SimpleActor:
        def ping(self):
            return "pong"

    actor = SimpleActor.remote()

    result = ray.get(actor.ping.remote())
    assert result == "pong"

    actor_id = actor._actor_id
    del actor

    def verify_actor_ref_deleted():
        actor_info = ray._private.state.state.get_actor_info(actor_id)
        if actor_info is None:
            return False
        actor_info = gcs_pb2.ActorTableData.FromString(actor_info)
        return (
            actor_info.state == gcs_pb2.ActorTableData.ActorState.DEAD
            and actor_info.death_cause.actor_died_error_context.reason
            == common_pb2.ActorDiedErrorContext.Reason.REF_DELETED
        )

    wait_for_condition(verify_actor_ref_deleted, timeout=30)


@pytest.fixture
def inject_cancel_remote_task_rpc_failure(monkeypatch, request):
    deterministic_failure = request.param
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "CoreWorkerService.grpc_client.CancelRemoteTask=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )


@pytest.mark.parametrize(
    "inject_cancel_remote_task_rpc_failure", ["request", "response"], indirect=True
)
def test_cancel_remote_task_rpc_retry_and_idempotency(
    inject_cancel_remote_task_rpc_failure, ray_start_cluster
):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"worker1": 1})
    cluster.add_node(num_cpus=1, resources={"worker2": 1})
    signaler = SignalActor.remote()

    @ray.remote(num_cpus=1, resources={"worker1": 1})
    def wait_for(y):
        return ray.get(y[0])

    @ray.remote(num_cpus=1, resources={"worker2": 1})
    def remote_wait(sg):
        return [wait_for.remote([sg[0]])]

    sig = signaler.wait.remote()

    outer = remote_wait.remote([sig])
    inner = ray.get(outer)[0]
    with pytest.raises(GetTimeoutError):
        ray.get(inner, timeout=1)
    ray.cancel(inner)
    with pytest.raises(TaskCancelledError):
        ray.get(inner, timeout=10)


def test_double_borrowing_with_rpc_failure(monkeypatch, shutdown_only):
    """Regression test for https://github.com/ray-project/ray/issues/57997"""
    monkeypatch.setenv(
        "RAY_testing_rpc_failure", "CoreWorkerService.grpc_client.PushTask=3:0:100"
    )

    ray.init()

    @ray.remote(max_task_retries=-1, max_restarts=-1)
    class Actor:
        def __init__(self, objs):
            # Actor is a borrower of obj
            self.obj = objs[0]

        def test(self):
            # Return the borrowed object inside the list
            # so the caller is a borrower as well.
            # This actor task will be retried since
            # the first PushTask RPC response will be lost.
            return [self.obj]

    obj = ray.put(31)
    actor = Actor.remote([obj])
    result = ray.get(actor.test.remote())
    assert ray.get(result[0]) == 31


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
