import sys

import numpy as np
import pytest

import ray
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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
