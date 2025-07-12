import ray
import pytest


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_get_object_status_rpc_retry_and_idempotency(
    monkeypatch, shutdown_only, deterministic_failure
):
    """Test that GetObjectStatus RPC retries work correctly.
    Verify that the RPC is idempotent when network failures occur.
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
        """Task that accesses objects from other workers - triggers GetObjectStatus RPC"""
        data = ray.get(objects)
        return data

    futures = [test_task.remote(i) for i in range(5)]

    # This triggers GetObjectStatus RPC calls when cross_worker_access_task
    # tries to get objects from other workers
    result_future = cross_worker_access_task.remote(futures)

    # Wait for the cross-worker object access to complete
    # This exercises the retry logic when RPC failures occur
    final_result = ray.get(result_future)

    # Verify the results are correct despite network failures
    assert final_result == [0, 2, 4, 6, 8]

    ray.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
