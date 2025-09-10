import pytest

import ray


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
