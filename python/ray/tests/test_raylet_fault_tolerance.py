import ray
import pytest
import sys

# Not testing response failure as HandleReturnWorkerLease idempotency is tested
# with a C++ unit test and testing the response failure case mentioned in #55469
# requires setting multiple env variables + specific timing, making the test brittle
def test_return_worker_lease_retry_and_idempotency(monkeypatch, shutdown_only):
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.ReturnWorkerLease=1:100:0",
    )
    ray.init(
        num_cpus=1,
    )

    # Create two functions with different names to ensure the SchedulingKey changes
    # and a new lease request is sent for each call
    @ray.remote
    def simple_task_1():
        return 0

    @ray.remote
    def simple_task_2():
        return 0

    _ = simple_task_1.remote()
    result_ref2 = simple_task_2.remote()
    assert ray.get(result_ref2) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
