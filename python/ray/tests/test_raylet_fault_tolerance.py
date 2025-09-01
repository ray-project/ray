import ray
import pytest
import sys

# Not testing response as HandleReturnWorkerLease is clearly idempotent and
# testing the edge case mentioned in #55469 makes the test brittle since a
# minor change in the implementation/config variables means the edge case is not tested


def test_return_worker_lease_retry_and_idempotency(monkeypatch, shutdown_only):
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.ReturnWorkerLease=1:100:0",
    )
    ray.init(
        num_cpus=1,
    )

    # Create two functions with different names to ensure the scheduler class changes
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
