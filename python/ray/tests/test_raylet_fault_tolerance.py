import ray
import time
import pytest
import sys


@ray.remote
def simple_task():
    time.sleep(1)


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_return_worker_lease_idempotent(
    monkeypatch, shutdown_only, deterministic_failure
):
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.ReturnWorkerLease=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )
    ray.init(num_cpus=1)
    # Testing ReturnWorkerLease idempotency issue mentioned in issue #55469
    _ = simple_task.remote()
    _ = simple_task.remote()
    result_ref3 = simple_task.remote()
    ray.get(result_ref3)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
