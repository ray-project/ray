import sys

import pytest

import ray


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_cancel_worker_lease_idempotent(
    monkeypatch, shutdown_only, deterministic_failure
):
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.CancelWorkerLease=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )

    @ray.remote
    def simple_task_1():
        return 0

    @ray.remote
    def simple_task_2():
        return 1

    ray.init(num_cpus=1)
    result_ref1 = simple_task_1.remote()
    result_ref2 = simple_task_2.remote()
    ray.cancel(result_ref2)

    assert ray.get(result_ref1) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
