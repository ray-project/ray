import sys

import pytest

import ray


def test_double_borrowing_with_network_failure(monkeypatch, shutdown_only):
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
    sys.exit(pytest.main(["-sv", __file__]))
