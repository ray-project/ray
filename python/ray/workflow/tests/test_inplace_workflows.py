import ray
from ray.tests.conftest import *  # noqa

import pytest
from ray import workflow


@ray.remote
def check_and_update(x, worker_id):
    from ray.worker import global_worker

    _worker_id = global_worker.worker_id
    if worker_id == _worker_id:
        return x + "0"
    return x + "1"


@ray.remote
def inplace_test():
    from ray.worker import global_worker

    worker_id = global_worker.worker_id
    x = check_and_update.options(**workflow.options(allow_inplace=True)).bind(
        "@", worker_id
    )
    y = check_and_update.bind(x, worker_id)
    z = check_and_update.options(**workflow.options(allow_inplace=True)).bind(
        y, worker_id
    )
    return workflow.continuation(z)


@ray.remote
def exp_inplace(k, n, worker_id=None):
    from ray.worker import global_worker

    _worker_id = global_worker.worker_id
    if worker_id is not None:
        # sub-workflows running inplace
        assert _worker_id == worker_id
    worker_id = _worker_id

    if n == 0:
        return k
    return workflow.continuation(
        exp_inplace.options(**workflow.options(allow_inplace=True)).bind(
            2 * k, n - 1, worker_id
        )
    )


@ray.remote
def exp_remote(k, n, worker_id=None):
    from ray.worker import global_worker

    _worker_id = global_worker.worker_id
    if worker_id is not None:
        # sub-workflows running in another worker
        assert _worker_id != worker_id
    worker_id = _worker_id

    if n == 0:
        return k
    return workflow.continuation(exp_remote.bind(2 * k, n - 1, worker_id))


def test_inplace_workflows(workflow_start_regular_shared):
    assert workflow.create(inplace_test.bind()).run() == "@010"

    k, n = 12, 10
    assert workflow.create(exp_inplace.bind(k, n)).run() == k * 2 ** n
    assert workflow.create(exp_remote.bind(k, n)).run() == k * 2 ** n


def test_tail_recursion_optimization(workflow_start_regular_shared):
    @ray.remote
    def tail_recursion(n):
        import inspect

        # check if the stack is growing
        assert len(inspect.stack(0)) < 20
        if n <= 0:
            return "ok"
        return workflow.continuation(
            tail_recursion.options(**workflow.options(allow_inplace=True)).bind(n - 1)
        )

    workflow.create(tail_recursion.bind(30)).run()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
