from ray.tests.conftest import *  # noqa

import pytest
from ray import workflow


@workflow.step
def check_and_update(x, worker_id):
    from ray.worker import global_worker

    _worker_id = global_worker.worker_id
    if worker_id == _worker_id:
        return x + "0"
    return x + "1"


@workflow.step
def inplace_test():
    from ray.worker import global_worker

    worker_id = global_worker.worker_id
    x = check_and_update.options(allow_inplace=True).step("@", worker_id)
    y = check_and_update.step(x, worker_id)
    z = check_and_update.options(allow_inplace=True).step(y, worker_id)
    return z


@workflow.step
def exp_inplace(k, n, worker_id=None):
    from ray.worker import global_worker

    _worker_id = global_worker.worker_id
    if worker_id is not None:
        # sub-workflows running inplace
        assert _worker_id == worker_id
    worker_id = _worker_id

    if n == 0:
        return k
    return exp_inplace.options(allow_inplace=True).step(2 * k, n - 1, worker_id)


@workflow.step
def exp_remote(k, n, worker_id=None):
    from ray.worker import global_worker

    _worker_id = global_worker.worker_id
    if worker_id is not None:
        # sub-workflows running in another worker
        assert _worker_id != worker_id
    worker_id = _worker_id

    if n == 0:
        return k
    return exp_remote.step(2 * k, n - 1, worker_id)


def test_inplace_workflows(workflow_start_regular_shared):
    assert inplace_test.step().run() == "@010"

    k, n = 12, 10
    assert exp_inplace.step(k, n).run() == k * 2 ** n
    assert exp_remote.step(k, n).run() == k * 2 ** n


def test_tail_recursion_optimization(workflow_start_regular_shared):
    @workflow.step
    def tail_recursion(n):
        import inspect

        # check if the stack is growing
        assert len(inspect.stack(0)) < 20
        if n <= 0:
            return "ok"
        return tail_recursion.options(allow_inplace=True).step(n - 1)

    tail_recursion.step(30).run()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
