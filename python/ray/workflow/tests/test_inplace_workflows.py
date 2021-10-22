from ray.tests.conftest import *  # noqa

import pytest
from ray import workflow


@workflow.step
def mul(a, b):
    return a * b


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
    return exp_inplace.step(2 * k, n - 1, worker_id)


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
    # Force remote by acquiring different resources.
    return exp_remote.options(num_cpus=1.0 - 0.001 * n).step(
        2 * k, n - 1, worker_id)


def test_inplace_workflows(workflow_start_regular_shared):
    k, n = 12, 10
    assert exp_inplace.step(k, n).run() == 12 * 2**10
    assert exp_remote.options(num_cpus=1).step(k, n).run() == 12 * 2**10


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
