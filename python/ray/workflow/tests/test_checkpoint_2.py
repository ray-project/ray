import ray
import time
import pytest
from ray.tests.conftest import *  # noqa

import numpy as np
from ray import workflow
from ray.workflow.tests import utils
from ray.exceptions import RaySystemError


SIZE = 2 ** 15


@ray.remote
def checkpoint_dag(checkpoint):
    @ray.remote
    def large_input():
        return np.arange(SIZE)

    @ray.remote
    def identity(x):
        if not utils.check_global_mark():
            import os

            os.kill(os.getpid(), 9)
        return x

    @ray.remote
    def average(x):
        return np.mean(x)

    x = large_input.options(**workflow.options(checkpoint=checkpoint)).bind()
    y = identity.options(**workflow.options(checkpoint=checkpoint)).bind(x)
    return workflow.continuation(average.bind(y))


def test_checkpoint_dag_recovery_skip(workflow_start_regular_shared):
    utils.unset_global_mark()

    start = time.time()
    with pytest.raises(RaySystemError):
        workflow.create(
            checkpoint_dag.options(**workflow.options(checkpoint=False)).bind(False)
        ).run(workflow_id="checkpoint_skip_recovery")
    run_duration_skipped = time.time() - start

    utils.set_global_mark()

    start = time.time()
    recovered = ray.get(workflow.resume("checkpoint_skip_recovery"))
    recover_duration_skipped = time.time() - start
    assert np.isclose(recovered, np.arange(SIZE).mean())

    print(
        f"[skipped] run_duration = {run_duration_skipped}, "
        f"recover_duration = {recover_duration_skipped}"
    )


def test_checkpoint_dag_recovery_partial(workflow_start_regular_shared):
    utils.unset_global_mark()

    start = time.time()
    with pytest.raises(RaySystemError):
        workflow.create(checkpoint_dag.bind(False)).run(
            workflow_id="checkpoint_partial_recovery"
        )
    run_duration_partial = time.time() - start

    utils.set_global_mark()

    start = time.time()
    recovered = ray.get(workflow.resume("checkpoint_partial_recovery"))
    recover_duration_partial = time.time() - start
    assert np.isclose(recovered, np.arange(SIZE).mean())
    print(
        f"[partial] run_duration = {run_duration_partial}, "
        f"recover_duration = {recover_duration_partial}"
    )


def test_checkpoint_dag_recovery_whole(workflow_start_regular_shared):
    utils.unset_global_mark()

    start = time.time()
    with pytest.raises(RaySystemError):
        workflow.create(checkpoint_dag.bind(True)).run(
            workflow_id="checkpoint_whole_recovery"
        )
    run_duration_whole = time.time() - start

    utils.set_global_mark()

    start = time.time()
    recovered = ray.get(workflow.resume("checkpoint_whole_recovery"))
    recover_duration_whole = time.time() - start
    assert np.isclose(recovered, np.arange(SIZE).mean())

    print(
        f"[whole] run_duration = {run_duration_whole}, "
        f"recover_duration = {recover_duration_whole}"
    )


@pytest.mark.skip(
    reason=(
        "Currently it is not clear how and if we need to check"
        "side effects of skipping checkpointing, e.g., the"
        "violation of exactly-once execution guarantee of workflow."
    )
)
def test_checkpoint_dag_validation(workflow_start_regular):
    @ray.remote
    def identity(x):
        return x

    @ray.remote
    def average(x):
        return np.mean(x)

    @workflow.step
    def valid_checkpoint_dag_1():
        y = identity.options(checkpoint=False).step(42)
        return average.options(checkpoint=True).step(y)

    @workflow.step
    def invalid_checkpoint_dag_1():
        y = identity.options(checkpoint=True).step(42)
        return average.options(checkpoint=True).step(y)

    @workflow.step
    def invalid_checkpoint_dag_2():
        y = valid_checkpoint_dag_1.options(checkpoint=False).step()
        return average.options(checkpoint=True).step(y)

    valid_checkpoint_dag_1.options(checkpoint=False).step().run()
    # check invalid configuration
    with pytest.raises(RaySystemError):
        invalid_checkpoint_dag_1.options(checkpoint=False).step().run()
    # check invalid configuration
    with pytest.raises(RaySystemError):
        invalid_checkpoint_dag_2.options(checkpoint=False).step().run()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
