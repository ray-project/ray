from ray.tests.conftest import *  # noqa

import time
import pytest
import ray
from ray import workflow
from ray.workflow.common import Workflow
from ray.workflow.tests import utils
from ray.exceptions import RaySystemError


@workflow.step
def wait_multiple_steps():
    @workflow.step
    def sleep_identity(x: int):
        time.sleep(x)
        return x

    ws = [
        sleep_identity.step(1),
        sleep_identity.step(3),
        sleep_identity.step(10),
        sleep_identity.step(2),
        sleep_identity.step(12),
    ]
    return workflow.wait(ws, num_returns=4, timeout=5)


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 8
            # We need more CPUs, otherwise task execution could be blocked.
        }
    ],
    indirect=True,
)
def test_wait_basics(workflow_start_regular_shared):
    # This tests basic usage of 'workflow.wait':
    # 1. It returns ready tasks precisely and preserves the original order.
    # 2. All steps would see the same waiting result.
    # 3. We can pass remaining pending workflows to another workflow,
    #    and they can be resolved like normal workflows.
    @workflow.step
    def return_ready(wait_results):
        ready, unready = wait_results
        return ready

    @workflow.step
    def join(a, b):
        return a, b

    wait_result = wait_multiple_steps.step()
    a = return_ready.step(wait_result)
    b = return_ready.step(wait_result)
    ready1, ready2 = join.step(a, b).run()
    assert ready1 == ready2 == [1, 3, 2]

    @workflow.step
    def get_all(ready, unready):
        return ready, unready

    @workflow.step
    def filter_all(wait_results):
        ready, unready = wait_results
        return get_all.step(ready, unready)

    @workflow.step
    def composite():
        w = wait_multiple_steps.step()
        return filter_all.step(w)

    ready, unready = composite.step().run()
    assert ready == [1, 3, 2]
    assert unready == [10, 12]

    with pytest.raises(TypeError):
        workflow.wait([1, 2])


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 8
            # We need more CPUs, otherwise task execution could be blocked.
        }
    ],
    indirect=True,
)
def test_wait_basics_2(workflow_start_regular_shared):
    # Test "workflow.wait" running in the top level DAG,
    # or running "workflow.wait" directly.
    @workflow.step
    def sleep_identity(x: int):
        time.sleep(x)
        return x

    @workflow.step
    def identity(x):
        return x

    ws = [
        sleep_identity.step(1),
        sleep_identity.step(5),
        sleep_identity.step(2),
    ]
    w = workflow.wait(ws, num_returns=2, timeout=3)
    ready, remaining = identity.step(w).run()
    assert ready == [1, 2]

    ws = [
        sleep_identity.step(2),
        sleep_identity.step(5),
        sleep_identity.step(1),
    ]
    w = workflow.wait(ws, num_returns=2, timeout=3)
    ready, remaining = w.run()
    assert ready == [2, 1]


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 8
            # We need more CPUs, otherwise task execution could be blocked.
        }
    ],
    indirect=True,
)
def test_wait_recursive(workflow_start_regular_shared):
    # This tests that we can 'workflow.wait' the remaining pending workflow
    # returned by another 'workflow.wait' recursively.
    w = wait_multiple_steps.step()

    @workflow.step
    def recursive_wait(s):
        ready, unready = s
        if len(unready) == 2 and not isinstance(unready[0], Workflow):
            ready_2, unready = unready
            print(ready, (ready_2, unready))
            ready += ready_2

        if not unready:
            return ready
        w = workflow.wait(unready)
        return recursive_wait.step([ready, w])

    assert recursive_wait.step(w).run() == [1, 3, 2, 10, 12]


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 8
            # We need more CPUs, otherwise task execution could be blocked.
        }
    ],
    indirect=True,
)
def test_wait_failure_recovery_1(workflow_start_regular_shared):
    # This tests that if a step using the output of "workflow.wait" as its
    # input, it can be recovered after failure.
    @workflow.step
    def get_all(ready, unready):
        return ready, unready

    @workflow.step
    def filter_all_2(wait_results):
        assert wait_results[0] == [1, 3, 2]
        # failure point
        assert utils.check_global_mark()
        ready, unready = wait_results
        return get_all.step(ready, unready)

    @workflow.step
    def composite_2():
        w = wait_multiple_steps.step()
        return filter_all_2.step(w)

    utils.unset_global_mark()

    with pytest.raises(RaySystemError):
        composite_2.step().run(workflow_id="wait_failure_recovery")

    utils.set_global_mark()

    ready, unready = ray.get(workflow.resume("wait_failure_recovery"))
    assert ready == [1, 3, 2]
    assert unready == [10, 12]


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 8
            # We need more CPUs, otherwise task execution could be blocked.
        }
    ],
    indirect=True,
)
def test_wait_failure_recovery_2(workflow_start_regular_shared):
    # Test failing "workflow.wait" and its input steps.

    @workflow.step
    def sleep_identity(x: int):
        # block the step by a global mark
        while not utils.check_global_mark():
            time.sleep(0.1)
        time.sleep(x)
        return x

    @workflow.step
    def identity(x):
        return x

    ws = [
        sleep_identity.step(2),
        sleep_identity.step(5),
        sleep_identity.step(1),
    ]
    w = workflow.wait(ws, num_returns=2, timeout=None)
    utils.unset_global_mark()
    _ = identity.step(w).run_async(workflow_id="wait_failure_recovery_2")
    # wait util "workflow.wait" has been running
    time.sleep(10)
    workflow.cancel("wait_failure_recovery_2")
    time.sleep(2)

    utils.set_global_mark()
    ready, unready = ray.get(workflow.resume("wait_failure_recovery_2"))
    assert ready == [2, 1]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
