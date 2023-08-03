import time

from ray.tests.conftest import *  # noqa

from filelock import FileLock
import pytest
import ray
from ray import workflow


def test_basic_workflows(workflow_start_regular_shared):
    @ray.remote
    def source1():
        return "[source1]"

    @ray.remote
    def append1(x):
        return x + "[append1]"

    @ray.remote
    def append2(x):
        return x + "[append2]"

    @ray.remote
    def simple_sequential():
        x = source1.bind()
        y = append1.bind(x)
        return workflow.continuation(append2.bind(y))

    @ray.remote
    def identity(x):
        return x

    @ray.remote
    def simple_sequential_with_input(x):
        y = append1.bind(x)
        return workflow.continuation(append2.bind(y))

    @ray.remote
    def loop_sequential(n):
        x = source1.bind()
        for _ in range(n):
            x = append1.bind(x)
        return workflow.continuation(append2.bind(x))

    @ray.remote
    def nested_task(x):
        return workflow.continuation(append2.bind(append1.bind(x + "~[nested]~")))

    @ray.remote
    def nested(x):
        return workflow.continuation(nested_task.bind(x))

    @ray.remote
    def join(x, y):
        return f"join({x}, {y})"

    @ray.remote
    def fork_join():
        x = source1.bind()
        y = append1.bind(x)
        y = identity.bind(y)
        z = append2.bind(x)
        return workflow.continuation(join.bind(y, z))

    @ray.remote
    def mul(a, b):
        return a * b

    @ray.remote
    def factorial(n):
        if n == 1:
            return 1
        else:
            return workflow.continuation(mul.bind(n, factorial.bind(n - 1)))

    # This test also shows different "style" of running workflows.
    assert workflow.run(simple_sequential.bind()) == "[source1][append1][append2]"

    wf = simple_sequential_with_input.bind("start:")
    assert workflow.run(wf) == "start:[append1][append2]"

    wf = loop_sequential.bind(3)
    assert workflow.run(wf) == "[source1]" + "[append1]" * 3 + "[append2]"

    wf = nested.bind("nested:")
    assert workflow.run(wf) == "nested:~[nested]~[append1][append2]"

    wf = fork_join.bind()
    assert workflow.run(wf) == "join([source1][append1], [source1][append2])"

    assert workflow.run(factorial.bind(10)) == 3628800


def test_async_execution(workflow_start_regular_shared):
    @ray.remote
    def blocking():
        time.sleep(10)
        return 314

    start = time.time()
    output = workflow.run_async(blocking.bind())
    duration = time.time() - start
    assert duration < 5  # workflow.run is not blocked
    assert ray.get(output) == 314


@pytest.mark.skip(reason="Ray DAG does not support partial")
def test_partial(workflow_start_regular_shared):
    ys = [1, 2, 3]

    def add(x, y):
        return x + y

    from functools import partial

    f1 = workflow.task(partial(add, 10)).task(10)

    assert "__anonymous_func__" in f1._name
    assert f1.run() == 20

    fs = [partial(add, y=y) for y in ys]

    @ray.remote
    def chain_func(*args, **kw_argv):
        # Get the first function as a start
        wf_task = workflow.task(fs[0]).task(*args, **kw_argv)
        for i in range(1, len(fs)):
            # Convert each function inside tasks into workflow task
            # function and then use the previous output as the input
            # for them.
            wf_task = workflow.task(fs[i]).task(wf_task)
        return wf_task

    assert workflow.run(chain_func.bind(1)) == 7


def test_run_or_resume_during_running(workflow_start_regular_shared, tmp_path):
    @ray.remote
    def source1():
        return "[source1]"

    @ray.remote
    def append1(x):
        return x + "[append1]"

    @ray.remote
    def append2(x):
        return x + "[append2]"

    @ray.remote
    def simple_sequential():
        with FileLock(tmp_path / "lock"):
            x = source1.bind()
            y = append1.bind(x)
            return workflow.continuation(append2.bind(y))

    with FileLock(tmp_path / "lock"):
        output = workflow.run_async(
            simple_sequential.bind(), workflow_id="running_workflow"
        )
        with pytest.raises(RuntimeError):
            workflow.run_async(simple_sequential.bind(), workflow_id="running_workflow")
        with pytest.raises(RuntimeError):
            workflow.resume_async(workflow_id="running_workflow")
    assert ray.get(output) == "[source1][append1][append2]"


def test_dynamic_output(workflow_start_regular_shared):
    @ray.remote
    def exponential_fail(k, n):
        if n > 0:
            if n < 3:
                raise Exception("Failed intentionally")
            return workflow.continuation(
                exponential_fail.options(**workflow.options(task_id=f"task_{n}")).bind(
                    k * 2, n - 1
                )
            )
        return k

    # When workflow fails, the dynamic output should points to the
    # latest successful task.
    try:
        workflow.run(
            exponential_fail.options(**workflow.options(task_id="task_0")).bind(3, 10),
            workflow_id="dynamic_output",
        )
    except Exception:
        pass
    from ray.workflow.workflow_storage import get_workflow_storage

    from ray._private.client_mode_hook import client_mode_wrap

    @client_mode_wrap
    def _check_storage():
        wf_storage = get_workflow_storage(workflow_id="dynamic_output")
        result = wf_storage.inspect_task("task_0")
        return result.output_task_id

    assert _check_storage() == "task_3"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
