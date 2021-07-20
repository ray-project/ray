import time

from ray.tests.conftest import *  # noqa

import pytest
import ray
from ray.experimental import workflow
from ray.experimental.workflow import workflow_access
from filelock import FileLock


@workflow.step
def identity(x):
    return x


@workflow.step
def source1():
    return "[source1]"


@workflow.step
def append1(x):
    return x + "[append1]"


@workflow.step
def append2(x):
    return x + "[append2]"


@workflow.step
def simple_sequential():
    x = source1.step()
    y = append1.step(x)
    return append2.step(y)


@workflow.step
def simple_sequential_with_input(x):
    y = append1.step(x)
    return append2.step(y)


@workflow.step
def loop_sequential(n):
    x = source1.step()
    for _ in range(n):
        x = append1.step(x)
    return append2.step(x)


@workflow.step
def nested_step(x):
    return append2.step(append1.step(x + "~[nested]~"))


@workflow.step
def nested(x):
    return nested_step.step(x)


@workflow.step
def join(x, y):
    return f"join({x}, {y})"


@workflow.step
def fork_join():
    x = source1.step()
    y = append1.step(x)
    y = identity.step(y)
    z = append2.step(x)
    return join.step(y, z)


@workflow.step
def blocking():
    time.sleep(10)
    return 314


@workflow.step
def mul(a, b):
    return a * b


@workflow.step
def factorial(n):
    if n == 1:
        return 1
    else:
        return mul.step(n, factorial.step(n - 1))


@pytest.mark.parametrize(
    "ray_start_regular_shared", [{
        "namespace": "workflow"
    }], indirect=True)
def test_basic_workflows(ray_start_regular_shared):
    # This test also shows different "style" of running workflows.
    assert simple_sequential.step().run() == "[source1][append1][append2]"

    wf = simple_sequential_with_input.step("start:")
    assert wf.run() == "start:[append1][append2]"

    wf = loop_sequential.step(3)
    assert wf.run() == "[source1]" + "[append1]" * 3 + "[append2]"

    wf = nested.step("nested:")
    assert wf.run() == "nested:~[nested]~[append1][append2]"

    wf = fork_join.step()
    assert wf.run() == "join([source1][append1], [source1][append2])"

    assert factorial.step(10).run() == 3628800


@pytest.mark.parametrize(
    "ray_start_regular_shared", [{
        "namespace": "workflow"
    }], indirect=True)
def test_async_execution(ray_start_regular_shared):
    start = time.time()
    output = blocking.step().run_async()
    duration = time.time() - start
    assert duration < 5  # workflow.run is not blocked
    assert ray.get(output) == 314


@ray.remote
def deep_nested(x):
    if x >= 42:
        return x
    return deep_nested.remote(x + 1)


def _resolve_workflow_output(workflow_id: str, output: ray.ObjectRef):
    while isinstance(output, ray.ObjectRef):
        output = ray.get(output)
    return output


@pytest.mark.parametrize(
    "ray_start_regular_shared", [{
        "namespace": "workflow"
    }], indirect=True)
def test_workflow_output_resolving(ray_start_regular_shared):
    # deep nested workflow
    nested_ref = deep_nested.remote(30)
    original_func = workflow_access._resolve_workflow_output
    # replace the original function with a new function that does not
    # involving named actor
    workflow_access._resolve_workflow_output = _resolve_workflow_output
    try:
        ref = workflow_access.flatten_workflow_output("fake_workflow_id",
                                                      nested_ref)
    finally:
        # restore the function
        workflow_access._resolve_workflow_output = original_func
    assert ray.get(ref) == 42


@pytest.mark.parametrize(
    "ray_start_regular_shared", [{
        "namespace": "workflow"
    }], indirect=True)
def test_run_or_resume_during_running(ray_start_regular_shared):
    output = simple_sequential.step().run_async(workflow_id="running_workflow")
    with pytest.raises(ValueError):
        simple_sequential.step().run_async(workflow_id="running_workflow")
    with pytest.raises(ValueError):
        workflow.resume(workflow_id="running_workflow")
    assert ray.get(output) == "[source1][append1][append2]"


@pytest.mark.parametrize(
    "ray_start_regular_shared", [{
        "namespace": "workflow"
    }], indirect=True)
def test_list_all(ray_start_regular_shared, tmp_path):
    tmp_file = str(tmp_path / "lock")
    lock = FileLock(tmp_file)
    lock.acquire()

    @workflow.step
    def long_running():
        with FileLock(tmp_file):
            return

    [long_running.step().run_async(workflow_id=str(i)) for i in range(100)]
    all_tasks = workflow.list_all()
    assert len(all_tasks) == 100
    print("!", all_tasks)
    # all_tasks_running = workflow.list_all(workflow.WorkflowStatus.RUNNING)
    # assert all_tasks == all_tasks_running
