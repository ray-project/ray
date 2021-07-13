import time

import pytest

import ray
from ray.experimental import workflow
from ray.experimental.workflow import workflow_access


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


def test_basic_workflows():
    ray.init(namespace="workflow")

    output = workflow.run(simple_sequential.step())
    assert ray.get(output) == "[source1][append1][append2]"

    output = workflow.run(simple_sequential_with_input.step("start:"))
    assert ray.get(output) == "start:[append1][append2]"

    output = workflow.run(loop_sequential.step(3))
    assert ray.get(output) == "[source1]" + "[append1]" * 3 + "[append2]"

    output = workflow.run(nested.step("nested:"))
    assert ray.get(output) == "nested:~[nested]~[append1][append2]"

    output = workflow.run(fork_join.step())
    assert ray.get(output) == "join([source1][append1], [source1][append2])"

    ray.shutdown()


def test_async_execution():
    ray.init(namespace="workflow")

    start = time.time()
    output = workflow.run(blocking.step())
    duration = time.time() - start
    assert duration < 5  # workflow.run is not blocked
    assert ray.get(output) == 314

    ray.shutdown()


@ray.remote
def deep_nested(x):
    if x >= 42:
        return x
    return deep_nested.remote(x + 1)


def _resolve_workflow_output(workflow_id: str, output: ray.ObjectRef):
    while isinstance(output, ray.ObjectRef):
        output = ray.get(output)
    return output


def test_workflow_output_resolving():
    ray.init(namespace="workflow")
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
    ray.shutdown()


def test_run_or_resume_during_running():
    ray.init(namespace="workflow")
    output = workflow.run(
        simple_sequential.step(), workflow_id="running_workflow")

    with pytest.raises(ValueError):
        workflow.run(simple_sequential.step(), workflow_id="running_workflow")
    with pytest.raises(ValueError):
        workflow.resume(workflow_id="running_workflow")

    assert ray.get(output) == "[source1][append1][append2]"
    ray.shutdown()
