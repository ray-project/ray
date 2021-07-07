import time

import ray
from ray.experimental import workflow
from ray.experimental.workflow.workflow_access import flatten_workflow_output


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
    ray.init()

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
    ray.init()

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


def test_workflow_output_resolving():
    ray.init()
    nested_ref = deep_nested.remote(30)
    ref = flatten_workflow_output("fake_workflow_id", nested_ref)
    assert ray.get(ref) == 42
    ray.shutdown()
