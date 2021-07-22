import time
from filelock import FileLock

from ray.tests.conftest import *  # noqa
from ray.test_utils import run_string_as_driver

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


@workflow.step
def mul(a, b):
    return a * b


@workflow.step
def factorial(n):
    if n == 1:
        return 1
    else:
        return mul.step(n, factorial.step(n - 1))


def test_basic_workflows(workflow_start_regular_shared):
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


def test_async_execution(workflow_start_regular_shared):
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


def test_workflow_output_resolving(workflow_start_regular_shared):
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


def test_run_or_resume_during_running(workflow_start_regular_shared):
    output = simple_sequential.step().run_async(workflow_id="running_workflow")
    with pytest.raises(ValueError):
        simple_sequential.step().run_async(workflow_id="running_workflow")
    with pytest.raises(ValueError):
        workflow.resume(workflow_id="running_workflow")
    assert ray.get(output) == "[source1][append1][append2]"


def test_step_failure(workflow_start_regular_shared, tmp_path):
    (tmp_path / "test").write_text("0")

    @workflow.step
    def unstable_step():
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < 10:
            raise ValueError("Invalid")
        return v

    with pytest.raises(Exception):
        unstable_step.options(step_max_retries=-1).step().run()

    with pytest.raises(Exception):
        unstable_step.options(step_max_retries=3).step().run()
    assert 10 == unstable_step.options(step_max_retries=8).step().run()
    (tmp_path / "test").write_text("0")
    (ret, err) = unstable_step.options(
        step_max_retries=3, catch_exceptions=True).step().run()
    assert ret is None
    assert isinstance(err, ValueError)
    (ret, err) = unstable_step.options(
        step_max_retries=8, catch_exceptions=True).step().run()
    assert ret == 10
    assert err is None


@pytest.mark.parametrize(
    "workflow_start_regular_shared", [{
        "num_cpus": 2,
    }], indirect=True)
def test_step_resources(workflow_start_regular_shared, tmp_path):
    lock_path = str(tmp_path / "lock")

    @workflow.step
    def step_run():
        with FileLock(lock_path):
            return None

    @ray.remote(num_cpus=1)
    def remote_run():
        return None

    lock = FileLock(lock_path)
    lock.acquire()
    ret = step_run.options(num_cpus=2).step().run_async()
    obj = remote_run.remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(obj, timeout=2)
    lock.release()
    assert ray.get(ret) is None
    assert ray.get(obj) is None


def test_init_twice(tmp_path):
    workflow.init()
    with pytest.raises(RuntimeError):
        workflow.init(str(tmp_path))


driver_script = """
from ray.experimental import workflow

if __name__ == "__main__":
    workflow.init()
"""


def test_init_twice_2(tmp_path):
    run_string_as_driver(driver_script)
    with pytest.raises(RuntimeError):
        workflow.init(str(tmp_path))
