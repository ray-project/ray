import os
import time

from ray.tests.conftest import *  # noqa

import pytest
import ray
from ray import workflow
from ray.workflow import workflow_access


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


def test_partial(workflow_start_regular_shared):
    ys = [1, 2, 3]

    def add(x, y):
        return x + y

    from functools import partial

    f1 = workflow.step(partial(add, 10)).step(10)

    assert "__anonymous_func__" in f1._name
    assert f1.run() == 20

    fs = [partial(add, y=y) for y in ys]

    @ray.workflow.step
    def chain_func(*args, **kw_argv):
        # Get the first function as a start
        wf_step = workflow.step(fs[0]).step(*args, **kw_argv)
        for i in range(1, len(fs)):
            # Convert each function inside steps into workflow step
            # function and then use the previous output as the input
            # for them.
            wf_step = workflow.step(fs[i]).step(wf_step)
        return wf_step

    assert chain_func.step(1).run() == 7


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
        ref = workflow_access.flatten_workflow_output("fake_workflow_id", nested_ref)
    finally:
        # restore the function
        workflow_access._resolve_workflow_output = original_func
    assert ray.get(ref) == 42


def test_run_or_resume_during_running(workflow_start_regular_shared):
    output = simple_sequential.step().run_async(workflow_id="running_workflow")
    with pytest.raises(RuntimeError):
        simple_sequential.step().run_async(workflow_id="running_workflow")
    with pytest.raises(RuntimeError):
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
        unstable_step.options(max_retries=-2).step().run()

    with pytest.raises(Exception):
        unstable_step.options(max_retries=2).step().run()
    assert 10 == unstable_step.options(max_retries=7).step().run()
    (tmp_path / "test").write_text("0")
    (ret, err) = (
        unstable_step.options(max_retries=2, catch_exceptions=True).step().run()
    )
    assert ret is None
    assert isinstance(err, ValueError)
    (ret, err) = (
        unstable_step.options(max_retries=7, catch_exceptions=True).step().run()
    )
    assert ret == 10
    assert err is None


def test_step_failure_decorator(workflow_start_regular_shared, tmp_path):
    (tmp_path / "test").write_text("0")

    @workflow.step(max_retries=10)
    def unstable_step():
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < 10:
            raise ValueError("Invalid")
        return v

    assert unstable_step.step().run() == 10

    (tmp_path / "test").write_text("0")

    @workflow.step(catch_exceptions=True)
    def unstable_step_exception():
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < 10:
            raise ValueError("Invalid")
        return v

    (ret, err) = unstable_step_exception.step().run()
    assert ret is None
    assert err is not None

    (tmp_path / "test").write_text("0")

    @workflow.step(catch_exceptions=True, max_retries=3)
    def unstable_step_exception():
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < 10:
            raise ValueError("Invalid")
        return v

    (ret, err) = unstable_step_exception.step().run()
    assert ret is None
    assert err is not None
    assert (tmp_path / "test").read_text() == "4"


def test_nested_catch_exception(workflow_start_regular_shared, tmp_path):
    @workflow.step
    def f2():
        return 10

    @workflow.step
    def f1():
        return f2.step()

    assert (10, None) == f1.options(catch_exceptions=True).step().run()


def test_nested_catch_exception_2(workflow_start_regular_shared, tmp_path):
    @workflow.step
    def f1(n):
        if n == 0:
            raise ValueError()
        else:
            return f1.step(n - 1)

    ret, err = f1.options(catch_exceptions=True).step(5).run()
    assert ret is None
    assert isinstance(err, ValueError)


def test_dynamic_output(workflow_start_regular_shared):
    @workflow.step
    def exponential_fail(k, n):
        if n > 0:
            if n < 3:
                raise Exception("Failed intentionally")
            return exponential_fail.options(name=f"step_{n}").step(k * 2, n - 1)
        return k

    # When workflow fails, the dynamic output should points to the
    # latest successful step.
    try:
        exponential_fail.options(name="step_0").step(3, 10).run(
            workflow_id="dynamic_output"
        )
    except Exception:
        pass
    from ray.workflow.workflow_storage import get_workflow_storage

    wf_storage = get_workflow_storage(workflow_id="dynamic_output")
    result = wf_storage.inspect_step("step_0")
    assert result.output_step_id == "step_3"


def test_workflow_error_message():
    storage_url = r"c:\ray"
    expected_error_msg = "Invalid url: {}.".format(storage_url)
    if os.name == "nt":

        expected_error_msg += (
            " Try using file://{} or file:///{} for Windows file paths.".format(
                storage_url, storage_url
            )
        )
    with pytest.raises(ValueError) as e:
        workflow.init(storage_url)
    assert str(e.value) == expected_error_msg


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
