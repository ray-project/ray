import os
import time

from ray.tests.conftest import *  # noqa

import pytest
import ray
from ray import workflow
from ray.workflow import workflow_access
from ray.workflow.tests.utils import update_workflow_options


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
    def nested_step(x):
        return workflow.continuation(append2.bind(append1.bind(x + "~[nested]~")))

    @ray.remote
    def nested(x):
        return workflow.continuation(nested_step.bind(x))

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
    assert (
        workflow.create(simple_sequential.bind()).run() == "[source1][append1][append2]"
    )

    wf = simple_sequential_with_input.bind("start:")
    assert workflow.create(wf).run() == "start:[append1][append2]"

    wf = loop_sequential.bind(3)
    assert workflow.create(wf).run() == "[source1]" + "[append1]" * 3 + "[append2]"

    wf = nested.bind("nested:")
    assert workflow.create(wf).run() == "nested:~[nested]~[append1][append2]"

    wf = fork_join.bind()
    assert workflow.create(wf).run() == "join([source1][append1], [source1][append2])"

    assert workflow.create(factorial.bind(10)).run() == 3628800


def test_async_execution(workflow_start_regular_shared):
    @ray.remote
    def blocking():
        time.sleep(10)
        return 314

    start = time.time()
    output = workflow.create(blocking.bind()).run_async()
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

    @ray.remote
    def chain_func(*args, **kw_argv):
        # Get the first function as a start
        wf_step = workflow.step(fs[0]).step(*args, **kw_argv)
        for i in range(1, len(fs)):
            # Convert each function inside steps into workflow step
            # function and then use the previous output as the input
            # for them.
            wf_step = workflow.step(fs[i]).step(wf_step)
        return wf_step

    assert workflow.create(chain_func.bind(1)).run() == 7


def _resolve_workflow_output(workflow_id: str, output: ray.ObjectRef):
    while isinstance(output, ray.ObjectRef):
        output = ray.get(output)
    return output


def test_workflow_output_resolving(workflow_start_regular_shared):
    @ray.remote
    def deep_nested(x):
        if x >= 42:
            return x
        return deep_nested.remote(x + 1)

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

    output = workflow.create(simple_sequential.bind()).run_async(
        workflow_id="running_workflow"
    )
    with pytest.raises(RuntimeError):
        workflow.create(simple_sequential.bind()).run_async(
            workflow_id="running_workflow"
        )
    with pytest.raises(RuntimeError):
        workflow.resume(workflow_id="running_workflow")
    assert ray.get(output) == "[source1][append1][append2]"


def test_step_failure(workflow_start_regular_shared, tmp_path):
    (tmp_path / "test").write_text("0")

    @ray.remote
    def unstable_step():
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < 10:
            raise ValueError("Invalid")
        return v

    with pytest.raises(Exception):
        workflow.create(update_workflow_options(unstable_step, max_retries=-2).bind())

    with pytest.raises(Exception):
        workflow.create(
            update_workflow_options(unstable_step, max_retries=2).bind()
        ).run()
    assert (
        10
        == workflow.create(
            update_workflow_options(unstable_step, max_retries=7).bind()
        ).run()
    )
    (tmp_path / "test").write_text("0")
    (ret, err) = workflow.create(
        update_workflow_options(
            unstable_step, max_retries=2, catch_exceptions=True
        ).bind()
    ).run()
    assert ret is None
    assert isinstance(err, ValueError)
    (ret, err) = workflow.create(
        update_workflow_options(
            unstable_step, max_retries=7, catch_exceptions=True
        ).bind()
    ).run()
    assert ret == 10
    assert err is None


def test_step_failure_decorator(workflow_start_regular_shared, tmp_path):
    (tmp_path / "test").write_text("0")

    @workflow.options(max_retries=10)
    @ray.remote
    def unstable_step():
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < 10:
            raise ValueError("Invalid")
        return v

    assert workflow.create(unstable_step.bind()).run() == 10

    (tmp_path / "test").write_text("0")

    @workflow.options(catch_exceptions=True)
    @ray.remote
    def unstable_step_exception():
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < 10:
            raise ValueError("Invalid")
        return v

    (ret, err) = workflow.create(unstable_step_exception.bind()).run()
    assert ret is None
    assert err is not None

    (tmp_path / "test").write_text("0")

    @workflow.options(catch_exceptions=True, max_retries=3)
    @ray.remote
    def unstable_step_exception():
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < 10:
            raise ValueError("Invalid")
        return v

    (ret, err) = workflow.create(unstable_step_exception.bind()).run()
    assert ret is None
    assert err is not None
    assert (tmp_path / "test").read_text() == "4"


def test_nested_catch_exception(workflow_start_regular_shared, tmp_path):
    @ray.remote
    def f2():
        return 10

    @ray.remote
    def f1():
        return workflow.continuation(f2.bind())

    assert (10, None) == workflow.create(
        update_workflow_options(f1, catch_exceptions=True).bind()
    ).run()


def test_nested_catch_exception_2(workflow_start_regular_shared, tmp_path):
    @ray.remote
    def f1(n):
        if n == 0:
            raise ValueError()
        else:
            return workflow.continuation(f1.bind(n - 1))

    ret, err = workflow.create(
        update_workflow_options(f1, catch_exceptions=True).bind(5)
    ).run()
    assert ret is None
    assert isinstance(err, ValueError)


def test_dynamic_output(workflow_start_regular_shared):
    @ray.remote
    def exponential_fail(k, n):
        if n > 0:
            if n < 3:
                raise Exception("Failed intentionally")
            return workflow.continuation(
                update_workflow_options(exponential_fail, name=f"step_{n}").bind(
                    k * 2, n - 1
                )
            )
        return k

    # When workflow fails, the dynamic output should points to the
    # latest successful step.
    try:
        workflow.create(
            update_workflow_options(exponential_fail, name="step_0").bind(3, 10)
        ).run(workflow_id="dynamic_output")
    except Exception:
        pass
    from ray.workflow.workflow_storage import get_workflow_storage

    wf_storage = get_workflow_storage(workflow_id="dynamic_output")
    result = wf_storage.inspect_step("step_0")
    assert result.output_step_id == "step_3"


def test_workflow_error_message():
    storage_url = r"c:\ray"
    expected_error_msg = f"Cannot parse URI: '{storage_url}'"
    if os.name == "nt":

        expected_error_msg += (
            " Try using file://{} or file:///{} for Windows file paths.".format(
                storage_url, storage_url
            )
        )
    if ray.is_initialized():
        ray.shutdown()
    with pytest.raises(ValueError) as e:
        ray.init(storage=storage_url)
    assert str(e.value) == expected_error_msg


def test_options_update(workflow_start_regular_shared):
    from ray.workflow.common import WORKFLOW_OPTIONS

    # Options are given in decorator first, then in the first .options()
    # and finally in the second .options()
    @workflow.options(name="old_name", metadata={"k": "v"}, max_retries=1)
    @ray.remote(num_cpus=2)
    def f():
        return

    # name is updated from the old name in the decorator to the new name in the first
    # .options(), then preserved in the second options.
    # metadata and ray_options are "updated"
    # max_retries only defined in the decorator and it got preserved all the way
    new_f = update_workflow_options(
        f, name="new_name", num_returns=2, metadata={"extra_k2": "extra_v2"}
    )
    options = new_f.bind().get_options()
    assert options == {
        "num_cpus": 2,
        "_metadata": {
            WORKFLOW_OPTIONS: {
                "name": "new_name",
                "metadata": {"extra_k2": "extra_v2"},
                "max_retries": 1,
                "num_returns": 2,
            }
        },
    }


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
