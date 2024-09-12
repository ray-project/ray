import pytest
import ray
from filelock import FileLock
from ray._private.test_utils import SignalActor
from ray import workflow
from ray.tests.conftest import *  # noqa
from ray._private.test_utils import skip_flaky_core_test_premerge


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 2,
        }
    ],
    indirect=True,
)
def test_task_resources(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")
    # We use signal actor here because we can't guarantee the order of tasks
    # sent from worker to raylet.
    signal_actor = SignalActor.remote()

    @ray.remote
    def task_run():
        ray.wait([signal_actor.send.remote()])
        with FileLock(lock_path):
            return None

    @ray.remote(num_cpus=1)
    def remote_run():
        return None

    lock = FileLock(lock_path)
    lock.acquire()
    ret = workflow.run_async(task_run.options(num_cpus=2).bind())
    ray.wait([signal_actor.wait.remote()])
    obj = remote_run.remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(obj, timeout=2)
    lock.release()
    assert ray.get(ret) is None
    assert ray.get(obj) is None


def test_get_output_1(workflow_start_regular, tmp_path):
    @ray.remote
    def simple(v):
        return v

    assert 0 == workflow.run(simple.bind(0), workflow_id="simple")
    assert 0 == workflow.get_output("simple")


def test_get_output_2(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")
    lock = FileLock(lock_path)

    @ray.remote
    def simple(v):
        with FileLock(lock_path):
            return v

    lock.acquire()
    obj = workflow.run_async(simple.bind(0), workflow_id="simple")
    obj2 = workflow.get_output_async("simple")
    lock.release()
    assert ray.get([obj, obj2]) == [0, 0]


def test_get_output_3(workflow_start_regular, tmp_path):
    cnt_file = tmp_path / "counter"
    cnt_file.write_text("0")
    error_flag = tmp_path / "error"
    error_flag.touch()

    @ray.remote
    def incr():
        v = int(cnt_file.read_text())
        cnt_file.write_text(str(v + 1))
        if error_flag.exists():
            raise ValueError()
        return 10

    with pytest.raises(workflow.WorkflowExecutionError):
        workflow.run(incr.options(max_retries=0).bind(), workflow_id="incr")

    assert cnt_file.read_text() == "1"

    from ray.exceptions import RaySystemError

    # TODO(suquark): We should prevent Ray from raising "RaySystemError",
    #   in workflow, because "RaySystemError" does not inherit the underlying
    #   error, so users and developers cannot catch the expected error.
    #   I feel this issue is a very annoying.
    with pytest.raises((RaySystemError, ValueError)):
        workflow.get_output("incr")

    assert cnt_file.read_text() == "1"
    error_flag.unlink()
    with pytest.raises((RaySystemError, ValueError)):
        workflow.get_output("incr")
    assert workflow.resume("incr") == 10


def test_get_output_4(workflow_start_regular, tmp_path):
    """Test getting output of a workflow tasks that are dynamically generated."""
    lock_path = str(tmp_path / "lock")
    lock = FileLock(lock_path)

    @ray.remote
    def recursive(n):
        if n <= 0:
            with FileLock(lock_path):
                return 42
        return workflow.continuation(
            recursive.options(**workflow.options(task_id=str(n - 1))).bind(n - 1)
        )

    workflow_id = "test_get_output_4"
    lock.acquire()
    obj = workflow.run_async(
        recursive.options(**workflow.options(task_id="10")).bind(10),
        workflow_id=workflow_id,
    )

    outputs = [
        workflow.get_output_async(workflow_id, task_id=str(i)) for i in range(11)
    ]
    outputs.append(obj)

    import time

    # wait so that 'get_output' is scheduled before executing the workflow
    time.sleep(3)
    lock.release()
    assert ray.get(outputs) == [42] * len(outputs)


def test_get_output_5(workflow_start_regular, tmp_path):
    """Test getting output of a workflow task immediately after executing it
    asynchronously."""

    @ray.remote
    def simple():
        return 314

    workflow_id = "test_get_output_5_{}"

    outputs = []
    for i in range(20):
        workflow.run_async(simple.bind(), workflow_id=workflow_id.format(i))
        outputs.append(workflow.get_output_async(workflow_id.format(i)))

    assert ray.get(outputs) == [314] * len(outputs)


@skip_flaky_core_test_premerge("https://github.com/ray-project/ray/issues/41511")
def test_output_with_name(workflow_start_regular):
    @ray.remote
    def double(v):
        return 2 * v

    inner_task = double.options(**workflow.options(task_id="inner")).bind(1)
    outer_task = double.options(**workflow.options(task_id="outer")).bind(inner_task)
    result = workflow.run_async(outer_task, workflow_id="double")
    inner = workflow.get_output_async("double", task_id="inner")
    outer = workflow.get_output_async("double", task_id="outer")

    assert ray.get(inner) == 2
    assert ray.get(outer) == 4
    assert ray.get(result) == 4

    @workflow.options(task_id="double")
    @ray.remote
    def double_2(s):
        return s * 2

    inner_task = double_2.bind(1)
    outer_task = double_2.bind(inner_task)
    workflow_id = "double_2"
    result = workflow.run_async(outer_task, workflow_id=workflow_id)

    inner = workflow.get_output_async(workflow_id, task_id="double")
    outer = workflow.get_output_async(workflow_id, task_id="double_1")

    assert ray.get(inner) == 2
    assert ray.get(outer) == 4
    assert ray.get(result) == 4


def test_get_non_exist_output(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")

    @ray.remote
    def simple():
        with FileLock(lock_path):
            return "hello"

    workflow_id = "test_get_non_exist_output"

    with FileLock(lock_path):
        dag = simple.options(**workflow.options(task_id="simple")).bind()
        ret = workflow.run_async(dag, workflow_id=workflow_id)
        exist = workflow.get_output_async(workflow_id, task_id="simple")
        non_exist = workflow.get_output_async(workflow_id, task_id="non_exist")

    assert ray.get(ret) == "hello"
    assert ray.get(exist) == "hello"
    with pytest.raises(ValueError, match="non_exist"):
        ray.get(non_exist)


def test_get_named_task_output_finished(workflow_start_regular, tmp_path):
    @ray.remote
    def double(v):
        return 2 * v

    # Get the result from named task after workflow finished
    assert 4 == workflow.run(
        double.options(**workflow.options(task_id="outer")).bind(
            double.options(**workflow.options(task_id="inner")).bind(1)
        ),
        workflow_id="double",
    )
    assert workflow.get_output("double", task_id="inner") == 2
    assert workflow.get_output("double", task_id="outer") == 4


def test_get_named_task_output_running(workflow_start_regular, tmp_path):
    @ray.remote
    def double(v, lock=None):
        if lock is not None:
            with FileLock(lock_path):
                return 2 * v
        else:
            return 2 * v

    # Get the result from named task after workflow before it's finished
    lock_path = str(tmp_path / "lock")
    lock = FileLock(lock_path)
    lock.acquire()
    output = workflow.run_async(
        double.options(**workflow.options(task_id="outer")).bind(
            double.options(**workflow.options(task_id="inner")).bind(1, lock_path),
            lock_path,
        ),
        workflow_id="double-2",
    )

    inner = workflow.get_output_async("double-2", task_id="inner")
    outer = workflow.get_output_async("double-2", task_id="outer")

    @ray.remote
    def wait(obj_ref):
        return ray.get(obj_ref[0])

    # Make sure nothing is finished.
    ready, waiting = ray.wait(
        [wait.remote([output]), wait.remote([inner]), wait.remote([outer])], timeout=1
    )
    assert 0 == len(ready)
    assert 3 == len(waiting)

    # Once job finished, we'll be able to get the result.
    lock.release()
    assert [4, 2, 4] == ray.get([output, inner, outer])

    inner = workflow.get_output_async("double-2", task_id="inner")
    outer = workflow.get_output_async("double-2", task_id="outer")
    assert [2, 4] == ray.get([inner, outer])


def test_get_named_task_output_error(workflow_start_regular, tmp_path):
    @ray.remote
    def double(v, error):
        if error:
            raise Exception()
        return v + v

    # Force it to fail for the outer task
    with pytest.raises(Exception):
        workflow.run(
            double.options(**workflow.options(task_id="outer")).bind(
                double.options(**workflow.options(task_id="inner")).bind(1, False), True
            ),
            workflow_id="double",
        )

    # For the inner task, it should have already been executed.
    assert 2 == workflow.get_output("double", task_id="inner")
    with pytest.raises(Exception):
        workflow.get_output("double", task_id="outer")


def test_get_named_task_default(workflow_start_regular, tmp_path):
    @ray.remote
    def factorial(n, r=1):
        if n == 1:
            return r
        return workflow.continuation(factorial.bind(n - 1, r * n))

    import math

    assert math.factorial(5) == workflow.run(factorial.bind(5), workflow_id="factorial")
    for i in range(5):
        task_name = (
            "python.ray.workflow.tests.test_basic_workflows_2."
            "test_get_named_task_default.locals.factorial"
        )

        if i != 0:
            task_name += "_" + str(i)
        # All outputs will be 120
        assert math.factorial(5) == workflow.get_output("factorial", task_id=task_name)


def test_get_named_task_duplicate(workflow_start_regular):
    @workflow.options(task_id="f")
    @ray.remote
    def f(n, dep):
        return n

    inner = f.bind(10, None)
    outer = f.bind(20, inner)
    assert 20 == workflow.run(outer, workflow_id="duplicate")
    # The outer will be checkpointed first. So there is no suffix for the name
    assert workflow.get_output("duplicate", task_id="f") == 10
    # The inner will be checkpointed after the outer. And there is a duplicate
    # for the name. suffix _1 is added automatically
    assert workflow.get_output("duplicate", task_id="f_1") == 20


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
