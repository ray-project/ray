import pytest
import ray
import re
from filelock import FileLock
from ray._private.test_utils import run_string_as_driver, SignalActor
from ray import workflow
from ray.tests.conftest import *  # noqa


def test_init_twice(call_ray_start, reset_workflow, tmp_path):
    workflow.init()
    with pytest.raises(RuntimeError):
        workflow.init(str(tmp_path))


driver_script = """
from ray import workflow

if __name__ == "__main__":
    workflow.init()
"""


def test_init_twice_2(call_ray_start, reset_workflow, tmp_path):
    run_string_as_driver(driver_script)
    with pytest.raises(RuntimeError):
        workflow.init(str(tmp_path))


@pytest.mark.parametrize(
    "workflow_start_regular", [{
        "num_cpus": 2,
    }], indirect=True)
def test_step_resources(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")
    # We use signal actor here because we can't guarantee the order of tasks
    # sent from worker to raylet.
    signal_actor = SignalActor.remote()

    @workflow.step
    def step_run():
        ray.wait([signal_actor.send.remote()])
        with FileLock(lock_path):
            return None

    @ray.remote(num_cpus=1)
    def remote_run():
        return None

    lock = FileLock(lock_path)
    lock.acquire()
    ret = step_run.options(num_cpus=2).step().run_async()
    ray.wait([signal_actor.wait.remote()])
    obj = remote_run.remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(obj, timeout=2)
    lock.release()
    assert ray.get(ret) is None
    assert ray.get(obj) is None


def test_get_output_1(workflow_start_regular, tmp_path):
    @workflow.step
    def simple(v):
        return v

    assert 0 == simple.step(0).run("simple")
    assert 0 == ray.get(workflow.get_output("simple"))


def test_get_output_2(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")
    lock = FileLock(lock_path)

    @workflow.step
    def simple(v):
        with FileLock(lock_path):
            return v

    lock.acquire()
    obj = simple.step(0).run_async("simple")
    obj2 = workflow.get_output("simple")
    lock.release()
    assert ray.get([obj, obj2]) == [0, 0]


def test_get_output_3(workflow_start_regular, tmp_path):
    cnt_file = tmp_path / "counter"
    cnt_file.write_text("0")
    error_flag = tmp_path / "error"
    error_flag.touch()

    @workflow.step
    def incr():
        v = int(cnt_file.read_text())
        cnt_file.write_text(str(v + 1))
        if error_flag.exists():
            raise ValueError()
        return 10

    with pytest.raises(ray.exceptions.RaySystemError):
        incr.options(max_retries=1).step().run("incr")

    assert cnt_file.read_text() == "1"

    with pytest.raises(ray.exceptions.RaySystemError):
        ray.get(workflow.get_output("incr"))

    assert cnt_file.read_text() == "1"
    error_flag.unlink()
    with pytest.raises(ray.exceptions.RaySystemError):
        ray.get(workflow.get_output("incr"))
    assert ray.get(workflow.resume("incr")) == 10


def test_get_named_step_output_finished(workflow_start_regular, tmp_path):
    @workflow.step
    def double(v):
        return 2 * v

    # Get the result from named step after workflow finished
    assert 4 == double.options(name="outer").step(
        double.options(name="inner").step(1)).run("double")
    assert ray.get(workflow.get_output("double", name="inner")) == 2
    assert ray.get(workflow.get_output("double", name="outer")) == 4


def test_get_named_step_output_running(workflow_start_regular, tmp_path):
    @workflow.step
    def double(v, lock=None):
        if lock is not None:
            with FileLock(lock_path):
                return 2 * v
        else:
            return 2 * v

    # Get the result from named step after workflow before it's finished
    lock_path = str(tmp_path / "lock")
    lock = FileLock(lock_path)
    lock.acquire()
    output = double.options(name="outer").step(
        double.options(name="inner").step(1, lock_path),
        lock_path).run_async("double-2")

    inner = workflow.get_output("double-2", name="inner")
    outer = workflow.get_output("double-2", name="outer")

    @ray.remote
    def wait(obj_ref):
        return ray.get(obj_ref[0])

    # Make sure nothing is finished.
    ready, waiting = ray.wait(
        [wait.remote([output]),
         wait.remote([inner]),
         wait.remote([outer])],
        timeout=1)
    assert 0 == len(ready)
    assert 3 == len(waiting)

    # Once job finished, we'll be able to get the result.
    lock.release()
    assert 4 == ray.get(output)

    # Here sometimes inner will not be generated when we call
    # run_async. So there is a race condition here.
    try:
        v = ray.get(inner)
    except Exception:
        v = None
    if v is not None:
        assert 2 == 20
    assert 4 == ray.get(outer)

    inner = workflow.get_output("double-2", name="inner")
    outer = workflow.get_output("double-2", name="outer")
    assert 2 == ray.get(inner)
    assert 4 == ray.get(outer)


def test_get_named_step_output_error(workflow_start_regular, tmp_path):
    @workflow.step
    def double(v, error):
        if error:
            raise Exception()
        return v + v

    # Force it to fail for the outer step
    with pytest.raises(Exception):
        double.options(name="outer").step(
            double.options(name="inner").step(1, False), True).run("double")

    # For the inner step, it should have already been executed.
    assert 2 == ray.get(workflow.get_output("double", name="inner"))
    outer = workflow.get_output("double", name="outer")
    with pytest.raises(Exception):
        ray.get(outer)


def test_get_named_step_default(workflow_start_regular, tmp_path):
    @workflow.step
    def factorial(n, r=1):
        if n == 1:
            return r
        return factorial.step(n - 1, r * n)

    import math
    assert math.factorial(5) == factorial.step(5).run("factorial")
    for i in range(5):
        step_name = ("test_basic_workflows_2."
                     "test_get_named_step_default.locals.factorial")
        if i != 0:
            step_name += "_" + str(i)
        # All outputs will be 120
        assert math.factorial(5) == ray.get(
            workflow.get_output("factorial", name=step_name))


def test_get_named_step_duplicate(workflow_start_regular):
    @workflow.step(name="f")
    def f(n, dep):
        return n

    inner = f.step(10, None)
    outer = f.step(20, inner)
    assert 20 == outer.run("duplicate")
    # The outer will be checkpointed first. So there is no suffix for the name
    assert ray.get(workflow.get_output("duplicate", name="f")) == 20
    # The inner will be checkpointed after the outer. And there is a duplicate
    # for the name. suffix _1 is added automatically
    assert ray.get(workflow.get_output("duplicate", name="f_1")) == 10


def test_no_init(shutdown_only):
    @workflow.step
    def f():
        pass

    fail_wf_init_error_msg = re.escape(
        "`workflow.init()` must be called prior to using "
        "the workflows API.")

    with pytest.raises(RuntimeError, match=fail_wf_init_error_msg):
        f.step().run()
    with pytest.raises(RuntimeError, match=fail_wf_init_error_msg):
        workflow.list_all()
    with pytest.raises(RuntimeError, match=fail_wf_init_error_msg):
        workflow.resume_all()
    with pytest.raises(RuntimeError, match=fail_wf_init_error_msg):
        workflow.cancel("wf")
    with pytest.raises(RuntimeError, match=fail_wf_init_error_msg):
        workflow.get_actor("wf")


def test_wf_run(workflow_start_regular, tmp_path):
    counter = tmp_path / "counter"
    counter.write_text("0")

    @workflow.step
    def f():
        v = int(counter.read_text()) + 1
        counter.write_text(str(v))

    f.step().run("abc")
    assert counter.read_text() == "1"
    # This will not rerun the job from beginning
    f.step().run("abc")
    assert counter.read_text() == "1"


def test_wf_no_run():
    @workflow.step
    def f1():
        pass

    f1.step()

    @workflow.step
    def f2(*w):
        pass

    f = f2.step(*[f1.step() for _ in range(10)])

    with pytest.raises(Exception):
        f.run()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
