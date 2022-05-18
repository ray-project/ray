import pytest
import ray
from filelock import FileLock
from ray._private.test_utils import SignalActor
from ray import workflow
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 2,
        }
    ],
    indirect=True,
)
def test_step_resources(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")
    # We use signal actor here because we can't guarantee the order of tasks
    # sent from worker to raylet.
    signal_actor = SignalActor.remote()

    @ray.remote
    def step_run():
        ray.wait([signal_actor.send.remote()])
        with FileLock(lock_path):
            return None

    @ray.remote(num_cpus=1)
    def remote_run():
        return None

    lock = FileLock(lock_path)
    lock.acquire()
    ret = workflow.create(step_run.options(num_cpus=2).bind()).run_async()
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

    assert 0 == workflow.create(simple.bind(0)).run("simple")
    assert 0 == ray.get(workflow.get_output("simple"))


def test_get_output_2(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")
    lock = FileLock(lock_path)

    @ray.remote
    def simple(v):
        with FileLock(lock_path):
            return v

    lock.acquire()
    obj = workflow.create(simple.bind(0)).run_async("simple")
    obj2 = workflow.get_output("simple")
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

    with pytest.raises(ray.exceptions.RaySystemError):
        workflow.create(incr.options(**workflow.options(max_retries=0)).bind()).run(
            "incr"
        )

    assert cnt_file.read_text() == "1"

    with pytest.raises(ray.exceptions.RaySystemError):
        ray.get(workflow.get_output("incr"))

    assert cnt_file.read_text() == "1"
    error_flag.unlink()
    with pytest.raises(ray.exceptions.RaySystemError):
        ray.get(workflow.get_output("incr"))
    assert ray.get(workflow.resume("incr")) == 10


def test_get_named_step_output_finished(workflow_start_regular, tmp_path):
    @ray.remote
    def double(v):
        return 2 * v

    # Get the result from named step after workflow finished
    assert 4 == workflow.create(
        double.options(**workflow.options(name="outer")).bind(
            double.options(**workflow.options(name="inner")).bind(1)
        )
    ).run("double")
    assert ray.get(workflow.get_output("double", name="inner")) == 2
    assert ray.get(workflow.get_output("double", name="outer")) == 4


def test_get_named_step_output_running(workflow_start_regular, tmp_path):
    @ray.remote
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
    output = workflow.create(
        double.options(**workflow.options(name="outer")).bind(
            double.options(**workflow.options(name="inner")).bind(1, lock_path),
            lock_path,
        )
    ).run_async("double-2")

    inner = workflow.get_output("double-2", name="inner")
    outer = workflow.get_output("double-2", name="outer")

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
    assert 4 == ray.get(output)

    # Here sometimes inner will not be generated when we call
    # run_async. So there is a race condition here.
    try:
        v = ray.get(inner)
    except Exception:
        v = None
    if v is not None:
        assert 2 == v
    assert 4 == ray.get(outer)

    inner = workflow.get_output("double-2", name="inner")
    outer = workflow.get_output("double-2", name="outer")
    assert 2 == ray.get(inner)
    assert 4 == ray.get(outer)


def test_get_named_step_output_error(workflow_start_regular, tmp_path):
    @ray.remote
    def double(v, error):
        if error:
            raise Exception()
        return v + v

    # Force it to fail for the outer step
    with pytest.raises(Exception):
        workflow.create(
            double.options(**workflow.options(name="outer")).bind(
                double.options(**workflow.options(name="inner")).bind(1, False), True
            )
        ).run("double")

    # For the inner step, it should have already been executed.
    assert 2 == ray.get(workflow.get_output("double", name="inner"))
    outer = workflow.get_output("double", name="outer")
    with pytest.raises(Exception):
        ray.get(outer)


def test_get_named_step_default(workflow_start_regular, tmp_path):
    @ray.remote
    def factorial(n, r=1):
        if n == 1:
            return r
        return workflow.continuation(factorial.bind(n - 1, r * n))

    import math

    assert math.factorial(5) == workflow.create(factorial.bind(5)).run("factorial")
    for i in range(5):
        step_name = (
            "test_basic_workflows_2.test_get_named_step_default.locals.factorial"
        )
        if i != 0:
            step_name += "_" + str(i)
        # All outputs will be 120
        assert math.factorial(5) == ray.get(
            workflow.get_output("factorial", name=step_name)
        )


def test_get_named_step_duplicate(workflow_start_regular):
    @workflow.options(name="f")
    @ray.remote
    def f(n, dep):
        return n

    inner = f.bind(10, None)
    outer = f.bind(20, inner)
    assert 20 == workflow.create(outer).run("duplicate")
    # The outer will be checkpointed first. So there is no suffix for the name
    assert ray.get(workflow.get_output("duplicate", name="f")) == 20
    # The inner will be checkpointed after the outer. And there is a duplicate
    # for the name. suffix _1 is added automatically
    assert ray.get(workflow.get_output("duplicate", name="f_1")) == 10


def test_no_init_run(shutdown_only):
    @ray.remote
    def f():
        pass

    workflow.create(f.bind()).run()


def test_no_init_api(shutdown_only):
    workflow.list_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
