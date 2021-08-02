import subprocess
import time

from ray.tests.conftest import *  # noqa
import pytest
from filelock import FileLock
import ray
from ray.test_utils import run_string_as_driver_nonblocking
from ray.exceptions import RaySystemError
from ray.experimental import workflow
from ray.experimental.workflow.tests import utils
from ray.experimental.workflow import workflow_storage


@workflow.step
def the_failed_step(x):
    if not utils.check_global_mark():
        import os
        os.kill(os.getpid(), 9)
    return "foo(" + x + ")"


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
def join(x, y):
    return f"join({x}, {y})"


@workflow.step
def complex(x1):
    x2 = source1.step()
    v = join.step(x1, x2)
    y = append1.step(x1)
    y = the_failed_step.step(y)
    z = append2.step(x2)
    u = join.step(y, z)
    return join.step(u, v)


@workflow.step
def simple(x):
    x = append1.step(x)
    y = the_failed_step.step(x)
    z = append2.step(y)
    return z


def test_recovery_simple(workflow_start_regular):
    utils.unset_global_mark()
    workflow_id = "test_recovery_simple"
    with pytest.raises(RaySystemError):
        # internally we get WorkerCrashedError
        simple.step("x").run(workflow_id=workflow_id)
    utils.set_global_mark()
    output = workflow.resume(workflow_id)
    assert ray.get(output) == "foo(x[append1])[append2]"
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    output = workflow.resume(workflow_id)
    assert ray.get(output) == "foo(x[append1])[append2]"


def test_recovery_complex(workflow_start_regular):
    utils.unset_global_mark()
    workflow_id = "test_recovery_complex"
    with pytest.raises(RaySystemError):
        # internally we get WorkerCrashedError
        complex.step("x").run(workflow_id=workflow_id)
    utils.set_global_mark()
    output = workflow.resume(workflow_id)
    r = "join(join(foo(x[append1]), [source1][append2]), join(x, [source1]))"
    assert ray.get(output) == r
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    output = workflow.resume(workflow_id)
    r = "join(join(foo(x[append1]), [source1][append2]), join(x, [source1]))"
    assert ray.get(output) == r


def test_recovery_non_exists_workflow(workflow_start_regular):
    with pytest.raises(ValueError):
        ray.get(workflow.resume("this_workflow_id_does_not_exist"))


driver_script = """
import time
from ray.experimental import workflow


@workflow.step
def foo(x):
    print("Executing", x)
    time.sleep(1)
    if x < 20:
        return foo.step(x + 1)
    else:
        return 20


if __name__ == "__main__":
    workflow.init("{tmp_path}")
    assert foo.step(0).run(workflow_id="cluster_failure") == 20
"""


def test_recovery_cluster_failure(reset_workflow, tmp_path):
    subprocess.check_call(["ray", "start", "--head"])
    time.sleep(1)
    proc = run_string_as_driver_nonblocking(
        driver_script.format(tmp_path=str(tmp_path)))
    time.sleep(10)
    subprocess.check_call(["ray", "stop"])
    proc.kill()
    time.sleep(1)
    workflow.init(str(tmp_path))
    assert ray.get(workflow.resume("cluster_failure")) == 20
    workflow.storage.set_global_storage(None)
    ray.shutdown()


def test_recovery_cluster_failure_resume_all(reset_workflow, tmp_path):
    tmp_path = tmp_path
    subprocess.check_call(["ray", "start", "--head"])
    time.sleep(1)
    workflow_dir = tmp_path / "workflow"
    lock_file = tmp_path / "lock_file"
    driver_script = f"""
import time
from ray.experimental import workflow
from filelock import FileLock
@workflow.step
def foo(x):
    with FileLock("{str(lock_file)}"):
        return 20

if __name__ == "__main__":
    workflow.init("{str(workflow_dir)}")
    assert foo.step(0).run(workflow_id="cluster_failure") == 20
"""
    lock = FileLock(lock_file)
    lock.acquire()

    proc = run_string_as_driver_nonblocking(driver_script)
    time.sleep(10)
    subprocess.check_call(["ray", "stop"])
    proc.kill()
    time.sleep(1)
    lock.release()
    workflow.init(str(workflow_dir))
    resumed = workflow.resume_all()
    assert len(resumed) == 1
    (wid, obj_ref) = resumed[0]
    assert wid == "cluster_failure"
    assert ray.get(obj_ref) == 20
    workflow.storage.set_global_storage(None)
    ray.shutdown()


@workflow.step
def recursive_chain(x):
    if x < 100:
        return recursive_chain.step(x + 1)
    else:
        return 100


def test_shortcut(workflow_start_regular):
    assert recursive_chain.step(0).run(workflow_id="shortcut") == 100
    # the shortcut points to the step with output checkpoint
    store = workflow_storage.get_workflow_storage("shortcut")
    step_id = store.get_entrypoint_step_id()
    output_step_id = store.inspect_step(step_id).output_step_id
    assert store.inspect_step(output_step_id).output_object_valid


@workflow.step
def constant():
    return 31416


def test_resume_different_storage(ray_start_regular, tmp_path, reset_workflow):
    workflow.init(storage=str(tmp_path))
    constant.step().run(workflow_id="const")
    assert ray.get(workflow.resume(workflow_id="const")) == 31416
    workflow.storage.set_global_storage(None)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
