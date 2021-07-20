import subprocess
import shutil
import tempfile
import time

from ray.tests.conftest import *  # noqa
import pytest

import ray
from ray.test_utils import run_string_as_driver_nonblocking
from ray.exceptions import RaySystemError, RayTaskError
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


def test_recovery_simple(ray_start_regular, raw_storage):
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


def test_recovery_complex(ray_start_regular, raw_storage):
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


def test_recovery_non_exists_workflow(ray_start_regular, raw_storage):
    with pytest.raises(RayTaskError):
        ray.get(workflow.resume("this_workflow_id_does_not_exist"))


driver_script = """
import time
import ray
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
    ray.init(address="auto")
    assert foo.step(0).run(workflow_id="cluster_failure") == 20
"""


# def test_recovery_cluster_failure():
#     subprocess.run(["ray start --head"], shell=True)
#     time.sleep(1)
#     proc = run_string_as_driver_nonblocking(driver_script)
#     time.sleep(10)
#     subprocess.run(["ray stop"], shell=True)
#     proc.kill()
#     time.sleep(1)
#     ray.init()
#     assert ray.get(workflow.resume("cluster_failure")) == 20
#     ray.shutdown()


@workflow.step
def recursive_chain(x):
    if x < 100:
        print(x)
        return recursive_chain.step(x + 1)
    else:
        return 100


def test_shortcut(ray_start_regular, raw_storage):
    assert recursive_chain.step(0).run(workflow_id="shortcut") == 100
    # the shortcut points to the step with output checkpoint
    store = workflow_storage.WorkflowStorage("shortcut")
    step_id = store.get_entrypoint_step_id()
    assert store.inspect_step(step_id).output_object_valid


@workflow.step
def constant_1():
    return 271828


@workflow.step
def constant_2():
    return 31416


def test_resume_different_storage(ray_start_regular, tmp_path):
    constant_1.step().run(workflow_id="const")
    tmp_dir = (tmp_path / "tempfile").mkdir()
    constant_2.step().run(workflow_id="const", storage=str(tmp_dir))
    assert ray.get(workflow.resume(workflow_id="const",
                                   storage=str(tmp_dir))) == 31416
