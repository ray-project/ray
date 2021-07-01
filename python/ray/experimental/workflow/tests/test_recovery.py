import os
import subprocess
import sys
import time
import pytest

import ray
from ray.exceptions import RaySystemError
from ray.experimental import workflow
from ray.experimental.workflow.tests import utils
from ray.experimental.workflow.recovery import WorkflowNotResumableError
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


def test_recovery_simple():
    ray.init()
    utils.unset_global_mark()
    workflow_id = "test_recovery_simple"
    with pytest.raises(RaySystemError):
        # internally we get WorkerCrashedError
        output = workflow.run(simple.step("x"), workflow_id=workflow_id)
        ray.get(output)
    utils.set_global_mark()
    output = workflow.resume(workflow_id)
    assert ray.get(output) == "foo(x[append1])[append2]"
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    output = workflow.resume(workflow_id)
    assert ray.get(output) == "foo(x[append1])[append2]"
    ray.shutdown()


def test_recovery_complex():
    ray.init()
    utils.unset_global_mark()
    workflow_id = "test_recovery_complex"
    with pytest.raises(RaySystemError):
        # internally we get WorkerCrashedError
        output = workflow.run(complex.step("x"), workflow_id=workflow_id)
        ray.get(output)
    utils.set_global_mark()
    output = workflow.resume(workflow_id)
    r = "join(join(foo(x[append1]), [source1][append2]), join(x, [source1]))"
    assert ray.get(output) == r
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    output = workflow.resume(workflow_id)
    r = "join(join(foo(x[append1]), [source1][append2]), join(x, [source1]))"
    assert ray.get(output) == r
    ray.shutdown()


def test_recovery_non_exists_workflow():
    ray.init()
    with pytest.raises(WorkflowNotResumableError):
        workflow.resume("this_workflow_id_does_not_exist")
    ray.shutdown()


def test_recovery_cluster_failure():
    subprocess.run(["ray start --head"], shell=True)
    time.sleep(1)
    script = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "workflows_to_fail.py")
    proc = subprocess.Popen([sys.executable, script])
    time.sleep(10)
    subprocess.run(["ray stop"], shell=True)
    proc.kill()
    time.sleep(1)
    ray.init()
    assert ray.get(workflow.resume("cluster_failure")) == 20
    ray.shutdown()


@workflow.step
def recursive_chain(x):
    if x < 100:
        return recursive_chain.step(x + 1)
    else:
        return 100


def test_shortcut():
    ray.init()
    output = workflow.run(recursive_chain.step(0), workflow_id="shortcut")
    assert ray.get(output) == 100
    # the shortcut points to the step with output checkpoint
    store = workflow_storage.WorkflowStorage("shortcut")
    step_id = store.get_entrypoint_step_id()
    assert store.inspect_step(step_id).output_object_valid
    ray.shutdown()
