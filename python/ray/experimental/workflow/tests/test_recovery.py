import pytest

import ray
from ray.exceptions import RayTaskError
from ray.experimental import workflow
from ray.experimental.workflow.tests import utils


@workflow.step
def the_failed_step(x):
    if not utils.check_global_mark():
        import os
        os.kill(os.getpid(), 9)
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
def join(x, y):
    return f"join({x}, {y})"


@workflow.step
def fork_join(x1):
    x2 = source1.step()
    v = join.step(x1, x2)
    y = append1.step(x1)
    y = the_failed_step.step(y)
    z = append2.step(x2)
    u = join.step(y, z)
    return join.step(u, v)


def test_recovery():
    ray.init()
    utils.unset_global_mark()
    workflow_id = "test_recovery"
    with pytest.raises(RayTaskError):
        # internally we get WorkerCrashedError
        output = workflow.run(fork_join.step("x"), workflow_id=workflow_id)
        ray.get(output)

    utils.set_global_mark()
    output = workflow.resume(workflow_id)
    r = "join(join(x[append1], [source1][append2]), join(x, [source1]))"
    assert ray.get(output) == r
    ray.shutdown()
