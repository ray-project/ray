from ray.tests.conftest import *  # noqa

import pytest
from ray import workflow
from ray.workflow.common import WorkflowRef


@workflow.step
def incr(x):
    return x + 1


def test_dynamic_workflow_ref(workflow_start_regular_shared):
    # This test also shows different "style" of running workflows.
    first_step = incr.step(0)
    assert first_step.run("test_dynamic_workflow_ref") == 1
    second_step = incr.step(WorkflowRef(first_step.step_id))
    # Without rerun, it'll just return the previous result
    assert second_step.run("test_dynamic_workflow_ref") == 1
    # TODO (yic) We need re-run to make this test work
    # assert second_step.run("test_dynamic_workflow_ref") == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
