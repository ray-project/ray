from ray.tests.conftest import *  # noqa

import pytest

import ray
from ray import workflow
from ray.workflow.common import WorkflowRef


def test_dynamic_workflow_ref(workflow_start_regular_shared):
    @ray.remote
    def incr(x):
        return x + 1

    # This test also shows different "style" of running workflows.
    assert workflow.run(incr.bind(0), workflow_id="test_dynamic_workflow_ref") == 1
    # Without rerun, it'll just return the previous result
    assert (
        workflow.run(
            incr.bind(WorkflowRef("incr")), workflow_id="test_dynamic_workflow_ref"
        )
        == 1
    )
    # TODO (yic) We need re-run to make this test work
    # assert second_task.run("test_dynamic_workflow_ref") == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
