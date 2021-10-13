import time

from ray import workflow
from ray.tests.conftest import *  # noqa

import pytest


def test_user_metadata(workflow_start_regular):

    user_step_metadata = {"k1": "v1"}
    user_run_metadata = {"k2": "v2"}
    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.options(
        name=step_name, metadata=user_step_metadata).step().run(
            workflow_id, metadata=user_run_metadata)

    assert user_run_metadata == workflow.get_metadata("simple")["user_metadata"]
    assert user_step_metadata == workflow.get_metadata("simple", "simple_step")["user_metadata"]


def test_no_user_metadata(workflow_start_regular):

    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.options(name=step_name).step().run(workflow_id)

    assert {} == workflow.get_metadata("simple")["user_metadata"]
    assert {} == workflow.get_metadata("simple", "simple_step")["user_metadata"]


def test_successful_workflow(workflow_start_regular):

    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.options(name=step_name).step().run(workflow_id)

    workflow_metadata = workflow.get_metadata("simple")
    assert "SUCCESSFUL" == workflow_metadata["status"]
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]

    step_metadata = workflow.get_metadata("simple", "simple_step")
    assert "start_time" in step_metadata["stats"]
    assert "end_time" in step_metadata["stats"]


def test_running_and_canceled_workflow(workflow_start_regular):

    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        time.sleep(1000)
        return 0

    simple.options(name=step_name).step().run_async(workflow_id)
    time.sleep(10)

    workflow_metadata = workflow.get_metadata("simple")
    assert "RUNNING" == workflow_metadata["status"]
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" not in workflow_metadata["stats"]

    step_metadata = workflow.get_metadata("simple", "simple_step")
    assert "start_time" in step_metadata["stats"]
    assert "end_time" not in step_metadata["stats"]

    workflow.cancel(workflow_id)

    workflow_metadata = workflow.get_metadata("simple")
    assert "CANCELED" == workflow_metadata["status"]
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" not in workflow_metadata["stats"]

    step_metadata = workflow.get_metadata("simple", "simple_step")
    assert "start_time" in step_metadata["stats"]
    assert "end_time" not in step_metadata["stats"]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
