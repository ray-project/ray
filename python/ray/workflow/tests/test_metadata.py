import time

from ray.tests.conftest import *  # noqa

import pytest
import ray
from ray import workflow


def test_user_metadata(workflow_start_regular):

    user_task_metadata = {"k1": "v1"}
    user_run_metadata = {"k2": "v2"}
    task_id = "simple_task"
    workflow_id = "simple"

    @workflow.options(task_id=task_id, metadata=user_task_metadata)
    @ray.remote
    def simple():
        return 0

    workflow.run(simple.bind(), workflow_id=workflow_id, metadata=user_run_metadata)

    assert workflow.get_metadata("simple")["user_metadata"] == user_run_metadata
    assert (
        workflow.get_metadata("simple", "simple_task")["user_metadata"]
        == user_task_metadata
    )


def test_user_metadata_empty(workflow_start_regular):

    task_id = "simple_task"
    workflow_id = "simple"

    @workflow.options(task_id=task_id)
    @ray.remote
    def simple():
        return 0

    workflow.run(simple.bind(), workflow_id=workflow_id)

    assert workflow.get_metadata("simple")["user_metadata"] == {}
    assert workflow.get_metadata("simple", "simple_task")["user_metadata"] == {}


def test_user_metadata_not_dict(workflow_start_regular):
    @ray.remote
    def simple():
        return 0

    with pytest.raises(ValueError):
        workflow.run_async(simple.options(**workflow.options(metadata="x")).bind())

    with pytest.raises(ValueError):
        workflow.run(simple.bind(), metadata="x")


def test_user_metadata_not_json_serializable(workflow_start_regular):
    @ray.remote
    def simple():
        return 0

    class X:
        pass

    with pytest.raises(ValueError):
        workflow.run_async(
            simple.options(**workflow.options(metadata={"x": X()})).bind()
        )

    with pytest.raises(ValueError):
        workflow.run(simple.bind(), metadata={"x": X()})


def test_runtime_metadata(workflow_start_regular):

    task_id = "simple_task"
    workflow_id = "simple"

    @workflow.options(task_id=task_id)
    @ray.remote
    def simple():
        time.sleep(2)
        return 0

    workflow.run(simple.bind(), workflow_id=workflow_id)

    workflow_metadata = workflow.get_metadata("simple")
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]
    assert (
        workflow_metadata["stats"]["end_time"]
        >= workflow_metadata["stats"]["start_time"] + 2
    )

    task_metadata = workflow.get_metadata("simple", "simple_task")
    assert "start_time" in task_metadata["stats"]
    assert "end_time" in task_metadata["stats"]
    assert (
        task_metadata["stats"]["end_time"] >= task_metadata["stats"]["start_time"] + 2
    )


def test_successful_workflow(workflow_start_regular):

    user_task_metadata = {"k1": "v1"}
    user_run_metadata = {"k2": "v2"}
    task_id = "simple_task"
    workflow_id = "simple"

    @workflow.options(task_id=task_id, metadata=user_task_metadata)
    @ray.remote
    def simple():
        time.sleep(2)
        return 0

    workflow.run(simple.bind(), workflow_id=workflow_id, metadata=user_run_metadata)

    workflow_metadata = workflow.get_metadata("simple")
    assert workflow_metadata["status"] == "SUCCESSFUL"
    assert workflow_metadata["user_metadata"] == user_run_metadata
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]
    assert (
        workflow_metadata["stats"]["end_time"]
        >= workflow_metadata["stats"]["start_time"] + 2
    )

    task_metadata = workflow.get_metadata("simple", "simple_task")
    assert task_metadata["user_metadata"] == user_task_metadata
    assert "start_time" in task_metadata["stats"]
    assert "end_time" in task_metadata["stats"]
    assert (
        task_metadata["stats"]["end_time"] >= task_metadata["stats"]["start_time"] + 2
    )


def test_running_and_canceled_workflow(workflow_start_regular, tmp_path):

    workflow_id = "simple"
    flag = tmp_path / "flag"

    @ray.remote
    def simple():
        flag.touch()
        time.sleep(1000)
        return 0

    workflow.run_async(simple.bind(), workflow_id=workflow_id)

    # Wait until task runs to make sure pre-run metadata is written
    while not flag.exists():
        time.sleep(1)

    workflow_metadata = workflow.get_metadata(workflow_id)
    assert workflow_metadata["status"] == "RUNNING"
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" not in workflow_metadata["stats"]

    workflow.cancel(workflow_id)

    workflow_metadata = workflow.get_metadata(workflow_id)
    assert workflow_metadata["status"] == "CANCELED"
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" not in workflow_metadata["stats"]


def test_failed_and_resumed_workflow(workflow_start_regular, tmp_path):

    workflow_id = "simple"
    error_flag = tmp_path / "error"
    error_flag.touch()

    @ray.remote
    def simple():
        if error_flag.exists():
            raise ValueError()
        return 0

    with pytest.raises(workflow.WorkflowExecutionError):
        workflow.run(simple.bind(), workflow_id=workflow_id)

    workflow_metadata_failed = workflow.get_metadata(workflow_id)
    assert workflow_metadata_failed["status"] == "FAILED"

    error_flag.unlink()
    assert workflow.resume(workflow_id) == 0

    workflow_metadata_resumed = workflow.get_metadata(workflow_id)
    assert workflow_metadata_resumed["status"] == "SUCCESSFUL"

    # make sure resume updated running metrics
    assert (
        workflow_metadata_resumed["stats"]["start_time"]
        > workflow_metadata_failed["stats"]["start_time"]
    )
    assert (
        workflow_metadata_resumed["stats"]["end_time"]
        > workflow_metadata_failed["stats"]["end_time"]
    )


def test_nested_workflow(workflow_start_regular):
    @workflow.options(task_id="inner", metadata={"inner_k": "inner_v"})
    @ray.remote
    def inner():
        time.sleep(2)
        return 10

    @workflow.options(task_id="outer", metadata={"outer_k": "outer_v"})
    @ray.remote
    def outer():
        time.sleep(2)
        return workflow.continuation(inner.bind())

    workflow.run(
        outer.bind(), workflow_id="nested", metadata={"workflow_k": "workflow_v"}
    )

    workflow_metadata = workflow.get_metadata("nested")
    outer_task_metadata = workflow.get_metadata("nested", "outer")
    inner_task_metadata = workflow.get_metadata("nested", "inner")

    assert workflow_metadata["user_metadata"] == {"workflow_k": "workflow_v"}
    assert outer_task_metadata["user_metadata"] == {"outer_k": "outer_v"}
    assert inner_task_metadata["user_metadata"] == {"inner_k": "inner_v"}

    assert (
        workflow_metadata["stats"]["end_time"]
        >= workflow_metadata["stats"]["start_time"] + 4
    )
    assert (
        outer_task_metadata["stats"]["end_time"]
        >= outer_task_metadata["stats"]["start_time"] + 2
    )
    assert (
        inner_task_metadata["stats"]["end_time"]
        >= inner_task_metadata["stats"]["start_time"] + 2
    )
    assert (
        inner_task_metadata["stats"]["start_time"]
        >= outer_task_metadata["stats"]["end_time"]
    )


def test_no_workflow_found(workflow_start_regular):

    task_id = "simple_task"
    workflow_id = "simple"

    @workflow.options(task_id=task_id)
    @ray.remote
    def simple():
        return 0

    workflow.run(simple.bind(), workflow_id=workflow_id)

    with pytest.raises(ValueError, match="No such workflow_id 'simple1'"):
        workflow.get_metadata("simple1")

    with pytest.raises(ValueError, match="No such workflow_id 'simple1'"):
        workflow.get_metadata("simple1", "simple_task")

    with pytest.raises(
        ValueError, match="No such task_id 'simple_task1' in workflow 'simple'"
    ):
        workflow.get_metadata("simple", "simple_task1")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
