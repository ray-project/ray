import time

from ray.tests.conftest import *  # noqa

import pytest
import ray
from ray import workflow


def test_user_metadata(workflow_start_regular):

    user_step_metadata = {"k1": "v1"}
    user_run_metadata = {"k2": "v2"}
    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.options(name=step_name, metadata=user_step_metadata).step().run(
        workflow_id, metadata=user_run_metadata
    )

    assert workflow.get_metadata("simple")["user_metadata"] == user_run_metadata
    assert (
        workflow.get_metadata("simple", "simple_step")["user_metadata"]
        == user_step_metadata
    )


def test_user_metadata_empty(workflow_start_regular):

    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.options(name=step_name).step().run(workflow_id)

    assert workflow.get_metadata("simple")["user_metadata"] == {}
    assert workflow.get_metadata("simple", "simple_step")["user_metadata"] == {}


def test_user_metadata_not_dict(workflow_start_regular):
    @workflow.step
    def simple():
        return 0

    with pytest.raises(ValueError):
        simple.options(metadata="x")

    with pytest.raises(ValueError):
        simple.step().run(metadata="x")


def test_user_metadata_not_json_serializable(workflow_start_regular):
    @workflow.step
    def simple():
        return 0

    class X:
        pass

    with pytest.raises(ValueError):
        simple.options(metadata={"x": X()})

    with pytest.raises(ValueError):
        simple.step().run(metadata={"x": X()})


def test_runtime_metadata(workflow_start_regular):

    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        time.sleep(2)
        return 0

    simple.options(name=step_name).step().run(workflow_id)

    workflow_metadata = workflow.get_metadata("simple")
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]
    assert (
        workflow_metadata["stats"]["end_time"]
        >= workflow_metadata["stats"]["start_time"] + 2
    )

    step_metadata = workflow.get_metadata("simple", "simple_step")
    assert "start_time" in step_metadata["stats"]
    assert "end_time" in step_metadata["stats"]
    assert (
        step_metadata["stats"]["end_time"] >= step_metadata["stats"]["start_time"] + 2
    )


def test_successful_workflow(workflow_start_regular):

    user_step_metadata = {"k1": "v1"}
    user_run_metadata = {"k2": "v2"}
    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        time.sleep(2)
        return 0

    simple.options(name=step_name, metadata=user_step_metadata).step().run(
        workflow_id, metadata=user_run_metadata
    )

    workflow_metadata = workflow.get_metadata("simple")
    assert workflow_metadata["status"] == "SUCCESSFUL"
    assert workflow_metadata["user_metadata"] == user_run_metadata
    assert "start_time" in workflow_metadata["stats"]
    assert "end_time" in workflow_metadata["stats"]
    assert (
        workflow_metadata["stats"]["end_time"]
        >= workflow_metadata["stats"]["start_time"] + 2
    )

    step_metadata = workflow.get_metadata("simple", "simple_step")
    assert step_metadata["user_metadata"] == user_step_metadata
    assert "start_time" in step_metadata["stats"]
    assert "end_time" in step_metadata["stats"]
    assert (
        step_metadata["stats"]["end_time"] >= step_metadata["stats"]["start_time"] + 2
    )


def test_running_and_canceled_workflow(workflow_start_regular, tmp_path):

    workflow_id = "simple"
    flag = tmp_path / "flag"

    @workflow.step
    def simple():
        flag.touch()
        time.sleep(1000)
        return 0

    simple.step().run_async(workflow_id)

    # Wait until step runs to make sure pre-run metadata is written
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

    @workflow.step
    def simple():
        if error_flag.exists():
            raise ValueError()
        return 0

    with pytest.raises(ray.exceptions.RaySystemError):
        simple.step().run(workflow_id)

    workflow_metadata_failed = workflow.get_metadata(workflow_id)
    assert workflow_metadata_failed["status"] == "FAILED"

    error_flag.unlink()
    ref = workflow.resume(workflow_id)
    assert ray.get(ref) == 0

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
    @workflow.step(name="inner", metadata={"inner_k": "inner_v"})
    def inner():
        time.sleep(2)
        return 10

    @workflow.step(name="outer", metadata={"outer_k": "outer_v"})
    def outer():
        time.sleep(2)
        return inner.step()

    outer.step().run("nested", metadata={"workflow_k": "workflow_v"})

    workflow_metadata = workflow.get_metadata("nested")
    outer_step_metadata = workflow.get_metadata("nested", "outer")
    inner_step_metadata = workflow.get_metadata("nested", "inner")

    assert workflow_metadata["user_metadata"] == {"workflow_k": "workflow_v"}
    assert outer_step_metadata["user_metadata"] == {"outer_k": "outer_v"}
    assert inner_step_metadata["user_metadata"] == {"inner_k": "inner_v"}

    assert (
        workflow_metadata["stats"]["end_time"]
        >= workflow_metadata["stats"]["start_time"] + 4
    )
    assert (
        outer_step_metadata["stats"]["end_time"]
        >= outer_step_metadata["stats"]["start_time"] + 2
    )
    assert (
        inner_step_metadata["stats"]["end_time"]
        >= inner_step_metadata["stats"]["start_time"] + 2
    )
    assert (
        inner_step_metadata["stats"]["start_time"]
        >= outer_step_metadata["stats"]["end_time"]
    )


def test_simple_virtual_actor(workflow_start_regular):
    @workflow.virtual_actor
    class Actor:
        def __init__(self, v):
            self.v = v

        def add_v(self, v):
            time.sleep(1)
            self.v += v
            return self.v

        @workflow.virtual_actor.readonly
        def get_v(self):
            return self.v

    actor = Actor.get_or_create("vid", 0)
    actor.add_v.options(name="add", metadata={"k1": "v1"}).run(10)
    actor.add_v.options(name="add", metadata={"k2": "v2"}).run(10)
    actor.add_v.options(name="add", metadata={"k3": "v3"}).run(10)

    assert workflow.get_metadata("vid", "add")["user_metadata"] == {"k1": "v1"}
    assert workflow.get_metadata("vid", "add_1")["user_metadata"] == {"k2": "v2"}
    assert workflow.get_metadata("vid", "add_2")["user_metadata"] == {"k3": "v3"}
    assert (
        workflow.get_metadata("vid", "add")["stats"]["end_time"]
        >= workflow.get_metadata("vid", "add")["stats"]["start_time"] + 1
    )
    assert (
        workflow.get_metadata("vid", "add_1")["stats"]["end_time"]
        >= workflow.get_metadata("vid", "add_1")["stats"]["start_time"] + 1
    )
    assert (
        workflow.get_metadata("vid", "add_2")["stats"]["end_time"]
        >= workflow.get_metadata("vid", "add_2")["stats"]["start_time"] + 1
    )


def test_nested_virtual_actor(workflow_start_regular):
    @workflow.virtual_actor
    class Counter:
        def __init__(self):
            self.n = 0

        def incr(self, n):
            self.n += 1
            if n - 1 > 0:
                return self.incr.options(
                    name="incr", metadata={"current_n": self.n}
                ).step(n - 1)
            else:
                return self.n

        @workflow.virtual_actor.readonly
        def get(self):
            return self.n

    counter = Counter.get_or_create("counter")
    counter.incr.options(name="incr").run(5)

    assert workflow.get_metadata("counter", "incr_1")["user_metadata"] == {
        "current_n": 1
    }
    assert workflow.get_metadata("counter", "incr_2")["user_metadata"] == {
        "current_n": 2
    }
    assert workflow.get_metadata("counter", "incr_3")["user_metadata"] == {
        "current_n": 3
    }
    assert workflow.get_metadata("counter", "incr_4")["user_metadata"] == {
        "current_n": 4
    }


def test_no_workflow_found(workflow_start_regular):

    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.options(name=step_name).step().run(workflow_id)

    with pytest.raises(ValueError) as excinfo:
        workflow.get_metadata("simple1")
    assert str(excinfo.value) == "No such workflow_id simple1"

    with pytest.raises(ValueError) as excinfo:
        workflow.get_metadata("simple1", "simple_step")
    assert str(excinfo.value) == "No such workflow_id simple1"

    with pytest.raises(ValueError) as excinfo:
        workflow.get_metadata("simple", "simple_step1")
    assert str(excinfo.value) == "No such step_id simple_step1 in workflow simple"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
