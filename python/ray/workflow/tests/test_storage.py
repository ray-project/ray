import pytest
import ray
from ray._private import signature
from ray.tests.conftest import *  # noqa
from ray import workflow
from ray.workflow import workflow_storage
from ray.workflow.common import (
    StepType,
    WorkflowStepRuntimeOptions,
    WorkflowNotFoundError,
)
from ray.workflow.tests import utils
import subprocess
import time


def some_func(x):
    return x + 1


def some_func2(x):
    return x - 1


def test_delete(workflow_start_regular):
    from ray.internal.storage import _storage_uri

    # Try deleting a random workflow that never existed.
    with pytest.raises(WorkflowNotFoundError):
        workflow.delete(workflow_id="never_existed")

    # Delete a workflow that has not finished and is not running.
    @ray.remote
    def never_ends(x):
        utils.set_global_mark()
        time.sleep(1000000)
        return x

    workflow.create(never_ends.bind("hello world")).run_async("never_finishes")

    # Make sure the step is actualy executing before killing the cluster
    while not utils.check_global_mark():
        time.sleep(0.1)

    # Restart
    ray.shutdown()
    subprocess.check_output("ray stop --force", shell=True)
    ray.init(storage=_storage_uri)
    workflow.init()

    with pytest.raises(ray.exceptions.RaySystemError):
        result = workflow.get_output("never_finishes")
        ray.get(result)

    workflow.delete("never_finishes")

    with pytest.raises(ValueError):
        ouput = workflow.get_output("never_finishes")

    # TODO(Alex): Uncomment after
    # https://github.com/ray-project/ray/issues/19481.
    # with pytest.raises(WorkflowNotFoundError):
    #     workflow.resume("never_finishes")

    with pytest.raises(WorkflowNotFoundError):
        workflow.delete(workflow_id="never_finishes")

    # Delete a workflow which has finished.
    @ray.remote
    def basic_step(arg):
        return arg

    result = workflow.create(basic_step.bind("hello world")).run(workflow_id="finishes")
    assert result == "hello world"
    ouput = workflow.get_output("finishes")
    assert ray.get(ouput) == "hello world"

    workflow.delete(workflow_id="finishes")

    with pytest.raises(ValueError):
        ouput = workflow.get_output("finishes")

    # TODO(Alex): Uncomment after
    # https://github.com/ray-project/ray/issues/19481.
    # with pytest.raises(ValueError):
    #     workflow.resume("finishes")

    with pytest.raises(WorkflowNotFoundError):
        workflow.delete(workflow_id="finishes")

    assert workflow.list_all() == []

    # The workflow can be re-run as if it was never run before.
    assert workflow.create(basic_step.bind("123")).run(workflow_id="finishes") == "123"

    # utils.unset_global_mark()
    # never_ends.step("123").run_async(workflow_id="never_finishes")
    # while not utils.check_global_mark():
    #     time.sleep(0.1)

    # assert workflow.get_status("never_finishes") == \
    #     workflow.WorkflowStatus.RUNNING

    # with pytest.raises(WorkflowRunningError):
    #     workflow.delete("never_finishes")

    # assert workflow.get_status("never_finishes") == \
    #     workflow.WorkflowStatus.RUNNING


def test_workflow_storage(workflow_start_regular):
    workflow_id = test_workflow_storage.__name__
    wf_storage = workflow_storage.WorkflowStorage(workflow_id)
    step_id = "some_step"
    step_options = WorkflowStepRuntimeOptions.make(step_type=StepType.FUNCTION)
    input_metadata = {
        "name": "test_basic_workflows.append1",
        "workflows": ["def"],
        "workflow_refs": ["some_ref"],
        "step_options": step_options.to_dict(),
    }
    output_metadata = {"output_step_id": "a12423", "dynamic_output_step_id": "b1234"}
    root_output_metadata = {"output_step_id": "c123"}
    flattened_args = [signature.DUMMY_TYPE, 1, signature.DUMMY_TYPE, "2", "k", b"543"]
    args = signature.recover_args(flattened_args)
    output = ["the_answer"]
    object_resolved = 42
    obj_ref = ray.put(object_resolved)

    # test basics
    wf_storage._put(wf_storage._key_step_input_metadata(step_id), input_metadata, True)

    wf_storage._put(wf_storage._key_step_function_body(step_id), some_func)
    wf_storage._put(wf_storage._key_step_args(step_id), flattened_args)

    wf_storage._put(wf_storage._key_obj_id(obj_ref.hex()), ray.get(obj_ref))
    wf_storage._put(
        wf_storage._key_step_output_metadata(step_id), output_metadata, True
    )
    wf_storage._put(
        wf_storage._key_step_output_metadata(""), root_output_metadata, True
    )
    wf_storage._put(wf_storage._key_step_output(step_id), output)

    assert wf_storage.load_step_output(step_id) == output
    assert wf_storage.load_step_args(step_id, [], []) == args
    assert wf_storage.load_step_func_body(step_id)(33) == 34
    assert ray.get(wf_storage.load_object_ref(obj_ref.hex())) == object_resolved

    # test s3 path
    # here we hardcode the path to make sure s3 path is parsed correctly
    from ray.internal.storage import _storage_uri

    if _storage_uri.startswith("s3://"):
        assert wf_storage._get("steps/outputs.json", True) == root_output_metadata

    # test "inspect_step"
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_object_valid=True
    )
    assert inspect_result.is_recoverable()

    step_id = "some_step2"
    wf_storage._put(wf_storage._key_step_input_metadata(step_id), input_metadata, True)
    wf_storage._put(wf_storage._key_step_function_body(step_id), some_func)
    wf_storage._put(wf_storage._key_step_args(step_id), args)
    wf_storage._put(
        wf_storage._key_step_output_metadata(step_id), output_metadata, True
    )

    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_step_id=output_metadata["dynamic_output_step_id"]
    )
    assert inspect_result.is_recoverable()

    step_id = "some_step3"
    wf_storage._put(wf_storage._key_step_input_metadata(step_id), input_metadata, True)
    wf_storage._put(wf_storage._key_step_function_body(step_id), some_func)
    wf_storage._put(wf_storage._key_step_args(step_id), args)
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        args_valid=True,
        func_body_valid=True,
        workflows=input_metadata["workflows"],
        workflow_refs=input_metadata["workflow_refs"],
        step_options=step_options,
    )
    assert inspect_result.is_recoverable()

    step_id = "some_step4"
    wf_storage._put(wf_storage._key_step_input_metadata(step_id), input_metadata, True)

    wf_storage._put(wf_storage._key_step_function_body(step_id), some_func)
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        func_body_valid=True,
        workflows=input_metadata["workflows"],
        workflow_refs=input_metadata["workflow_refs"],
        step_options=step_options,
    )
    assert not inspect_result.is_recoverable()

    step_id = "some_step5"
    wf_storage._put(wf_storage._key_step_input_metadata(step_id), input_metadata, True)

    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        workflows=input_metadata["workflows"],
        workflow_refs=input_metadata["workflow_refs"],
        step_options=step_options,
    )
    assert not inspect_result.is_recoverable()

    step_id = "some_step6"
    inspect_result = wf_storage.inspect_step(step_id)
    print(inspect_result)
    assert inspect_result == workflow_storage.StepInspectResult()
    assert not inspect_result.is_recoverable()


def test_cluster_storage_init(workflow_start_cluster, tmp_path):
    address, storage_uri = workflow_start_cluster

    err_msg = "When connecting to an existing cluster, "
    "storage must not be provided."

    with pytest.raises(ValueError, match=err_msg):
        ray.init(address=address, storage=str(tmp_path))

    with pytest.raises(ValueError, match=err_msg):
        ray.init(address=address, storage=storage_uri)

    ray.init(address=address)

    @workflow.step
    def f():
        return 10

    assert f.step().run() == 10


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
