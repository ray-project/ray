import pytest
import ray
from ray._private import signature
from ray.tests.conftest import *  # noqa
from ray import workflow
from ray.workflow import workflow_storage
from ray.workflow import storage
from ray.workflow.storage.s3 import S3StorageImpl
from ray.workflow.workflow_storage import asyncio_run
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


@pytest.mark.asyncio
async def test_kv_storage(workflow_start_regular):
    kv_store = storage.get_global_storage()
    json_data = {"hello": "world"}
    bin_data = (31416).to_bytes(8, "big")
    key_1 = kv_store.make_key("aaa", "bbb", "ccc")
    key_2 = kv_store.make_key("aaa", "ddd")
    key_3 = kv_store.make_key("aaa", "eee")
    await kv_store.put(key_1, json_data, is_json=True)
    await kv_store.put(key_2, bin_data, is_json=False)
    assert json_data == await kv_store.get(key_1, is_json=True)
    assert bin_data == await kv_store.get(key_2, is_json=False)
    with pytest.raises(storage.KeyNotFoundError):
        await kv_store.get(key_3)
    prefix = kv_store.make_key("aaa")
    assert set(await kv_store.scan_prefix(prefix)) == {"bbb", "ddd"}
    assert set(await kv_store.scan_prefix(kv_store.make_key(""))) == {"aaa"}
    # TODO(suquark): Test "delete" once fully implemented.


def test_delete(workflow_start_regular):
    _storage = storage.get_global_storage()

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
    workflow.init(storage=_storage)

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
    wf_storage = workflow_storage.WorkflowStorage(
        workflow_id, storage.get_global_storage()
    )
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
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata, True
        )
    )
    asyncio_run(wf_storage._put(wf_storage._key_step_function_body(step_id), some_func))
    asyncio_run(wf_storage._put(wf_storage._key_step_args(step_id), flattened_args))

    asyncio_run(
        wf_storage._put(wf_storage._key_obj_id(obj_ref.hex()), ray.get(obj_ref))
    )
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_output_metadata(step_id), output_metadata, True
        )
    )
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_output_metadata(""), root_output_metadata, True
        )
    )
    asyncio_run(wf_storage._put(wf_storage._key_step_output(step_id), output))

    assert wf_storage.load_step_output(step_id) == output
    assert wf_storage.load_step_args(step_id, [], []) == args
    assert wf_storage.load_step_func_body(step_id)(33) == 34
    assert ray.get(wf_storage.load_object_ref(obj_ref.hex())) == object_resolved

    # test s3 path
    # here we hardcode the path to make sure s3 path is parsed correctly
    if isinstance(wf_storage._storage, S3StorageImpl):
        assert (
            asyncio_run(
                wf_storage._storage.get(
                    "workflow/test_workflow_storage/steps/outputs.json", True
                )
            )
            == root_output_metadata
        )

    # test "inspect_step"
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_object_valid=True
    )
    assert inspect_result.is_recoverable()

    step_id = "some_step2"
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata, True
        )
    )
    asyncio_run(wf_storage._put(wf_storage._key_step_function_body(step_id), some_func))
    asyncio_run(wf_storage._put(wf_storage._key_step_args(step_id), args))
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_output_metadata(step_id), output_metadata, True
        )
    )

    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_step_id=output_metadata["dynamic_output_step_id"]
    )
    assert inspect_result.is_recoverable()

    step_id = "some_step3"
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata, True
        )
    )
    asyncio_run(wf_storage._put(wf_storage._key_step_function_body(step_id), some_func))
    asyncio_run(wf_storage._put(wf_storage._key_step_args(step_id), args))
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
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata, True
        )
    )
    asyncio_run(wf_storage._put(wf_storage._key_step_function_body(step_id), some_func))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        func_body_valid=True,
        workflows=input_metadata["workflows"],
        workflow_refs=input_metadata["workflow_refs"],
        step_options=step_options,
    )
    assert not inspect_result.is_recoverable()

    step_id = "some_step5"
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata, True
        )
    )
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
