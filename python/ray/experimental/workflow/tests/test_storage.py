import pytest
import asyncio
import ray
from ray._private import signature
from ray.tests.conftest import *  # noqa
from ray.experimental.workflow import workflow_storage
from ray.experimental.workflow import storage
from ray.experimental.workflow.workflow_storage import asyncio_run
from ray.experimental.workflow.common import StepType


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


@pytest.mark.asyncio
async def test_raw_storage(workflow_start_regular):
    raw_storage = workflow_storage._StorageImpl(storage.get_global_storage())
    workflow_id = test_workflow_storage.__name__
    step_id = "some_step"
    input_metadata = {"2": "c"}
    output_metadata = {"a": 1}
    args = ([1, "2"], {"k": b"543"})
    output = ["the_answer"]
    object_resolved = 42
    obj_ref = ray.put(object_resolved)
    progress_metadata = {"step_id": "the_current_progress"}
    # test creating normal objects
    await asyncio.gather(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata),
        raw_storage.save_step_func_body(workflow_id, step_id, some_func),
        raw_storage.save_step_args(workflow_id, step_id, args),
        raw_storage.save_object_ref(workflow_id, obj_ref),
        raw_storage.save_step_output_metadata(workflow_id, step_id,
                                              output_metadata),
        raw_storage.save_step_output(workflow_id, step_id, output),
        raw_storage.save_workflow_progress(workflow_id, progress_metadata))

    step_status = await raw_storage.get_step_status(workflow_id, step_id)
    assert step_status.args_exists
    assert step_status.output_object_exists
    assert step_status.output_metadata_exists
    assert step_status.input_metadata_exists
    assert step_status.func_body_exists

    (load_input_metadata, load_step_func_body, load_step_args, load_object_ref,
     load_step_output_meta, load_step_output,
     load_workflow_progress) = await asyncio.gather(
         raw_storage.load_step_input_metadata(workflow_id, step_id),
         raw_storage.load_step_func_body(workflow_id, step_id),
         raw_storage.load_step_args(workflow_id, step_id),
         raw_storage.load_object_ref(workflow_id, obj_ref.hex()),
         raw_storage.load_step_output_metadata(workflow_id, step_id),
         raw_storage.load_step_output(workflow_id, step_id),
         raw_storage.load_workflow_progress(workflow_id))
    assert load_input_metadata == input_metadata
    assert load_step_func_body(33) == 34
    assert load_step_args == args
    assert ray.get(load_object_ref) == object_resolved
    assert load_step_output_meta == output_metadata
    assert load_step_output == output
    assert load_workflow_progress == progress_metadata

    # test overwrite
    input_metadata = [input_metadata, "overwrite"]
    output_metadata = [output_metadata, "overwrite"]
    progress_metadata = {"step_id": "overwrite"}
    args = (args, "overwrite")
    output = (output, "overwrite")
    object_resolved = (object_resolved, "overwrite")
    obj_ref = ray.put(object_resolved)

    await asyncio.gather(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata),
        raw_storage.save_step_func_body(workflow_id, step_id, some_func2),
        raw_storage.save_step_args(workflow_id, step_id, args),
        raw_storage.save_object_ref(workflow_id, obj_ref),
        raw_storage.save_step_output_metadata(workflow_id, step_id,
                                              output_metadata),
        raw_storage.save_step_output(workflow_id, step_id, output),
        raw_storage.save_workflow_progress(workflow_id, progress_metadata))
    (load_input_metadata, load_step_func_body, load_step_args, load_object_ref,
     load_step_output_meta, load_step_output,
     load_workflow_progress) = await asyncio.gather(
         raw_storage.load_step_input_metadata(workflow_id, step_id),
         raw_storage.load_step_func_body(workflow_id, step_id),
         raw_storage.load_step_args(workflow_id, step_id),
         raw_storage.load_object_ref(workflow_id, obj_ref.hex()),
         raw_storage.load_step_output_metadata(workflow_id, step_id),
         raw_storage.load_step_output(workflow_id, step_id),
         raw_storage.load_workflow_progress(workflow_id))
    assert load_input_metadata == input_metadata
    assert load_step_func_body(33) == 32
    assert load_step_args == args
    assert ray.get(load_object_ref) == object_resolved
    assert load_step_output_meta == output_metadata
    assert load_step_output == output
    assert load_workflow_progress == progress_metadata


def test_workflow_storage(workflow_start_regular):
    raw_storage = workflow_storage._StorageImpl(storage.get_global_storage())
    workflow_id = test_workflow_storage.__name__
    step_id = "some_step"
    input_metadata = {
        "name": "test_basic_workflows.append1",
        "step_type": StepType.FUNCTION,
        "object_refs": ["abc"],
        "workflows": ["def"],
        "workflow_refs": ["some_ref"],
        "max_retries": 1,
        "catch_exceptions": False,
        "ray_options": {},
    }
    output_metadata = {
        "output_step_id": "a12423",
        "dynamic_output_step_id": "b1234"
    }
    flattened_args = [
        signature.DUMMY_TYPE, 1, signature.DUMMY_TYPE, "2", "k", b"543"
    ]
    args = signature.recover_args(flattened_args)
    output = ["the_answer"]
    object_resolved = 42
    obj_ref = ray.put(object_resolved)

    # test basics
    asyncio_run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    asyncio_run(
        raw_storage.save_step_func_body(workflow_id, step_id, some_func))
    asyncio_run(
        raw_storage.save_step_args(workflow_id, step_id, flattened_args))
    asyncio_run(raw_storage.save_object_ref(workflow_id, obj_ref))
    asyncio_run(
        raw_storage.save_step_output_metadata(workflow_id, step_id,
                                              output_metadata))
    asyncio_run(raw_storage.save_step_output(workflow_id, step_id, output))

    wf_storage = workflow_storage.WorkflowStorage(workflow_id,
                                                  storage.get_global_storage())
    assert wf_storage.load_step_output(step_id) == output
    assert wf_storage.load_step_args(step_id, [], [], []) == args
    assert wf_storage.load_step_func_body(step_id)(33) == 34
    assert ray.get(wf_storage.load_object_ref(
        obj_ref.hex())) == object_resolved

    # test "inspect_step"
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_object_valid=True)
    assert inspect_result.is_recoverable()

    step_id = "some_step2"
    asyncio_run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    asyncio_run(
        raw_storage.save_step_func_body(workflow_id, step_id, some_func))
    asyncio_run(raw_storage.save_step_args(workflow_id, step_id, args))
    asyncio_run(
        raw_storage.save_step_output_metadata(workflow_id, step_id,
                                              output_metadata))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_step_id=output_metadata["dynamic_output_step_id"])
    assert inspect_result.is_recoverable()

    step_id = "some_step3"
    asyncio_run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    asyncio_run(
        raw_storage.save_step_func_body(workflow_id, step_id, some_func))
    asyncio_run(raw_storage.save_step_args(workflow_id, step_id, args))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        step_type=StepType.FUNCTION,
        args_valid=True,
        func_body_valid=True,
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"],
        workflow_refs=input_metadata["workflow_refs"],
        ray_options={})
    assert inspect_result.is_recoverable()

    step_id = "some_step4"
    asyncio_run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    asyncio_run(
        raw_storage.save_step_func_body(workflow_id, step_id, some_func))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        step_type=StepType.FUNCTION,
        func_body_valid=True,
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"],
        workflow_refs=input_metadata["workflow_refs"],
        ray_options={})
    assert not inspect_result.is_recoverable()

    step_id = "some_step5"
    asyncio_run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        step_type=StepType.FUNCTION,
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"],
        workflow_refs=input_metadata["workflow_refs"],
        ray_options={})
    assert not inspect_result.is_recoverable()

    step_id = "some_step6"
    inspect_result = wf_storage.inspect_step(step_id)
    print(inspect_result)
    assert inspect_result == workflow_storage.StepInspectResult()
    assert not inspect_result.is_recoverable()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
