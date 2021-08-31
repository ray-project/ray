import pytest
import ray
from ray._private import signature
from ray.tests.conftest import *  # noqa
from ray.experimental import workflow
from ray.experimental.workflow import workflow_storage
from ray.experimental.workflow import storage
from ray.experimental.workflow.workflow_storage import asyncio_run, \
    get_workflow_storage
from ray.experimental.workflow.common import StepType
import subprocess


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


def test_workflow_storage(workflow_start_regular):
    workflow_id = test_workflow_storage.__name__
    wf_storage = workflow_storage.WorkflowStorage(workflow_id,
                                                  storage.get_global_storage())
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
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata,
            True))
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_function_body(step_id), some_func))
    asyncio_run(
        wf_storage._put(wf_storage._key_step_args(step_id), flattened_args))

    asyncio_run(
        wf_storage._put(
            wf_storage._key_obj_id(obj_ref.hex()), ray.get(obj_ref)))
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_output_metadata(step_id), output_metadata,
            True))
    asyncio_run(wf_storage._put(wf_storage._key_step_output(step_id), output))

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
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata,
            True))
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_function_body(step_id), some_func))
    asyncio_run(wf_storage._put(wf_storage._key_step_args(step_id), args))
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_output_metadata(step_id), output_metadata,
            True))

    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_step_id=output_metadata["dynamic_output_step_id"])
    assert inspect_result.is_recoverable()

    step_id = "some_step3"
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata,
            True))
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_function_body(step_id), some_func))
    asyncio_run(wf_storage._put(wf_storage._key_step_args(step_id), args))
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
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata,
            True))
    asyncio_run(
        wf_storage._put(
            wf_storage._key_step_function_body(step_id), some_func))
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
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), input_metadata,
            True))
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


def test_embedded_objectrefs(workflow_start_regular):
    workflow_id = test_workflow_storage.__name__

    class ObjectRefsWrapper:
        def __init__(self, refs):
            self.refs = refs

    wf_storage = workflow_storage.WorkflowStorage(workflow_id,
                                                  storage.get_global_storage())
    url = storage.get_global_storage().storage_url

    wrapped = ObjectRefsWrapper([ray.put(1), ray.put(2)])

    asyncio_run(wf_storage._put(["key"], wrapped))

    # Be extremely explicit about shutting down. We want to make sure the
    # `_get` call deserializes the full object and puts it in the object store.
    # Shutting down the cluster should guarantee we don't accidently get the
    # old object and pass the test.
    ray.shutdown()
    subprocess.check_output("ray stop --force", shell=True)

    workflow.init(url)
    storage2 = get_workflow_storage(workflow_id)

    result = asyncio_run(storage2._get(["key"]))
    assert ray.get(result.refs) == [1, 2]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
