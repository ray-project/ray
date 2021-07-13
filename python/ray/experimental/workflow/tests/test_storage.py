import pytest
import asyncio
import ray
from ray.tests.conftest import *  # noqa
from ray.experimental.workflow import storage
from ray.experimental.workflow import workflow_storage
import boto3
from moto import mock_s3
from mock_server import *  # noqa


def some_func(x):
    return x + 1


def some_func2(x):
    return x - 1


@pytest.fixture(scope="function")
def filesystem_storage(tmp_path):
    storage.set_global_storage(
        storage.create_storage(f"file:///{str(tmp_path)}/workflow_data"))
    yield storage.get_global_storage()


@pytest.fixture(scope="function")
def aws_credentials():
    import os
    old_env = os.environ
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    yield
    os.environ = old_env


@pytest.fixture(scope="function")
def s3_storage(aws_credentials, s3_server):
    with mock_s3():
        client = boto3.client(
            "s3", region_name="us-west-2", endpoint_url=s3_server)
        client.create_bucket(Bucket="test_bucket")
        url = ("s3://test_bucket/workflow"
               f"?region_name=us-west-2&endpoint_url={s3_server}")
        storage.set_global_storage(storage.create_storage(url))
        yield storage.get_global_storage()


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_storage", [
    pytest.lazy_fixture("filesystem_storage"),
    pytest.lazy_fixture("s3_storage")
])
async def test_raw_storage(ray_start_regular, raw_storage):
    workflow_id = test_workflow_storage.__name__
    step_id = "some_step"
    input_metadata = {"2": "c"}
    output_metadata = {"a": 1}
    args = ([1, "2"], {"k": b"543"})
    output = ["the_answer"]
    object_resolved = 42
    obj_ref = ray.put(object_resolved)

    # test creating normal objects
    await asyncio.gather(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata),
        raw_storage.save_step_func_body(workflow_id, step_id, some_func),
        raw_storage.save_step_args(workflow_id, step_id, args),
        raw_storage.save_object_ref(workflow_id, obj_ref),
        raw_storage.save_step_output_metadata(workflow_id, step_id,
                                              output_metadata),
        raw_storage.save_step_output(workflow_id, step_id, output))

    step_status = await raw_storage.get_step_status(workflow_id, step_id)
    assert step_status.args_exists
    assert step_status.output_object_exists
    assert step_status.output_metadata_exists
    assert step_status.input_metadata_exists
    assert step_status.func_body_exists

    (load_input_metadata, load_step_func_body, load_step_args, load_object_ref,
     load_step_output_meta, load_step_output) = await asyncio.gather(
         raw_storage.load_step_input_metadata(workflow_id, step_id),
         raw_storage.load_step_func_body(workflow_id, step_id),
         raw_storage.load_step_args(workflow_id, step_id),
         raw_storage.load_object_ref(workflow_id, obj_ref.hex()),
         raw_storage.load_step_output_metadata(workflow_id, step_id),
         raw_storage.load_step_output(workflow_id, step_id))
    assert load_input_metadata == input_metadata
    assert load_step_func_body(33) == 34
    assert load_step_args == args
    assert ray.get(load_object_ref) == object_resolved
    assert load_step_output_meta == output_metadata
    assert load_step_output == output

    # test overwrite
    input_metadata = [input_metadata, "overwrite"]
    output_metadata = [output_metadata, "overwrite"]
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
        raw_storage.save_step_output(workflow_id, step_id, output))
    (load_input_metadata, load_step_func_body, load_step_args, load_object_ref,
     load_step_output_meta, load_step_output) = await asyncio.gather(
         raw_storage.load_step_input_metadata(workflow_id, step_id),
         raw_storage.load_step_func_body(workflow_id, step_id),
         raw_storage.load_step_args(workflow_id, step_id),
         raw_storage.load_object_ref(workflow_id, obj_ref.hex()),
         raw_storage.load_step_output_metadata(workflow_id, step_id),
         raw_storage.load_step_output(workflow_id, step_id))
    assert load_input_metadata == input_metadata
    assert load_step_func_body(33) == 32
    assert load_step_args == args
    assert ray.get(load_object_ref) == object_resolved
    assert load_step_output_meta == output_metadata
    assert load_step_output == output


@pytest.mark.parametrize("raw_storage", [
    pytest.lazy_fixture("filesystem_storage"),
    pytest.lazy_fixture("s3_storage")
])
def test_workflow_storage(ray_start_regular, raw_storage):
    workflow_id = test_workflow_storage.__name__
    step_id = "some_step"
    input_metadata = {
        "name": "test_basic_workflows.append1",
        "object_refs": ["abc"],
        "workflows": ["def"]
    }
    output_metadata = {
        "output_step_id": "a12423",
        "dynamic_output_step_id": "b1234"
    }
    args = ([1, "2"], {"k": b"543"})
    output = ["the_answer"]
    object_resolved = 42
    obj_ref = ray.put(object_resolved)

    # test basics
    asyncio.run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    asyncio.run(
        raw_storage.save_step_func_body(workflow_id, step_id, some_func))
    asyncio.run(raw_storage.save_step_args(workflow_id, step_id, args))
    asyncio.run(raw_storage.save_object_ref(workflow_id, obj_ref))
    asyncio.run(
        raw_storage.save_step_output_metadata(workflow_id, step_id,
                                              output_metadata))
    asyncio.run(raw_storage.save_step_output(workflow_id, step_id, output))

    wf_storage = workflow_storage.WorkflowStorage(workflow_id)
    assert wf_storage.load_step_output(step_id) == output
    assert wf_storage.load_step_args(step_id, [], []) == args
    assert wf_storage.load_step_func_body(step_id)(33) == 34
    assert ray.get(wf_storage.load_object_ref(
        obj_ref.hex())) == object_resolved

    # test "inspect_step"
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_object_valid=True)
    assert inspect_result.is_recoverable()

    step_id = "some_step2"
    asyncio.run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    asyncio.run(
        raw_storage.save_step_func_body(workflow_id, step_id, some_func))
    asyncio.run(raw_storage.save_step_args(workflow_id, step_id, args))
    asyncio.run(
        raw_storage.save_step_output_metadata(workflow_id, step_id,
                                              output_metadata))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_step_id=output_metadata["dynamic_output_step_id"])
    assert inspect_result.is_recoverable()

    step_id = "some_step3"
    asyncio.run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    asyncio.run(
        raw_storage.save_step_func_body(workflow_id, step_id, some_func))
    asyncio.run(raw_storage.save_step_args(workflow_id, step_id, args))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        args_valid=True,
        func_body_valid=True,
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"])
    assert inspect_result.is_recoverable()

    step_id = "some_step4"
    asyncio.run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    asyncio.run(
        raw_storage.save_step_func_body(workflow_id, step_id, some_func))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        func_body_valid=True,
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"])
    assert not inspect_result.is_recoverable()

    step_id = "some_step5"
    asyncio.run(
        raw_storage.save_step_input_metadata(workflow_id, step_id,
                                             input_metadata))
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"])
    assert not inspect_result.is_recoverable()

    step_id = "some_step6"
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult()
    assert not inspect_result.is_recoverable()
