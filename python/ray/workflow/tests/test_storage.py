import subprocess
import time

import pytest

import ray
from ray import workflow
from ray._private import signature
from ray.tests.conftest import *  # noqa
from ray.workflow import workflow_storage
from ray.workflow.common import (
    TaskType,
    WorkflowTaskRuntimeOptions,
)
from ray.workflow.exceptions import WorkflowNotFoundError
from ray.workflow import serialization_context
from ray.workflow.tests import utils


def some_func(x):
    return x + 1


def some_func2(x):
    return x - 1


def test_delete_1(workflow_start_regular):
    with pytest.raises(WorkflowNotFoundError):
        workflow.delete(workflow_id="never_existed")

    @ray.remote
    def hello():
        return "hello world"

    workflow.run(hello.bind(), workflow_id="workflow_exists")
    workflow.delete(workflow_id="workflow_exists")


def test_delete_2(workflow_start_regular):
    from ray._private.storage import _storage_uri
    from ray.workflow.tests.utils import skip_client_mode_test

    # This test restarts the cluster, so we cannot test under client mode.
    skip_client_mode_test()

    # Delete a workflow that has not finished and is not running.
    @ray.remote
    def never_ends(x):
        utils.set_global_mark()
        time.sleep(1000000)
        return x

    workflow.run_async(never_ends.bind("hello world"), workflow_id="never_finishes")

    # Make sure the task is actualy executing before killing the cluster
    while not utils.check_global_mark():
        time.sleep(0.1)

    # Restart
    ray.shutdown()
    subprocess.check_output("ray stop --force", shell=True)
    ray.init(storage=_storage_uri)
    workflow.init()

    with pytest.raises(ValueError):
        workflow.get_output("never_finishes")

    workflow.delete("never_finishes")

    with pytest.raises(ValueError):
        # TODO(suquark): we should raise "ValueError" without
        #  been blocking over the result.
        workflow.get_output("never_finishes")

    # TODO(Alex): Uncomment after
    # https://github.com/ray-project/ray/issues/19481.
    # with pytest.raises(WorkflowNotFoundError):
    #     workflow.resume("never_finishes")

    with pytest.raises(WorkflowNotFoundError):
        workflow.delete(workflow_id="never_finishes")

    # Delete a workflow which has finished.
    @ray.remote
    def basic_task(arg):
        return arg

    result = workflow.run(basic_task.bind("hello world"), workflow_id="finishes")
    assert result == "hello world"
    assert workflow.get_output("finishes") == "hello world"

    workflow.delete(workflow_id="finishes")

    with pytest.raises(ValueError):
        # TODO(suquark): we should raise "ValueError" without
        #  blocking over the result.
        workflow.get_output("finishes")

    # TODO(Alex): Uncomment after
    # https://github.com/ray-project/ray/issues/19481.
    # with pytest.raises(ValueError):
    #     workflow.resume("finishes")

    with pytest.raises(WorkflowNotFoundError):
        workflow.delete(workflow_id="finishes")

    assert workflow.list_all() == []

    # The workflow can be re-run as if it was never run before.
    assert workflow.run(basic_task.bind("123"), workflow_id="finishes") == "123"

    # utils.unset_global_mark()
    # never_ends.task("123").run_async(workflow_id="never_finishes")
    # while not utils.check_global_mark():
    #     time.sleep(0.1)

    # assert workflow.get_status("never_finishes") == \
    #     workflow.WorkflowStatus.RUNNING

    # with pytest.raises(WorkflowRunningError):
    #     workflow.delete("never_finishes")

    # assert workflow.get_status("never_finishes") == \
    #     workflow.WorkflowStatus.RUNNING


def test_workflow_storage(workflow_start_regular):
    from ray.workflow.tests.utils import skip_client_mode_test

    # This test depends on raw storage, so we cannot test under client mode.
    skip_client_mode_test()

    workflow_id = test_workflow_storage.__name__
    wf_storage = workflow_storage.WorkflowStorage(workflow_id)
    task_id = "some_task"
    task_options = WorkflowTaskRuntimeOptions(
        task_type=TaskType.FUNCTION,
        catch_exceptions=False,
        retry_exceptions=True,
        max_retries=0,
        checkpoint=False,
        ray_options={},
    )
    input_metadata = {
        "name": "test_basic_workflows.append1",
        "workflow_refs": ["some_ref"],
        "task_options": task_options.to_dict(),
    }
    output_metadata = {"output_task_id": "a12423", "dynamic_output_task_id": "b1234"}
    root_output_metadata = {"output_task_id": "c123"}
    flattened_args = [signature.DUMMY_TYPE, 1, signature.DUMMY_TYPE, "2", "k", b"543"]
    args = signature.recover_args(flattened_args)
    output = ["the_answer"]
    object_resolved = 42
    obj_ref = ray.put(object_resolved)

    # test basics
    wf_storage._put(wf_storage._key_task_input_metadata(task_id), input_metadata, True)

    wf_storage._put(wf_storage._key_task_function_body(task_id), some_func)
    wf_storage._put(wf_storage._key_task_args(task_id), flattened_args)

    wf_storage._put(wf_storage._key_obj_id(obj_ref.hex()), ray.get(obj_ref))
    wf_storage._put(
        wf_storage._key_task_output_metadata(task_id), output_metadata, True
    )
    wf_storage._put(
        wf_storage._key_task_output_metadata(""), root_output_metadata, True
    )
    wf_storage._put(wf_storage._key_task_output(task_id), output)

    assert wf_storage.load_task_output(task_id) == output

    with serialization_context.workflow_args_resolving_context([]):
        assert (
            signature.recover_args(ray.get(wf_storage.load_task_args(task_id))) == args
        )
    assert wf_storage.load_task_func_body(task_id)(33) == 34
    assert ray.get(wf_storage.load_object_ref(obj_ref.hex())) == object_resolved

    # test s3 path
    # here we hardcode the path to make sure s3 path is parsed correctly
    from ray._private.storage import _storage_uri

    if _storage_uri.startswith("s3://"):
        assert wf_storage._get("tasks/outputs.json", True) == root_output_metadata

    # test "inspect_task"
    inspect_result = wf_storage.inspect_task(task_id)
    assert inspect_result == workflow_storage.TaskInspectResult(
        output_object_valid=True
    )
    assert inspect_result.is_recoverable()

    task_id = "some_task2"
    wf_storage._put(wf_storage._key_task_input_metadata(task_id), input_metadata, True)
    wf_storage._put(wf_storage._key_task_function_body(task_id), some_func)
    wf_storage._put(wf_storage._key_task_args(task_id), args)
    wf_storage._put(
        wf_storage._key_task_output_metadata(task_id), output_metadata, True
    )

    inspect_result = wf_storage.inspect_task(task_id)
    assert inspect_result == workflow_storage.TaskInspectResult(
        output_task_id=output_metadata["dynamic_output_task_id"]
    )
    assert inspect_result.is_recoverable()

    task_id = "some_task3"
    wf_storage._put(wf_storage._key_task_input_metadata(task_id), input_metadata, True)
    wf_storage._put(wf_storage._key_task_function_body(task_id), some_func)
    wf_storage._put(wf_storage._key_task_args(task_id), args)
    inspect_result = wf_storage.inspect_task(task_id)
    assert inspect_result == workflow_storage.TaskInspectResult(
        args_valid=True,
        func_body_valid=True,
        workflow_refs=input_metadata["workflow_refs"],
        task_options=task_options,
    )
    assert inspect_result.is_recoverable()

    task_id = "some_task4"
    wf_storage._put(wf_storage._key_task_input_metadata(task_id), input_metadata, True)

    wf_storage._put(wf_storage._key_task_function_body(task_id), some_func)
    inspect_result = wf_storage.inspect_task(task_id)
    assert inspect_result == workflow_storage.TaskInspectResult(
        func_body_valid=True,
        workflow_refs=input_metadata["workflow_refs"],
        task_options=task_options,
    )
    assert not inspect_result.is_recoverable()

    task_id = "some_task5"
    wf_storage._put(wf_storage._key_task_input_metadata(task_id), input_metadata, True)

    inspect_result = wf_storage.inspect_task(task_id)
    assert inspect_result == workflow_storage.TaskInspectResult(
        workflow_refs=input_metadata["workflow_refs"],
        task_options=task_options,
    )
    assert not inspect_result.is_recoverable()

    task_id = "some_task6"
    inspect_result = wf_storage.inspect_task(task_id)
    print(inspect_result)
    assert inspect_result == workflow_storage.TaskInspectResult()
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

    @ray.remote
    def f():
        return 10

    assert workflow.run(f.bind()) == 10


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
