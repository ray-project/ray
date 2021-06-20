import ray
from ray.experimental.workflow import storage
from ray.experimental.workflow import workflow_storage


def some_func(x):
    return x + 1


def some_func2(x):
    return x - 1


def test_raw_storage():
    ray.init()
    workflow_id = test_workflow_storage.__name__
    raw_storage = storage.get_global_storage()
    step_id = "some_step"
    input_metadata = {"2": "c"}
    output_metadata = {"a": 1}
    args = ([1, "2"], {"k": b"543"})
    output = ["the_answer"]
    object_resolved = 42
    rref = ray.put(object_resolved)

    # test creating normal objects
    raw_storage.save_step_input_metadata(workflow_id, step_id, input_metadata)
    raw_storage.save_step_func_body(workflow_id, step_id, some_func)
    raw_storage.save_step_args(workflow_id, step_id, args)
    raw_storage.save_object_ref(workflow_id, rref)
    raw_storage.save_step_output_metadata(workflow_id, step_id,
                                          output_metadata)
    raw_storage.save_step_output(workflow_id, step_id, output)

    step_status = raw_storage.get_step_status(workflow_id, step_id)
    assert step_status.args_exists
    assert step_status.output_object_exists
    assert step_status.output_metadata_exists
    assert step_status.input_metadata_exists
    assert step_status.func_body_exists

    assert raw_storage.load_step_input_metadata(workflow_id,
                                                step_id) == input_metadata
    assert raw_storage.load_step_func_body(workflow_id, step_id)(33) == 34
    assert raw_storage.load_step_args(workflow_id, step_id) == args
    assert ray.get(raw_storage.load_object_ref(workflow_id,
                                               rref.hex())) == object_resolved
    assert raw_storage.load_step_output_metadata(workflow_id,
                                                 step_id) == output_metadata
    assert raw_storage.load_step_output(workflow_id, step_id) == output

    # test overwrite
    input_metadata = [input_metadata, "overwrite"]
    output_metadata = [output_metadata, "overwrite"]
    args = (args, "overwrite")
    output = (output, "overwrite")
    object_resolved = (object_resolved, "overwrite")
    rref = ray.put(object_resolved)

    raw_storage.save_step_input_metadata(workflow_id, step_id, input_metadata)
    raw_storage.save_step_func_body(workflow_id, step_id, some_func2)
    raw_storage.save_step_args(workflow_id, step_id, args)
    raw_storage.save_object_ref(workflow_id, rref)
    raw_storage.save_step_output_metadata(workflow_id, step_id,
                                          output_metadata)
    raw_storage.save_step_output(workflow_id, step_id, output)
    assert raw_storage.load_step_input_metadata(workflow_id,
                                                step_id) == input_metadata
    assert raw_storage.load_step_func_body(workflow_id, step_id)(33) == 32
    assert raw_storage.load_step_args(workflow_id, step_id) == args
    assert ray.get(raw_storage.load_object_ref(workflow_id,
                                               rref.hex())) == object_resolved
    assert raw_storage.load_step_output_metadata(workflow_id,
                                                 step_id) == output_metadata
    assert raw_storage.load_step_output(workflow_id, step_id) == output

    ray.shutdown()


def test_workflow_storage():
    ray.init()
    workflow_id = test_workflow_storage.__name__
    raw_storage = storage.get_global_storage()
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
    rref = ray.put(object_resolved)

    # test basics
    raw_storage.save_step_input_metadata(workflow_id, step_id, input_metadata)
    raw_storage.save_step_func_body(workflow_id, step_id, some_func)
    raw_storage.save_step_args(workflow_id, step_id, args)
    raw_storage.save_object_ref(workflow_id, rref)
    raw_storage.save_step_output_metadata(workflow_id, step_id,
                                          output_metadata)
    raw_storage.save_step_output(workflow_id, step_id, output)

    wf_storage = workflow_storage.WorkflowStorage(workflow_id)
    assert wf_storage.load_step_output(step_id) == output
    assert wf_storage.load_step_args(step_id, [], []) == args
    assert wf_storage.load_step_func_body(step_id)(33) == 34
    assert ray.get(wf_storage.load_object_ref(rref.hex())) == object_resolved

    # test "inspect_step"
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_object_valid=True)
    assert inspect_result.is_recoverable()

    step_id = "some_step2"
    raw_storage.save_step_input_metadata(workflow_id, step_id, input_metadata)
    raw_storage.save_step_func_body(workflow_id, step_id, some_func)
    raw_storage.save_step_args(workflow_id, step_id, args)
    raw_storage.save_step_output_metadata(workflow_id, step_id,
                                          output_metadata)
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        output_step_id=output_metadata["dynamic_output_step_id"])
    assert inspect_result.is_recoverable()

    step_id = "some_step3"
    raw_storage.save_step_input_metadata(workflow_id, step_id, input_metadata)
    raw_storage.save_step_func_body(workflow_id, step_id, some_func)
    raw_storage.save_step_args(workflow_id, step_id, args)
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        args_valid=True,
        func_body_valid=True,
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"])
    assert inspect_result.is_recoverable()

    step_id = "some_step4"
    raw_storage.save_step_input_metadata(workflow_id, step_id, input_metadata)
    raw_storage.save_step_func_body(workflow_id, step_id, some_func)
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        func_body_valid=True,
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"])
    assert not inspect_result.is_recoverable()

    step_id = "some_step5"
    raw_storage.save_step_input_metadata(workflow_id, step_id, input_metadata)
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult(
        object_refs=input_metadata["object_refs"],
        workflows=input_metadata["workflows"])
    assert not inspect_result.is_recoverable()

    step_id = "some_step6"
    inspect_result = wf_storage.inspect_step(step_id)
    assert inspect_result == workflow_storage.StepInspectResult()
    assert not inspect_result.is_recoverable()

    ray.shutdown()
