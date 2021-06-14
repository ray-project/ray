import pathlib
from typing import List, Any, Union, Dict

import ray
from ray.experimental.workflow.constants import (STEPS_DIR, OBJECTS_DIR,
                                                 TASK_BODY_FILE)
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow import storage
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow.workflow_manager import WorkflowStepFunction

import ray.cloudpickle


class WorkflowStepNotRecoverableException(Exception):
    def __init__(self, step_id: str):
        self.message = f"Workflow step (id={step_id}) is not recoverable"
        super().__init__(self.message)


def _load_output_object(job_path: pathlib.Path,
                        object_id_hex: str) -> ray.ObjectRef:
    object_file = job_path / OBJECTS_DIR / object_id_hex
    with open(object_file, "rb") as f:
        obj = ray.cloudpickle.load(f)
    return obj


@ray.remote
def _load_output_object_remote(job_path: pathlib.Path,
                               object_id_hex: str) -> ray.ObjectRef:
    return _load_output_object(job_path, object_id_hex)


@WorkflowStepFunction
def _recover_workflow_step(
        workflow_root_dir: pathlib.Path, workflow_id: str, step_id: str,
        input_placeholder_id: str, input_object_refs: List[str],
        workflow_results: List[Any], instant_workflow_outputs: Dict[int, str]):
    # NOTE: this overrides the workflow context, this changes the way
    # checkpointing behaves.
    context = workflow_context.WorkflowStepContext(workflow_id,
                                                   workflow_root_dir)
    job_path = workflow_root_dir / workflow_id
    workflow_context.update_workflow_step_context(context, step_id)
    for index, object_id in instant_workflow_outputs.items():
        workflow_results[index] = _load_output_object(job_path, object_id)
    input_object_refs = [
        _load_output_object_remote.remote(r) for r in input_object_refs
    ]
    with serialization_context.workflow_args_resolving_context(
            workflow_results, input_object_refs):
        args, kwargs = _load_output_object(job_path, input_placeholder_id)

    task_body_file = job_path / STEPS_DIR / step_id / TASK_BODY_FILE
    with open(task_body_file, "rb") as f:
        func = ray.cloudpickle.load(f)
    return func(*args, **kwargs)


def _construct_resume_workflow_from_step(reader: storage.WorkflowStorageReader,
                                         step_id: str) -> Union[Workflow, str]:
    result: storage.StepInspectResult = reader.inspect_step(step_id)
    if not result.is_recoverable():
        raise WorkflowStepNotRecoverableException(step_id)
    if result.output_type == "object":
        if result.output_object_valid:
            return result.output_object_id
    elif result.output_type == "workflow":
        if isinstance(result.output_step_id, str):
            return _construct_resume_workflow_from_step(
                reader, result.output_step_id)
    # output does not exists or not valid. reconstruct it.
    input_workflows = []
    instant_workflow_outputs: Dict[int, str] = {}
    for i, _step_id in enumerate(result.input_workflows):
        r = _construct_resume_workflow_from_step(reader, _step_id)
        if isinstance(r, Workflow):
            input_workflows.append(r)
        else:
            input_workflows.append(None)
            instant_workflow_outputs[i] = r
    recovery_workflow = _recover_workflow_step.step(
        reader.workflow_root_dir, reader.job_id, step_id,
        result.input_placeholder, result.input_object_refs, input_workflows,
        instant_workflow_outputs)
    # skip saving the inputs of a recovery workflow step
    recovery_workflow.skip_saving_inputs = True
    recovery_workflow._step_id = step_id
    return recovery_workflow


def resume_workflow_job(
        job_id: str, workflow_root_dir=None) -> Union[ray.ObjectRef, Workflow]:
    """
    Resume a workflow job.

    Args:
        job_id: The ID of the workflow job. The ID is used to identify
            the workflow.
        workflow_root_dir: The path of an external storage used for
            checkpointing.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    reader = storage.WorkflowStorageReader(job_id, workflow_root_dir)
    r = _construct_resume_workflow_from_step(reader, reader.entrypoint_step_id)
    # TODO(suquark): maybe not need to override "steps/outputs.json"?
    if isinstance(r, Workflow):
        return r
    else:
        return ray.put(reader.read_object(r))
