from typing import List, Any, Union, Dict, Callable

import ray
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow.common import Workflow, StepID
from ray.experimental.workflow import storage
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow.workflow_manager import WorkflowStepFunction


class WorkflowStepNotRecoverableException(Exception):
    """Raise the exception when we find a workflow step cannot be recovered
    using the checkpointed inputs."""

    def __init__(self, step_id: str):
        self.message = f"Workflow step (id={step_id}) is not recoverable"
        super().__init__(self.message)


@WorkflowStepFunction
def _recover_workflow_step(input_object_refs: List[str],
                           input_workflows: List[Any],
                           instant_workflow_inputs: Dict[int, StepID]):
    """A workflow step that recovers the output of an unfinished step.

    Args:
        input_object_refs: The object refs in the argument of
            the (original) step.
        input_workflows: The workflows in the argument of the (original) step.
            They are resolved into physical objects (i.e. the output of the
            workflows) here. They come from other recover workflows we
            construct recursively.
        instant_workflow_inputs: Same as 'input_workflows', but they come
            point to workflow steps that have output checkpoints. They override
            corresponding workflows in 'input_workflows'.

    Returns:
        The output of the recovered step.
    """
    context = workflow_context.get_workflow_step_context()
    reader = storage.WorkflowStepReader(context.workflow_root_dir,
                                        context.workflow_id)
    for index, _step_id in instant_workflow_inputs.items():
        # override input workflows with instant workflows
        input_workflows[index] = reader.get_step_output(_step_id)
    input_object_refs = [reader.read_object_ref(r) for r in input_object_refs]
    step_id = workflow_context.get_current_step_id()
    with serialization_context.workflow_args_resolving_context(
            input_workflows, input_object_refs):
        args, kwargs = reader.get_step_args(step_id)
    func: Callable = reader.get_func_body(step_id)
    return func(*args, **kwargs)


def _construct_resume_workflow_from_step(
        reader: storage.WorkflowStorageReader,
        step_id: StepID) -> Union[Workflow, StepID]:
    """Try to construct a workflow (step) that recovers the workflow step.
    If the workflow step already has an output checkpointing file, we return
    the workflow step id instead.

    Args:
        reader: The storage reader for inspecting the step.
        step_id: The ID of the step we want to recover.

    Returns:
        A workflow that recovers the step, or a ID of a step
        that contains the output checkpoint file.
    """
    result: storage.StepInspectResult = reader.inspect_step(step_id)
    if result.output_object_valid:
        # we already have the output
        return step_id
    if isinstance(result.output_step_id, str):
        return _construct_resume_workflow_from_step(reader,
                                                    result.output_step_id)
    # output does not exists or not valid. try to reconstruct it.
    if not result.is_recoverable():
        raise WorkflowStepNotRecoverableException(step_id)
    input_workflows = []
    instant_workflow_outputs: Dict[int, str] = {}
    for i, _step_id in enumerate(result.workflows):
        r = _construct_resume_workflow_from_step(reader, _step_id)
        if isinstance(r, Workflow):
            input_workflows.append(r)
        else:
            input_workflows.append(None)
            instant_workflow_outputs[i] = r
    recovery_workflow = _recover_workflow_step.step(
        result.object_refs, input_workflows, instant_workflow_outputs)
    # skip saving the inputs of a recovery workflow step
    recovery_workflow.skip_saving_inputs = True
    recovery_workflow._step_id = step_id
    return recovery_workflow


def resume_workflow_job(
        job_id: str, workflow_root_dir=None) -> Union[ray.ObjectRef, Workflow]:
    """Resume a workflow job.

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
    if isinstance(r, Workflow):
        return r
    else:
        return ray.put(reader.read_step_output(r))
