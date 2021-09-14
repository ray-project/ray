import asyncio
from typing import List, Any, Union, Dict, Callable, Tuple, Optional

import ray
from ray.workflow import workflow_context
from ray.workflow.common import (Workflow, StepID, WorkflowRef,
                                 WorkflowExecutionResult)
from ray.workflow import storage
from ray.workflow import workflow_storage
from ray.workflow.step_function import WorkflowStepFunction


class WorkflowStepNotRecoverableError(Exception):
    """Raise the exception when we find a workflow step cannot be recovered
    using the checkpointed inputs."""

    def __init__(self, step_id: StepID):
        self.message = f"Workflow step[id={step_id}] is not recoverable"
        super().__init__(self.message)


class WorkflowNotResumableError(Exception):
    """Raise the exception when we cannot resume from a workflow."""

    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] is not resumable."
        super().__init__(self.message)


@WorkflowStepFunction
def _recover_workflow_step(input_object_refs: List[ray.ObjectRef],
                           input_workflows: List[Any],
                           input_workflow_refs: List[WorkflowRef],
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
    reader = workflow_storage.get_workflow_storage()
    for index, _step_id in instant_workflow_inputs.items():
        # override input workflows with instant workflows
        input_workflows[index] = reader.load_step_output(_step_id)

    step_id = workflow_context.get_current_step_id()
    func: Callable = reader.load_step_func_body(step_id)
    args, kwargs = reader.load_step_args(
        step_id, input_workflows, input_object_refs, input_workflow_refs)
    return func(*args, **kwargs)


def _construct_resume_workflow_from_step(
        reader: workflow_storage.WorkflowStorage,
        step_id: StepID,
        objectref_cache: Dict[str, Any] = None) -> Union[Workflow, StepID]:
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
    if objectref_cache is None:
        objectref_cache = {}
    result: workflow_storage.StepInspectResult = reader.inspect_step(step_id)
    if result.output_object_valid:
        # we already have the output
        return step_id
    if isinstance(result.output_step_id, str):
        return _construct_resume_workflow_from_step(
            reader, result.output_step_id, objectref_cache=objectref_cache)
    # output does not exists or not valid. try to reconstruct it.
    if not result.is_recoverable():
        raise WorkflowStepNotRecoverableError(step_id)
    input_workflows = []
    instant_workflow_outputs: Dict[int, str] = {}
    for i, _step_id in enumerate(result.workflows):
        r = _construct_resume_workflow_from_step(
            reader, _step_id, objectref_cache=objectref_cache)
        if isinstance(r, Workflow):
            input_workflows.append(r)
        else:
            input_workflows.append(None)
            instant_workflow_outputs[i] = r
    workflow_refs = list(map(WorkflowRef, result.workflow_refs))

    # TODO (Alex): Refactor to remove this special case handling of object refs
    resolved_object_refs = []
    identifiers_to_await = []
    promises_to_await = []

    for identifier in result.object_refs:
        if identifier not in objectref_cache:
            paths = reader._key_step_args(identifier)
            promise = reader._get(paths)
            promises_to_await.append(promise)
            identifiers_to_await.append(identifier)

    loop = asyncio.get_event_loop()
    object_refs_to_cache = loop.run_until_complete(
        asyncio.gather(*promises_to_await))

    for identifier, object_ref in zip(identifiers_to_await,
                                      object_refs_to_cache):
        objectref_cache[identifier] = object_ref

    for identifier in result.object_refs:
        resolved_object_refs.append(objectref_cache[identifier])

    recovery_workflow: Workflow = _recover_workflow_step.options(
        max_retries=result.max_retries,
        catch_exceptions=result.catch_exceptions,
        **result.ray_options).step(resolved_object_refs, input_workflows,
                                   workflow_refs, instant_workflow_outputs)
    recovery_workflow._step_id = step_id
    recovery_workflow.data.step_type = result.step_type
    return recovery_workflow


@ray.remote(num_returns=2)
def _resume_workflow_step_executor(workflow_id: str, step_id: "StepID",
                                   store_url: str, current_output: [
                                       ray.ObjectRef
                                   ]) -> Tuple[ray.ObjectRef, ray.ObjectRef]:
    # TODO (yic): We need better dependency management for virtual actor
    # The current output will always be empty for normal workflow
    # For virtual actor, if it's not empty, it means the previous job is
    # running. This is a really bad one.
    for ref in current_output:
        try:
            while isinstance(ref, ray.ObjectRef):
                ref = ray.get(ref)
        except Exception:
            pass
    try:
        store = storage.create_storage(store_url)
        wf_store = workflow_storage.WorkflowStorage(workflow_id, store)
        r = _construct_resume_workflow_from_step(wf_store, step_id)
    except Exception as e:
        raise WorkflowNotResumableError(workflow_id) from e

    if isinstance(r, Workflow):
        with workflow_context.workflow_step_context(workflow_id,
                                                    store.storage_url):
            from ray.workflow.step_executor import (execute_workflow)
            result = execute_workflow(r, last_step_of_workflow=True)
            return result.persisted_output, result.volatile_output
    assert isinstance(r, StepID)
    return wf_store.load_step_output(r), None


def resume_workflow_step(
        workflow_id: str, step_id: "StepID", store_url: str,
        current_output: Optional[ray.ObjectRef]) -> WorkflowExecutionResult:
    """Resume a step of a workflow.

    Args:
        workflow_id: The ID of the workflow job. The ID is used to identify
            the workflow.
        step_id: The step to resume in the workflow.
        store_url: The url of the storage to access the workflow.

    Raises:
        WorkflowNotResumableException: fail to resume the workflow.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    if current_output is None:
        current_output = []
    else:
        current_output = [current_output]

    persisted_output, volatile_output = _resume_workflow_step_executor.remote(
        workflow_id, step_id, store_url, current_output)
    return WorkflowExecutionResult(persisted_output, volatile_output)


def get_latest_output(workflow_id: str, store: storage.Storage) -> Any:
    """Get the latest output of a workflow. This function is intended to be
    used by readonly virtual actors. To resume a workflow,
    `resume_workflow_job` should be used instead.

    Args:
        workflow_id: The ID of the workflow.
        store: The storage of the workflow.

    Returns:
        The output of the workflow.
    """
    reader = workflow_storage.WorkflowStorage(workflow_id, store)
    try:
        step_id: StepID = reader.get_latest_progress()
        while True:
            result: workflow_storage.StepInspectResult = reader.inspect_step(
                step_id)
            if result.output_object_valid:
                # we already have the output
                return reader.load_step_output(step_id)
            if isinstance(result.output_step_id, str):
                step_id = result.output_step_id
            else:
                raise ValueError(
                    "Workflow output does not exists or not valid.")
    except Exception as e:
        raise WorkflowNotResumableError(workflow_id) from e
