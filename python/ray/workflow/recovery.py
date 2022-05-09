from typing import List, Any, Union, Dict, Callable, Tuple, Optional

import ray
from ray.workflow import workflow_context
from ray.workflow import serialization
from ray.workflow.common import (
    Workflow,
    StepID,
    WorkflowRef,
    WorkflowStaticRef,
    WorkflowExecutionResult,
    StepType,
)
from ray.workflow import workflow_storage


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


def _construct_resume_workflow_from_step(
    workflow_id: str, step_id: StepID
) -> Union[Workflow, Any]:
    """Try to construct a workflow (step) that recovers the workflow step.
    If the workflow step already has an output checkpointing file, we return
    the workflow step id instead.

    Args:
        workflow_id: The ID of the workflow.
        step_id: The ID of the step we want to recover.

    Returns:
        A workflow that recovers the step, or a ID of a step
        that contains the output checkpoint file.
    """
    reader = workflow_storage.WorkflowStorage(workflow_id)

    # Step 1: collect all workflow steps to be recovered
    result: workflow_storage.StepInspectResult = reader.inspect_step(step_id)
    if not result.is_recoverable():
        raise WorkflowStepNotRecoverableError(step_id)
    stack: List[Tuple(StepID, workflow_storage.StepInspectResult)] = [(step_id, result)]
    inspected_steps = {step_id}
    pointer = 0
    while pointer < len(stack):
        _, result = stack[pointer]
        pointer += 1
        if result.output_object_valid:
            # The step is already checkpointed. Skip.
            continue
        if isinstance(result.output_step_id, str):
            # The output of this step is held by another
            # step.
            steps_to_inspect = [result.output_step_id]
        else:
            steps_to_inspect = result.workflows
        for _step_id in steps_to_inspect:
            if _step_id in inspected_steps:
                continue
            inspected_steps.add(_step_id)
            r = reader.inspect_step(_step_id)
            if not r.is_recoverable():
                raise WorkflowStepNotRecoverableError(_step_id)
            stack.append((_step_id, r))

    # Step 2: recover steps in the reversed order in the stack
    with serialization.objectref_cache():
        # "input_map" is a context storing the input which has been loaded.
        # This context is important for deduplicate step inputs.
        input_map: Dict[StepID, Any] = {}

        while stack:
            _step_id, result = stack.pop()
            if result.output_object_valid:
                input_map[_step_id] = reader.load_step_output(_step_id)
                continue
            if isinstance(result.output_step_id, str):
                input_map[_step_id] = input_map[result.output_step_id]
                continue

            # Process the wait step as a special case.
            if result.step_options.step_type == StepType.WAIT:
                wait_input_workflows = []
                for w in result.workflows:
                    output = input_map[w]
                    if isinstance(output, Workflow):
                        wait_input_workflows.append(output)
                    else:
                        # Simulate a workflow with a workflow reference so it could be
                        # used directly by 'workflow.wait'.
                        static_ref = WorkflowStaticRef(step_id=w, ref=ray.put(output))
                        wait_input_workflows.append(Workflow.from_ref(static_ref))
                recovery_workflow = ray.workflow.wait(
                    wait_input_workflows,
                    **result.step_options.ray_options.get("wait_options", {}),
                )
            else:
                args, kwargs = reader.load_step_args(
                    _step_id,
                    workflows=[input_map[w] for w in result.workflows],
                    workflow_refs=list(map(WorkflowRef, result.workflow_refs)),
                )
                func: Callable = reader.load_step_func_body(_step_id)
                # TODO(suquark): Use an alternative function when "workflow.step"
                # is fully deprecated.
                recovery_workflow = ray.workflow.step(func).step(*args, **kwargs)

            # override step_options
            recovery_workflow._step_id = _step_id
            recovery_workflow.data.step_options = result.step_options

            input_map[_step_id] = recovery_workflow

    # Step 3: return the output of the requested step
    return input_map[step_id]


@ray.remote(num_returns=2)
def _resume_workflow_step_executor(
    job_id: str,
    workflow_id: str,
    step_id: "StepID",
    current_output: [ray.ObjectRef],
) -> Tuple[ray.ObjectRef, ray.ObjectRef]:
    with workflow_context.workflow_logging_context(job_id):
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
            r = _construct_resume_workflow_from_step(workflow_id, step_id)
        except Exception as e:
            raise WorkflowNotResumableError(workflow_id) from e

        if not isinstance(r, Workflow):
            return r, None
        with workflow_context.workflow_step_context(
            workflow_id, last_step_of_workflow=True
        ):
            from ray.workflow.step_executor import execute_workflow

            result = execute_workflow(job_id, r)
            return result.persisted_output, result.volatile_output


def resume_workflow_step(
    job_id: str,
    workflow_id: str,
    step_id: "StepID",
    current_output: Optional[ray.ObjectRef],
) -> WorkflowExecutionResult:
    """Resume a step of a workflow.

    Args:
        job_id: The ID of the job that submits the workflow execution. The ID
        is used to identify the submitter of the workflow.
        workflow_id: The ID of the workflow job. The ID is used to identify
            the workflow.
        step_id: The step to resume in the workflow.

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
        job_id, workflow_id, step_id, current_output
    )
    persisted_output = WorkflowStaticRef.from_output(step_id, persisted_output)
    volatile_output = WorkflowStaticRef.from_output(step_id, volatile_output)
    return WorkflowExecutionResult(persisted_output, volatile_output)


def get_latest_output(workflow_id: str) -> Any:
    """Get the latest output of a workflow. This function is intended to be
    used by readonly virtual actors. To resume a workflow,
    `resume_workflow_job` should be used instead.

    Args:
        workflow_id: The ID of the workflow.

    Returns:
        The output of the workflow.
    """
    reader = workflow_storage.WorkflowStorage(workflow_id)
    try:
        step_id: StepID = reader.get_latest_progress()
        while True:
            result: workflow_storage.StepInspectResult = reader.inspect_step(step_id)
            if result.output_object_valid:
                # we already have the output
                return reader.load_step_output(step_id)
            if isinstance(result.output_step_id, str):
                step_id = result.output_step_id
            else:
                raise ValueError("Workflow output does not exists or not valid.")
    except Exception as e:
        raise WorkflowNotResumableError(workflow_id) from e
