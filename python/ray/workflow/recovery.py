from typing import List, Any, Union, Dict, Callable, Tuple, Optional
from collections import deque, defaultdict

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
        A workflow that recovers the step, or the output of the step
            if it has been checkpointed.
    """
    reader = workflow_storage.WorkflowStorage(workflow_id)

    # Step 1: construct dependency of the DAG (BFS)
    inpsect_results = {}
    dependency_map = defaultdict(list)
    num_in_edges = {}

    dag_visit_queue = deque([step_id])
    while dag_visit_queue:
        s: StepID = dag_visit_queue.popleft()
        if s in inpsect_results:
            continue
        r = reader.inspect_step(s)
        inpsect_results[s] = r
        if not r.is_recoverable():
            raise WorkflowStepNotRecoverableError(s)
        if r.output_object_valid:
            deps = []
        elif isinstance(r.output_step_id, str):
            deps = [r.output_step_id]
        else:
            deps = r.workflows
        for w in deps:
            dependency_map[w].append(s)
        num_in_edges[s] = len(deps)
        dag_visit_queue.extend(deps)

    # Step 2: topological sort to determine the execution order (Kahn's algorithm)
    execution_queue: List[StepID] = []

    start_nodes = deque(k for k, v in num_in_edges.items() if v == 0)
    while start_nodes:
        n = start_nodes.popleft()
        execution_queue.append(n)
        for m in dependency_map[n]:
            num_in_edges[m] -= 1
            assert num_in_edges[m] >= 0, (m, n)
            if num_in_edges[m] == 0:
                start_nodes.append(m)

    # Step 3: recover the workflow by the order of the execution queue
    with serialization.objectref_cache():
        # "input_map" is a context storing the input which has been loaded.
        # This context is important for deduplicate step inputs.
        input_map: Dict[StepID, Any] = {}

        for _step_id in execution_queue:
            result = inpsect_results[_step_id]
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

    # Step 4: return the output of the requested step
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
