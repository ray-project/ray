"""Utils for debugging purpose."""
import ray
from ray.dag import DAGNode, DAGInputData

from ray.workflow.common import asyncio_run
from ray.workflow.workflow_executor import WorkflowExecutor
from ray.workflow.workflow_context import workflow_task_context, WorkflowTaskContext
from ray.workflow.workflow_storage import get_workflow_storage


def execute_workflow_local(dag: DAGNode, workflow_id: str, *args, **kwargs):
    """Execute the workflow locally."""
    from ray.workflow.workflow_state_from_dag import workflow_state_from_dag

    job_id = ray.get_runtime_context().get_job_id()
    context = WorkflowTaskContext(workflow_id=workflow_id)
    with workflow_task_context(context):
        wf_store = get_workflow_storage()
        state = workflow_state_from_dag(
            dag, DAGInputData(*args, **kwargs), workflow_id=workflow_id
        )
        executor = WorkflowExecutor(state)
        fut = executor.get_task_output_async(state.output_task_id)
        asyncio_run(executor.run_until_complete(job_id, context, wf_store))
        return asyncio_run(fut)


def resume_workflow_local(workflow_id: str):
    """Resume the workflow locally."""
    from ray.workflow.workflow_state_from_storage import workflow_state_from_storage

    job_id = ray.get_runtime_context().get_job_id()
    context = WorkflowTaskContext(workflow_id=workflow_id)
    with workflow_task_context(context):
        wf_store = get_workflow_storage()
        state = workflow_state_from_storage(workflow_id, None)
        executor = WorkflowExecutor(state)
        fut = executor.get_task_output_async(state.output_task_id)
        asyncio_run(executor.run_until_complete(job_id, context, wf_store))
        return asyncio_run(fut)
