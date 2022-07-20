import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import ray
from ray._private.ray_logging import configure_log_file, get_worker_log_file_name
from ray.workflow.common import CheckpointModeType, WorkflowStatus

logger = logging.getLogger(__name__)


@dataclass
class WorkflowStepContext:
    """
    The structure for saving workflow step context. The context provides
    critical info (e.g. where to checkpoint, which is its parent step)
    for the step to execute correctly.
    """

    # ID of the workflow.
    workflow_id: Optional[str] = None
    # ID of the current task.
    task_id: str = ""
    # ID of the task that creates the current task.
    creator_task_id: str = ""
    # The checkpoint context of parent workflow tasks.
    checkpoint: CheckpointModeType = True
    # The context of catching exceptions.
    catch_exceptions: bool = False


_context: Optional[WorkflowStepContext] = None


@contextmanager
def workflow_step_context(context) -> None:
    """Initialize the workflow step context.

    Args:
        context: The new context.
    """
    global _context
    original_context = _context
    try:
        _context = context
        yield
    finally:
        _context = original_context


def get_workflow_step_context() -> Optional[WorkflowStepContext]:
    return _context


def get_current_step_id() -> str:
    """Get the current workflow step ID. Empty means we are in
    the workflow job driver."""
    return get_workflow_step_context().task_id


def get_current_workflow_id() -> str:
    assert _context is not None
    return _context.workflow_id


def get_name() -> str:
    return f"{get_current_workflow_id()}@{get_current_step_id()}"


def get_step_status_info(status: WorkflowStatus) -> str:
    assert _context is not None
    return f"Step status [{status}]\t[{get_name()}]"


_in_workflow_execution = False


@contextmanager
def workflow_execution() -> None:
    """Scope for workflow step execution."""
    global _in_workflow_execution
    try:
        _in_workflow_execution = True
        yield
    finally:
        _in_workflow_execution = False


def in_workflow_execution() -> bool:
    """Whether we are in workflow step execution."""
    global _in_workflow_execution
    return _in_workflow_execution


@contextmanager
def workflow_logging_context(job_id) -> None:
    """Initialize the workflow logging context.

    Workflow executions are running as remote functions from
    WorkflowManagementActor. Without logging redirection, workflow
    inner execution logs will be pushed to the driver that initially
    created WorkflowManagementActor rather than the driver that
    actually submits the current workflow execution.
    We use this conext manager to re-configure the log files to send
    the logs to the correct driver, and to restore the log files once
    the execution is done.

    Args:
        job_id: The ID of the job that submits the workflow execution.
    """
    node = ray._private.worker._global_node
    original_out_file, original_err_file = node.get_log_file_handles(
        get_worker_log_file_name("WORKER")
    )
    out_file, err_file = node.get_log_file_handles(
        get_worker_log_file_name("WORKER", job_id)
    )
    try:
        configure_log_file(out_file, err_file)
        yield
    finally:
        configure_log_file(original_out_file, original_err_file)
