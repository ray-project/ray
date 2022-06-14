import copy
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

import ray
from ray._private.ray_logging import configure_log_file, get_worker_log_file_name
from ray.workflow.common import WorkflowStatus

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.workflow.common import CheckpointModeType, StepID


@dataclass
class CheckpointContext:
    # The step is checkpointed or not.
    checkpoint: "CheckpointModeType" = True
    # Detached from DAG means step is not checkpointed in the DAG.
    # The output step inside current step is an exception, because
    # its output is linked to the output of the current step.
    detached_from_dag: bool = False

    def copy(self) -> "CheckpointContext":
        """Copy the dataclass safely."""
        return copy.copy(self)


@dataclass
class WorkflowStepContext:
    """
    The structure for saving workflow step context. The context provides
    critical info (e.g. where to checkpoint, which is its parent step)
    for the step to execute correctly.

    To fully explain what we are doing, we need to introduce some syntax
    first. The syntax for dependencies between workflow steps
    "B.step(A.step())" is "A - B"; the syntax for nested workflow steps
    "def A(): return B.step()" is "A / B".

    In a chain/DAG of step dependencies, the "output step" is the step of
    last (topological) order. For example, in "A - B - C", C is the
    output step.

    In a chain of nested workflow steps, the initial "output step" is
    called the "outer most step" for other "output steps". For example, in
    "A / B / C / D", "A" is the outer most step for "B", "C", "D";
    in the hybrid workflow "((A - B) / C / D) - (E / (F - G) / H)",
    "B" is the outer most step for "C", "D"; "E" is the outer most step
    for "G", "H".
    """

    # ID of the workflow.
    workflow_id: Optional[str] = None
    # The "calling stack" of the current workflow step. It describe
    # the parent workflow steps.
    workflow_scope: List[str] = field(default_factory=list)
    # The ID of the outer most workflow. "None" if it does not exists.
    outer_most_step_id: "Optional[StepID]" = None
    # The step that generates the output of the workflow (including all
    # nested steps).
    last_step_of_workflow: bool = False
    # The checkpoint context.
    checkpoint_context: CheckpointContext = field(default_factory=CheckpointContext)


_context: Optional[WorkflowStepContext] = None


@contextmanager
def workflow_step_context(workflow_id, last_step_of_workflow=False) -> None:
    """Initialize the workflow step context.

    Args:
        workflow_id: The ID of the workflow.
    """
    global _context
    original_context = _context
    assert workflow_id is not None
    try:
        _context = WorkflowStepContext(
            workflow_id, last_step_of_workflow=last_step_of_workflow
        )
        yield
    finally:
        _context = original_context


_sentinel = object()


@contextmanager
def fork_workflow_step_context(
    workflow_id: Optional[str] = _sentinel,
    workflow_scope: Optional[List[str]] = _sentinel,
    outer_most_step_id: Optional[str] = _sentinel,
    last_step_of_workflow: Optional[bool] = _sentinel,
    checkpoint_context: CheckpointContext = _sentinel,
):
    """Fork the workflow step context.
    Inherits the original value if no value is provided.

    Args:
        workflow_id: The ID of the workflow.
    """
    global _context
    original_context = _context
    assert workflow_id is not None
    try:
        _context = WorkflowStepContext(
            workflow_id=original_context.workflow_id
            if workflow_id is _sentinel
            else workflow_id,
            workflow_scope=original_context.workflow_scope
            if workflow_scope is _sentinel
            else workflow_scope,
            outer_most_step_id=original_context.outer_most_step_id
            if outer_most_step_id is _sentinel
            else outer_most_step_id,
            last_step_of_workflow=original_context.last_step_of_workflow
            if last_step_of_workflow is _sentinel
            else last_step_of_workflow,
            checkpoint_context=original_context.checkpoint_context
            if checkpoint_context is _sentinel
            else checkpoint_context,
        )
        yield
    finally:
        _context = original_context


def get_workflow_step_context() -> Optional[WorkflowStepContext]:
    return _context


def set_workflow_step_context(context: Optional[WorkflowStepContext]):
    global _context
    _context = context


def update_workflow_step_context(context: Optional[WorkflowStepContext], step_id: str):
    global _context
    _context = context
    _context.workflow_scope.append(step_id)


def get_current_step_id() -> str:
    """Get the current workflow step ID. Empty means we are in
    the workflow job driver."""
    s = get_scope()
    return s[-1] if s else ""


def get_current_workflow_id() -> str:
    assert _context is not None
    return _context.workflow_id


def get_name() -> str:
    return f"{get_current_workflow_id()}@{get_current_step_id()}"


def get_step_status_info(status: WorkflowStatus) -> str:
    assert _context is not None
    return f"Step status [{status}]\t[{get_name()}]"


def get_scope():
    return _context.workflow_scope


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
