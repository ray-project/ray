from typing import Optional, List


class WorkflowStepContext:
    def __init__(self,
                 workflow_id: str = None,
                 storage_url: str = None,
                 workflow_scope: List[str] = None):
        """
        The structure for saving workflow step context. The context provides
        critical info (e.g. where to checkpoint, which is its parent step)
        for the step to execute correctly.

        Args:
            workflow_id: The workflow job ID.
            storage_url: The storage of the workflow, used for checkpointing.
            workflow_scope: The "calling stack" of the current workflow step.
                It describe the parent workflow steps.
        """
        self.workflow_id = workflow_id
        self.storage_url = storage_url
        self.workflow_scope = workflow_scope or []

    def __reduce__(self):
        return WorkflowStepContext, (self.workflow_id, self.storage_url,
                                     self.workflow_scope)


_context: Optional[WorkflowStepContext] = None


def init_workflow_step_context(workflow_id, storage_url) -> None:
    """Initialize the workflow step context.

    Args:
        workflow_id: The ID of the workflow.
        storage_url: The storage the workflow is using.
    """
    global _context
    assert workflow_id is not None
    _context = WorkflowStepContext(workflow_id, storage_url)


def get_workflow_step_context() -> Optional[WorkflowStepContext]:
    return _context


def set_workflow_step_context(context: Optional[WorkflowStepContext]):
    global _context
    _context = context


def update_workflow_step_context(context: Optional[WorkflowStepContext],
                                 step_id: str):
    global _context
    _context = context
    _context.workflow_scope.append(step_id)


def get_current_step_id() -> str:
    """Get the current workflow step ID. Empty means we are in
    the workflow job driver."""
    s = get_scope()
    return s[-1] if s else ""


def get_scope():
    return _context.workflow_scope
