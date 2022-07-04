from ray.util.annotations import PublicAPI
from ray.workflow.common import TaskID


class WorkflowError(Exception):
    """Workflow error base class."""


@PublicAPI(stability="beta")
class WorkflowExecutionError(WorkflowError):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] failed during execution."
        super().__init__(self.message)


@PublicAPI(stability="beta")
class WorkflowCancellationError(WorkflowError):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] is cancelled during execution."
        super().__init__(self.message)


class WorkflowStepNotRecoverableError(WorkflowError):
    """Raise the exception when we find a workflow step cannot be recovered
    using the checkpointed inputs."""

    def __init__(self, task_id: TaskID):
        self.message = f"Workflow step[id={task_id}] is not recoverable"
        super().__init__(self.message)


class WorkflowNotResumableError(WorkflowError):
    """Raise the exception when we cannot resume from a workflow."""

    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] is not resumable."
        super().__init__(self.message)
