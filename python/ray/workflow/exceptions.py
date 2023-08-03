from ray.util.annotations import PublicAPI
from ray.workflow.common import TaskID


@PublicAPI(stability="alpha")
class WorkflowError(Exception):
    """Workflow error base class."""


@PublicAPI(stability="alpha")
class WorkflowExecutionError(WorkflowError):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] failed during execution."
        super().__init__(self.message)


@PublicAPI(stability="alpha")
class WorkflowCancellationError(WorkflowError):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] is cancelled during execution."
        super().__init__(self.message)


@PublicAPI(stability="alpha")
class WorkflowNotResumableError(WorkflowError):
    """Raise the exception when we cannot resume from a workflow."""

    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] is not resumable."
        super().__init__(self.message)


@PublicAPI(stability="alpha")
class WorkflowTaskNotRecoverableError(WorkflowNotResumableError):
    """Raise the exception when we find a workflow task cannot be recovered
    using the checkpointed inputs."""

    def __init__(self, task_id: TaskID):
        self.message = f"Workflow task[id={task_id}] is not recoverable"
        super(WorkflowError, self).__init__(self.message)


@PublicAPI(stability="alpha")
class WorkflowNotFoundError(WorkflowError):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] was referenced but doesn't exist."
        super().__init__(self.message)


@PublicAPI(stability="alpha")
class WorkflowStillActiveError(WorkflowError):
    def __init__(self, operation: str, workflow_id: str):
        self.message = (
            f"{operation} couldn't be completed because "
            f"Workflow[id={workflow_id}] is still running or pending."
        )
        super().__init__(self.message)
