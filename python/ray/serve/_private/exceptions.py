from ray.serve.exceptions import RayServeException


class DeploymentIsBeingDeletedError(RayServeException):
    """Raised when an operation is attempted on a deployment that is being deleted."""

    pass
