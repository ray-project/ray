class DeploymentIsBeingDeletedError(Exception):
    """Raised when an operation is attempted on a deployment that is being deleted."""

    pass


class ExternalScalerDisabledError(Exception):
    """Raised when the external scaling API is used but external_scaler_enabled is False."""

    pass


class HealthCheckFailedError(Exception):
    """A replica's health check failed. Carries the check's number, so the
    controller can order this result against pushed ones by when each started."""

    def __init__(self, check_seq: int, message: str):
        super().__init__(check_seq, message)
        self.check_seq = check_seq
        self.message = message

    def __str__(self) -> str:
        return self.message
