class DeploymentIsBeingDeletedError(Exception):
    """Raised when an operation is attempted on a deployment that is being deleted."""

    pass


class ExternalScalerNotEnabledError(Exception):
    """Raised when the external scaling API is used but external_scaler_enabled is False."""

    pass
