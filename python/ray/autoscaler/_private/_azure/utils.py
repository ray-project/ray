"""Azure credential error handling utilities.

Convenience re-exports from :mod:`ray._common.azure_utils` so that
autoscaler code can use a short, local import path.
"""

from ray._common.azure_utils import (  # noqa: F401
    _RECOVERY_STEPS,
    catch_azure_credential_errors,
    handle_azure_credential_error,
    validate_azure_credentials,
)
