"""Azure credential error handling utilities for the Ray autoscaler.

Provides user-friendly error messages and recovery instructions when
Azure SDK authentication failures occur, mirroring the pattern used
by the AWS provider's handle_boto_error() in aws/utils.py.
"""

import functools
import logging

logger = logging.getLogger(__name__)

_RECOVERY_STEPS = [
    "Try re-authenticating: `az login`",
    "Verify token validity: `az account get-access-token`",
    "For service principal auth, ensure these environment variables are set:",
    "  export AZURE_CLIENT_ID=<appId>",
    "  export AZURE_CLIENT_SECRET=<password>",
    "  export AZURE_TENANT_ID=<tenant>",
    "For managed identity, verify the VM has an assigned identity in the Azure portal.",
]


def handle_azure_credential_error(exc, resource_type=None):
    """Inspect *exc* and, if it is an Azure credential/auth error, raise a
    ``RuntimeError`` with actionable recovery instructions.

    If *exc* is **not** a recognised credential error the call is a no-op so
    that callers can unconditionally invoke this in ``except`` blocks and
    re-raise the original exception afterwards.

    Args:
        exc: The caught exception instance.
        resource_type: Optional human-readable label (e.g. ``'compute'``,
            ``'network'``, ``'resource'``) included in the message to indicate
            which Azure API call failed.
    """
    # Lazy imports – the Azure SDK may not be installed.
    try:
        from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
    except ImportError:
        return

    try:
        from azure.identity import CredentialUnavailableError
    except ImportError:
        CredentialUnavailableError = None

    is_credential_error = False

    if isinstance(exc, ClientAuthenticationError):
        is_credential_error = True
    elif CredentialUnavailableError and isinstance(exc, CredentialUnavailableError):
        is_credential_error = True
    elif isinstance(exc, HttpResponseError) and getattr(exc, "status_code", None) in (
        401,
        403,
    ):
        is_credential_error = True

    if not is_credential_error:
        return

    logger.debug("Azure credential error intercepted: %s", exc, exc_info=True)

    context = f" while accessing {resource_type} resources" if resource_type else ""
    raise RuntimeError(
        f"Azure credential error{context}: {exc}\n\n"
        "Your Azure credentials may have expired or are misconfigured.\n"
        "To fix this, try the following:\n"
        + "\n".join(f"  {step}" for step in _RECOVERY_STEPS)
    ) from exc


def validate_azure_credentials(credential=None):
    """Proactively verify that Azure credentials can acquire a token.

    Attempts to obtain an ARM management token via *credential* (defaulting to
    ``DefaultAzureCredential``).  On failure the exception is routed through
    :func:`handle_azure_credential_error` to provide recovery guidance.
    """
    from azure.identity import DefaultAzureCredential

    cred = credential or DefaultAzureCredential()
    try:
        cred.get_token("https://management.azure.com/.default")
    except Exception as e:
        handle_azure_credential_error(e)
        raise


def catch_azure_credential_errors(resource_type=None):
    """Decorator that wraps a function so that Azure credential errors are
    intercepted and re-raised as ``RuntimeError`` with recovery instructions.

    Non-credential exceptions pass through unmodified.

    Args:
        resource_type: Optional context string (e.g. ``'compute'``) forwarded
            to :func:`handle_azure_credential_error`.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                handle_azure_credential_error(e, resource_type=resource_type)
                raise

        return wrapper

    return decorator
