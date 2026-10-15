"""Helpers for recognizing and recovering from cloud credential failures.

Format-agnostic: an expired S3 session token or Azure SAS token looks the same
to any datasource or datasink that talks to object storage, so these live here
rather than next to one format's implementation.
"""

import os
from typing import Dict

# Substring patterns identifying an authentication/authorization failure from a
# cloud filesystem (expired/invalid credentials), as opposed to a transient
# network error or a genuine logical error. A plain backoff-and-retry fails
# identically every time on one of these -- credentials need to be refreshed
# first.
AUTH_ERROR_PATTERNS = [
    # AWS / S3 (botocore, PyArrow S3FileSystem).
    "ExpiredToken",
    "ExpiredTokenException",
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "RequestTimeTooSkewed",
    "AccessDenied",
    "UnrecognizedClientException",
    # Azure.
    "AuthenticationFailed",
    "InvalidAuthenticationInfo",
    "ExpiredAuthenticationToken",
    # GCS.
    "invalid_grant",
    "Invalid Credentials",
    # Generic HTTP status codes surfaced by cloud filesystem clients.
    "401 ",
    "403 ",
]


def is_auth_error(exc: BaseException) -> bool:
    """Best-effort check for whether ``exc`` is an authentication/authorization
    failure from a cloud filesystem (expired/invalid credentials), as opposed
    to a transient network error or a genuine logical error.

    Used to trigger a credential-refresh-and-retry rather than the plain
    backoff-and-retry used for transient errors: retrying an auth failure
    unchanged always fails identically, so it needs a different response.
    """
    message = str(exc)
    return any(pattern in message for pattern in AUTH_ERROR_PATTERNS)


def restore_environ(snapshot: Dict[str, str]) -> None:
    """Restore ``os.environ`` to ``snapshot``, dropping any keys added since.

    Used to undo credential env vars that a credential-vending call writes as a
    side effect, so they don't outlive the task in a reused worker process.
    Best-effort: it can't tell our own additions apart from an unrelated
    concurrent mutation in the same process. Having the vending mechanism not
    mutate the environment at all is the real fix; this contains the damage
    until then.
    """
    for key in list(os.environ):
        if key not in snapshot:
            del os.environ[key]
    for key, value in snapshot.items():
        if os.environ.get(key) != value:
            os.environ[key] = value
