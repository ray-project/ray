import asyncio
import inspect
import logging
import os
import random
import re
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlparse

from ray.util import log_once

if TYPE_CHECKING:
    import s3fs  # noqa: F401

    from ray.data.context import DataContext

import pyarrow as pa
import pyarrow.fs

from ray.data._internal.util import (
    RetryingPyFileSystem,
    _iter_arrow_table_for_target_max_block_size,
)
from ray.data.block import BlockAccessor
from ray.data.datasource.path_util import _split_uri

try:
    from obstore import parse_scheme as obstore_parse_scheme
except ImportError:
    obstore_parse_scheme = None

logger = logging.getLogger(__name__)


def _parse_obstore_int_env(var_name: str, default: int) -> int:
    """Parse a non-negative Ray Data obstore env var without breaking import.

    Malformed values log a warning and fall back to default so bad config in
    the environment cannot prevent ``_obstore_download`` from loading.
    """
    raw = os.environ.get(var_name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(str(raw).strip(), 10)
    except ValueError:
        logger.warning(
            "Ignoring invalid integer for %s=%r — using default %s",
            var_name,
            raw,
            default,
        )
        return default
    if value < 0:
        # A negative count or duration can't be handed to obstore (Rust
        # durations are unsigned); treat it like a malformed value.
        logger.warning(
            "Ignoring negative value for %s=%r — using default %s",
            var_name,
            raw,
            default,
        )
        return default
    return value


# Retry budget for the obstore GET/HEAD clients. obstore retries 5xx, 429 and
# connection errors itself and sleeps between attempts using decorrelated
# jitter: each sleep is drawn from ``[init_backoff, base * previous)`` and
# capped at ``max_backoff``, so in expectation the sleep grows by about
# ``init_backoff / 2`` per retry (near-linear, not exponential). ``init_backoff``
# is therefore the lever that decides how long a sustained S3 SlowDown burst
# can be ridden out: with obstore's 100 ms default, 10 retries completed in
# ~2 s and a transient 503 in the read_from_uris release test became a failed
# download. These are read at call time, not import time, so a job can
# override them through ``runtime_env`` env vars.
_DEFAULT_RETRY_MAX_RETRIES_INT = 10
_DEFAULT_RETRY_INIT_BACKOFF_MS_INT = 1000
_DEFAULT_RETRY_MAX_BACKOFF_MS_INT = 20_000
_DEFAULT_RETRY_TIMEOUT_S_INT = 120


def _obstore_retry_config_from_env() -> Dict[str, Any]:
    """Build the obstore ``retry_config`` for download GET/HEAD clients.

    With the defaults the expected sleeps are 1.0, 1.5, 2.0, ... 5.5 s, about
    30 s in total over 10 retries and never more than ``retry_timeout``,
    instead of the ~2 s that obstore's own defaults allow. Backoff sleeps are
    async inside obstore, so a throttled object only holds its concurrency
    slot; other URIs keep downloading.
    """
    return {
        "max_retries": _parse_obstore_int_env(
            "RAY_DATA_OBSTORE_RETRY_MAX_RETRIES", _DEFAULT_RETRY_MAX_RETRIES_INT
        ),
        "retry_timeout": timedelta(
            seconds=_parse_obstore_int_env(
                "RAY_DATA_OBSTORE_RETRY_TIMEOUT_S", _DEFAULT_RETRY_TIMEOUT_S_INT
            )
        ),
        "backoff": {
            "init_backoff": timedelta(
                milliseconds=_parse_obstore_int_env(
                    "RAY_DATA_OBSTORE_RETRY_INIT_BACKOFF_MS",
                    _DEFAULT_RETRY_INIT_BACKOFF_MS_INT,
                )
            ),
            "max_backoff": timedelta(
                milliseconds=_parse_obstore_int_env(
                    "RAY_DATA_OBSTORE_RETRY_MAX_BACKOFF_MS",
                    _DEFAULT_RETRY_MAX_BACKOFF_MS_INT,
                )
            ),
            "base": 2,
        },
    }


def _obstore_client_options_from_env() -> Dict[str, Any]:
    """Optional obstore ``client_options`` overrides from the environment.

    ``RAY_DATA_OBSTORE_REQUEST_TIMEOUT_S`` sets the per-request timeout
    (obstore's default is 30 s). Large whole-file GETs can legitimately take
    longer than that, and a timed-out request consumes the same retry budget
    as a 5xx. Unset or ``0`` keeps obstore's default.
    """
    timeout_s = _parse_obstore_int_env("RAY_DATA_OBSTORE_REQUEST_TIMEOUT_S", 0)
    if timeout_s <= 0:
        return {}
    return {"timeout": timedelta(seconds=timeout_s)}


class DownloadError(OSError):
    """A URI could not be downloaded after the retries were exhausted.

    Raised by ``download()`` for transient or systemic failures (5xx,
    throttling, timeouts, dropped connections, bad credentials) once obstore's
    own retries and the block-level attempts
    (``RAY_DATA_DOWNLOAD_MAX_ATTEMPTS``) are used up, so the task fails
    instead of silently yielding ``None`` for the row. Missing objects,
    access-denied objects and invalid URIs still yield ``None`` and are
    counted in a per-block warning.

    Also raised when every URI in a block is access-denied: one denied object
    is row-local, a block in which nothing could be read points at the
    credentials or the bucket permissions.

    Match on ``"DownloadError"`` in ``DataContext.retried_map_errors`` (with
    ``max_map_retries``) to retry the task, or set
    ``DataContext.max_errored_blocks`` to tolerate the failure.
    """


try:
    from obstore import exceptions as _obstore_exceptions
except ImportError:  # obstore not installed, or too old to ship the module.
    _obstore_exceptions = None


def _permanent_download_error_types() -> Tuple[type, ...]:
    """Exception types meaning "this URI can never be downloaded"."""
    types: List[type] = [
        # 404 from obstore (NotFound maps to the builtin) and from PyArrow.
        FileNotFoundError,
        # Local-filesystem permission denial; see PermissionDeniedError below.
        PermissionError,
        # Malformed URI, raised before any request is made (ArrowInvalid is a
        # ValueError subclass).
        ValueError,
    ]
    if _obstore_exceptions is not None:
        for name in (
            # Deprecated in favour of FileNotFoundError since obstore 0.7;
            # keep it for older versions.
            "NotFoundError",
            # S3 answers 403 for a *missing* key when the caller lacks
            # s3:ListBucket, so a permission denial has to stay row-local.
            "PermissionDeniedError",
            "InvalidPathError",
            "NotSupportedError",
        ):
            exc_type = getattr(_obstore_exceptions, name, None)
            if exc_type is not None:
                types.append(exc_type)
    return tuple(types)


_PERMANENT_DOWNLOAD_ERROR_TYPES = _permanent_download_error_types()


def _range_error_types_without_fallback() -> Tuple[type, ...]:
    """Range-request failures that a whole-file GET would only repeat.

    A missing, denied or unauthenticated object is refused the same way
    whichever way it is requested, and ``GenericError`` means obstore already
    spent its retry budget against the same store, so retrying the file as a
    whole GET would spend it again. The fallback is kept for everything else,
    in particular a store that does not honour range requests
    (``NotSupportedError``) and a chunk of the wrong size.
    """
    types: List[type] = [FileNotFoundError, PermissionError]
    if _obstore_exceptions is not None:
        for name in (
            "NotFoundError",
            "PermissionDeniedError",
            "UnauthenticatedError",
            "GenericError",
        ):
            exc_type = getattr(_obstore_exceptions, name, None)
            if exc_type is not None:
                types.append(exc_type)
    return tuple(types)


_RANGE_ERROR_TYPES_WITHOUT_FALLBACK = _range_error_types_without_fallback()


# S3 reports these with HTTP 403 just like a plain ``AccessDenied``, but they
# mean the credentials themselves are bad and so affect every request. They
# must fail the task rather than turn into a column of nulls. obstore puts the
# S3 XML error body into the exception message for GET requests, which is
# where the code is read from; S3 error codes are a stable, documented API.
_CREDENTIAL_ERROR_CODES = frozenset(
    {
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "ExpiredToken",
        "InvalidToken",
        "TokenRefreshRequired",
        "AuthorizationHeaderMalformed",
        "InvalidSecurity",
    }
)
_S3_ERROR_CODE_RE = re.compile(r"<Code>([A-Za-z]+)</Code>")

# PyArrow's S3FileSystem reports an access-denied object with this spelling.
# A HEAD response carries no body, so the S3 error code is unknown there and
# one 403 is treated as row-local, like obstore's ``AccessDenied``; a block in
# which every object is denied raises instead (``_raise_if_all_denied``).
# A 400 (``AWS Error UNKNOWN (HTTP status 400)``: expired or malformed
# token) is deliberately not matched and so raises ``DownloadError``.
_PYARROW_INACCESSIBLE_OBJECT_RE = re.compile(
    r"AWS Error ACCESS_DENIED during (?:HeadObject|GetObject) operation"
)

# A URI whose download failed transiently is re-fetched this many times in
# total within the task before ``DownloadError`` is raised; only the failed
# URIs are re-fetched, bytes already downloaded are kept.
_DEFAULT_DOWNLOAD_MAX_ATTEMPTS_INT = 2
_DEFAULT_DOWNLOAD_RETRY_BACKOFF_MS_INT = 1000


def _is_credential_error(exc: BaseException) -> bool:
    """Whether ``exc`` carries an S3 error code that means bad credentials."""
    match = _S3_ERROR_CODE_RE.search(str(exc))
    return match is not None and match.group(1) in _CREDENTIAL_ERROR_CODES


def _is_permanent_download_error(exc: BaseException) -> bool:
    """Whether ``exc`` means the URI can never be downloaded (yield ``None``).

    Everything else is transient or systemic and must raise ``DownloadError``
    so task-level retry and ``max_errored_blocks`` apply instead of the row
    being lost: ``obstore.exceptions.GenericError`` (what obstore raises once
    its retries on 5xx / 429 / timeouts / dropped connections are exhausted),
    ``UnauthenticatedError``, ``JoinError``, a 403 whose S3 error code says
    the credentials are bad, and any other ``OSError``. ``GenericError`` is
    deliberately not inspected further: the retry exhaustion text differs
    across object_store versions, and a message gate would silently revert
    to ``None`` on an upgrade.
    """
    if _is_credential_error(exc):
        return False
    if isinstance(exc, _PERMANENT_DOWNLOAD_ERROR_TYPES):
        return True
    return (
        isinstance(exc, OSError)
        and _PYARROW_INACCESSIBLE_OBJECT_RE.search(str(exc)) is not None
    )


def _skip_reason(exc: BaseException) -> str:
    """Short label for a permanent failure, used in the per-block summary."""
    if _obstore_exceptions is not None:
        if isinstance(exc, getattr(_obstore_exceptions, "NotFoundError", ())):
            return "missing"
        if isinstance(exc, getattr(_obstore_exceptions, "InvalidPathError", ())):
            return "invalid uri"
        if isinstance(exc, getattr(_obstore_exceptions, "NotSupportedError", ())):
            return "unsupported"
    if isinstance(exc, FileNotFoundError):
        return "missing"
    if isinstance(exc, (ValueError, TypeError)):
        return "invalid uri"
    return "access denied"


def _store_failure_reason(exc: BaseException) -> str:
    """Label for a store that could not be built for one URI's store URL.

    obstore reports an unrecognised scheme or host as ``GenericError``
    ("Unable to recognise URL"); a malformed store URL or an option the
    scheme rejects comes back as ``ValueError`` / ``InvalidPathError``.
    """
    if isinstance(exc, (ValueError, TypeError)):
        return "invalid uri"
    if _obstore_exceptions is not None and isinstance(
        exc, getattr(_obstore_exceptions, "InvalidPathError", ())
    ):
        return "invalid uri"
    return "unsupported"


def _is_usable_uri(uri: Any) -> bool:
    """A non-empty string. ``None``, NaN, ``pd.NA`` and ``""`` are not."""
    return isinstance(uri, str) and uri.strip() != ""


@dataclass
class _DownloadFailure:
    """Why one URI was not downloaded. ``permanent`` ones become ``None``."""

    uri: Any
    reason: str
    error: BaseException
    permanent: bool
    # The store for this URI's store URL could not be built. Row-local on its
    # own; if it happens to every URI in a block it points at the block's
    # shared configuration instead and the first such error is raised.
    store_failure: bool = False


def _download_error(
    uri: Any, uri_column_name: str, cause: BaseException, attempts: int = 1
) -> DownloadError:
    """Build the ``DownloadError`` for a transient failure on ``uri``."""
    return DownloadError(
        f"Failed to download {uri!r} (column {uri_column_name!r}) after "
        f"{attempts} attempt(s): {type(cause).__name__}: {cause}. Missing or "
        "inaccessible objects (404/403) yield null; other failures fail the "
        "block. See the RAY_DATA_OBSTORE_RETRY_* and "
        "RAY_DATA_DOWNLOAD_MAX_ATTEMPTS environment variables and "
        "DataContext.max_errored_blocks."
    )


def _download_attempt_settings() -> Tuple[int, int]:
    """``(max_attempts, backoff_ms)`` for re-fetching transient failures."""
    attempts = max(
        1,
        _parse_obstore_int_env(
            "RAY_DATA_DOWNLOAD_MAX_ATTEMPTS", _DEFAULT_DOWNLOAD_MAX_ATTEMPTS_INT
        ),
    )
    backoff_ms = _parse_obstore_int_env(
        "RAY_DATA_DOWNLOAD_RETRY_BACKOFF_MS", _DEFAULT_DOWNLOAD_RETRY_BACKOFF_MS_INT
    )
    return attempts, backoff_ms


def _retry_backoff_s(attempt: int, backoff_ms: int) -> float:
    """Jittered, doubling delay before re-fetching after ``attempt`` failed."""
    if backoff_ms <= 0:
        return 0.0
    return (
        min(backoff_ms * 2 ** (attempt - 1), 30_000) * random.uniform(0.8, 1.2) / 1000
    )


def _log_skipped_uris(
    uri_column_name: str, skipped: "Counter[str]", num_uris: int
) -> None:
    """One warning per block listing how many URIs yielded null and why."""
    if not skipped:
        return
    breakdown = ", ".join(
        f"{count} {reason}" for reason, count in sorted(skipped.items())
    )
    logger.warning(
        "Skipped %d of %d URIs for column %r; their bytes are null: %s. "
        "A missing object (404) or an access-denied object (403) is reported "
        "per row; bad credentials and other errors fail the task instead.",
        sum(skipped.values()),
        num_uris,
        uri_column_name,
        breakdown,
    )


def _raise_if_all_denied(
    uri_column_name: str, uris: List[Any], denied: List[_DownloadFailure]
) -> None:
    """Fail the block when every downloadable URI in it was access-denied.

    One denied object stays row-local: S3 answers 403 for a *missing* key
    when the caller lacks ``s3:ListBucket``. A block in which nothing at all
    could be read points at the credentials or the bucket policy instead,
    and a column of nulls would hide that. On the PyArrow backend this is
    the only credential signal, because a HEAD response carries no S3 error
    code to tell ``AccessDenied`` from ``InvalidAccessKeyId``.
    """
    num_usable = sum(1 for uri in uris if _is_usable_uri(uri))
    if num_usable == 0 or len(denied) < num_usable:
        return
    first = denied[0]
    raise DownloadError(
        f"Access denied for all {num_usable} URIs of column {uri_column_name!r} "
        f"(first: {first.uri!r}: {type(first.error).__name__}: {first.error}). "
        "A single denied object yields null, but a block in which every "
        "object is denied points at the credentials or the bucket "
        "permissions, so the task fails instead of yielding a column of "
        "nulls. See DataContext.max_errored_blocks to tolerate it."
    ) from first.error


# Constants & configuration
RAY_DATA_USE_OBSTORE = os.environ.get("RAY_DATA_USE_OBSTORE", "1") == "1"
OBSTORE_AVAILABLE = RAY_DATA_USE_OBSTORE and obstore_parse_scheme is not None

_DEFAULT_RANGE_THRESHOLD_INT = 4 * 1024 * 1024  # 4 MB
_DEFAULT_RANGE_CHUNK_SIZE_INT = 8 * 1024 * 1024  # 8 MB
_DEFAULT_MAX_CONCURRENCY_INT = 128

# Range-split downloads: files above this threshold (bytes) are downloaded as
# parallel get_range requests instead of a single get.  Default 4 MB — below
# the crossover where ranged overhead exceeds the single-GET time, but above
# it large/skewed workloads see 10-25x speedup.  Set to 0 to disable.
RAY_DATA_OBSTORE_RANGE_THRESHOLD = _parse_obstore_int_env(
    "RAY_DATA_OBSTORE_RANGE_THRESHOLD", _DEFAULT_RANGE_THRESHOLD_INT
)
RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE = _parse_obstore_int_env(
    "RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE", _DEFAULT_RANGE_CHUNK_SIZE_INT
)
RAY_DATA_OBSTORE_MAX_CONCURRENCY = _parse_obstore_int_env(
    "RAY_DATA_OBSTORE_MAX_CONCURRENCY", _DEFAULT_MAX_CONCURRENCY_INT
)

_FILE_SIZE_COLUMN_PREFIX = "__ray_file_size__"

# Public utilities (exported to plan_download_op.py)
_fallback_warned = False


def _log_fallback_warning() -> None:
    """Log a one-time warning when falling back to the PyArrow download path."""
    global _fallback_warned
    if _fallback_warned:
        return
    _fallback_warned = True
    if not RAY_DATA_USE_OBSTORE:
        logger.info(
            "obstore disabled via RAY_DATA_USE_OBSTORE=0 — "
            "using the PyArrow download path."
        )
    else:
        logger.warning(
            "obstore is not installed — falling back to the "
            "PyArrow download path, which is significantly "
            "slower for large or numerous files. "
            "Install it with: pip install obstore"
        )


# Path-style AWS S3 host: ``s3.amazonaws.com`` (legacy global) or
# ``s3.<region>.amazonaws.com`` (regional). Virtual-host hosts like
# ``<bucket>.s3.<region>.amazonaws.com`` already carry the bucket in the
# netloc and route correctly through obstore without rewriting.
_AWS_PATH_STYLE_HOST_RE = re.compile(
    r"^s3(?:\.[a-z0-9-]+)?\.amazonaws\.com$", re.IGNORECASE
)


def _split_obstore_uri(uri: str) -> Tuple[str, str]:
    """Split a URI into ``(store_url, path)`` for ``obstore.store.from_url``.

    Wraps :func:`_split_uri` to rewrite AWS path-style HTTPS URLs of the
    form ``https://s3.<region>.amazonaws.com/<bucket>/<key>`` into
    ``store_url='s3://<bucket>'`` / ``path='<key>'``. Without this, obstore
    constructs an ``S3Store`` keyed by the regional host endpoint, which
    pins region from the URL and bails with ``BareRedirect`` whenever the
    bucket actually lives in a different region (AWS's PermanentRedirect
    omits the ``Location`` header, so generic redirect-following fails).
    Rewriting to ``s3://<bucket>`` lets ``S3Store`` discover the bucket's
    real region once and cache it for the store's lifetime.

    Pre-signed HTTPS URLs (any URI with a query string) are passed through
    unchanged because their signature is bound to host + path + query, so
    rewriting would invalidate the signature.
    """
    parsed = urlparse(uri, allow_fragments=False)
    if (
        parsed.scheme in ("http", "https")
        and not parsed.query
        and _AWS_PATH_STYLE_HOST_RE.match(parsed.netloc)
    ):
        raw_path = parsed.path[1:] if parsed.path.startswith("/") else parsed.path
        bucket, _, key = raw_path.partition("/")
        if bucket:
            return f"s3://{bucket}", key
    return _split_uri(uri)


def _is_obstore_supported_url(path: str) -> bool:
    """Check if *path* is a URL that obstore can handle.

    obstore supports ``s3://``, ``gs://``, ``az://``, ``file://``,
    ``http://``, and ``https://`` schemes.  Uses obstore's native
    ``parse_scheme`` when available.

    Returns ``False`` for relative paths or unsupported schemes.
    This function should only be called when obstore is known to be available.
    """
    if obstore_parse_scheme is None:
        return False
    try:
        obstore_parse_scheme(path)
        return True
    except Exception:
        return False


# Credential extraction & store management
class _S3FSSessionCredentialProvider:
    """Obstore credential provider backed by an fsspec s3fs session.

    s3fs with Okta / STS / profile-based auth resolves credentials lazily via
    ``fs.session.get_credentials()`` and rotates them on expiry. A single
    snapshot becomes stale during long-running jobs, so we install this as
    obstore's ``credential_provider`` callable instead — obstore will invoke
    it whenever it needs fresh keys.

    Cached in memory until ``expires_at`` so we don't re-enter the session on
    every request. Thread-safe via an ``RLock`` because obstore may call this
    concurrently from its async runtime.
    """

    _DEFAULT_TTL = timedelta(minutes=30)

    def __init__(self, session: Any, ttl: Optional[timedelta] = None) -> None:
        self._session = session
        self._ttl = ttl if ttl is not None else self._DEFAULT_TTL
        self._lock = threading.RLock()
        self._cached: Optional[Dict[str, Any]] = None

    @classmethod
    def from_fsspec_fs(
        cls, fsspec_fs: "s3fs.S3FileSystem"
    ) -> Optional["_S3FSSessionCredentialProvider"]:
        """Build a provider from an s3fs filesystem, or ``None`` if no session."""
        session = getattr(fsspec_fs, "session", None) or getattr(
            fsspec_fs, "_session", None
        )
        if session is None or not hasattr(session, "get_credentials"):
            return None
        return cls(session)

    async def __call__(self) -> Dict[str, Any]:
        """Return obstore-compatible S3 credentials, refreshing past expiry."""
        now = datetime.now(timezone.utc)
        with self._lock:
            if self._cached is not None and self._cached["expires_at"] > now:
                return dict(self._cached)

        session_creds = await self._await_maybe(self._session.get_credentials())
        if session_creds is None:
            raise RuntimeError("fsspec S3 session returned no credentials")
        frozen = await self._await_maybe(session_creds.get_frozen_credentials())
        access_key = getattr(frozen, "access_key", None)
        if not access_key:
            raise RuntimeError("fsspec S3 session returned no access key")

        creds: Dict[str, Any] = {
            "access_key_id": access_key,
            "secret_access_key": getattr(frozen, "secret_key", None) or "",
            "expires_at": self._compute_expires_at(session_creds),
        }
        token = getattr(frozen, "token", None)
        if token:
            creds["token"] = token

        with self._lock:
            self._cached = creds
        return dict(creds)

    @staticmethod
    async def _await_maybe(value: Any) -> Any:
        """Await *value* if it's awaitable (coroutine, Task, Future, custom)."""
        if inspect.isawaitable(value):
            return await value
        return value

    def _compute_expires_at(self, session_creds: Any) -> datetime:
        """Read the session's expiry, falling back to now + TTL."""
        for attr in ("expiry_time", "_expiry_time"):
            exp = getattr(session_creds, attr, None)
            if isinstance(exp, datetime):
                return exp if exp.tzinfo else exp.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) + self._ttl

    def can_fetch_credentials(self) -> bool:
        """Return True if a credential fetch succeeds from sync code right now.

        Used by the planner during routing decisions: if the session can't
        produce a credential set, fall back to the threaded path rather than
        installing a provider that would fail on first request. Logs at DEBUG
        so legitimate failures (expired creds, transient session errors) are
        diagnosable without spamming the user.
        """
        try:
            asyncio.run(self())
        except Exception as e:
            logger.debug("Could not fetch fsspec session credentials: %r", e)
            return False
        return True


def _native_s3_obstore_kwargs(
    filesystem: "pyarrow.fs.S3FileSystem",
) -> Optional[Dict[str, Any]]:
    """Translate a native PyArrow ``S3FileSystem`` into ``obstore.store.from_url``
    kwargs, or ``None`` to route to the threaded path.

    PyArrow exposes only ``region`` as a Python attribute, but the full
    construction config (credential kind, keys, region, endpoint) is preserved in
    the filesystem's pickle state -- PyArrow round-trips it to ship filesystems to
    worker processes. Read it from there so obstore can be configured to match,
    keeping the fast obstore download path instead of falling back to the slower
    threaded one.

    Returns ``None`` (caller uses the threaded path, applying the filesystem
    verbatim) when the config can't be represented as *static* obstore options:
    assume-role credentials, which PyArrow refreshes but a one-time obstore
    snapshot would let go stale; or an unrecognized pickle shape (e.g. a future
    PyArrow), which is caught and treated conservatively.
    """
    try:
        # __reduce__() -> (reconstruct_fn, (state_dict,)); the state dict holds
        # every constructor argument. PyArrow derives ``anonymous`` and the keys
        # from the underlying credentials kind, so this is the authoritative view.
        state = filesystem.__reduce__()[1][0]
    except Exception as e:
        if log_once("ray-data-obstore-filesystem-reduce-error"):
            logger.warning(
                "Unable to fetch PyArrow filesystem state from %s. Ray Data is unable to use obstore for this filesystem for faster downloads.",
                filesystem,
                exc_info=e,
            )
        return None

    kwargs: Dict[str, Any] = {}
    if state.get("region"):
        kwargs["region"] = state["region"]
    if state.get("endpoint_override"):
        kwargs["endpoint"] = state["endpoint_override"]

    if state.get("anonymous"):
        kwargs["skip_signature"] = True
        return kwargs

    if state.get("role_arn"):
        # Assume-role creds are refreshed periodically by PyArrow; a static
        # obstore snapshot would go stale. Let the threaded path use the FS.
        return None

    if state.get("access_key"):
        kwargs["access_key_id"] = state["access_key"]
        if state.get("secret_key"):
            kwargs["secret_access_key"] = state["secret_key"]
        if state.get("session_token"):
            kwargs["session_token"] = state["session_token"]
        return kwargs

    # Default credential chain: obstore resolves ambient credentials itself,
    # matching the filesystem's default chain. Region (if any) is enough.
    return kwargs


def _extract_credentials_from_filesystem(
    filesystem: Optional["pyarrow.fs.FileSystem"],
) -> Optional[Dict[str, Any]]:
    """Extract credentials from a PyArrow filesystem for use with obstore.

    Maps PyArrow filesystem configuration to obstore keyword arguments.
    See obstore docs for available options per store type.

    **Native S3 (``S3FileSystem``):** PyArrow exposes only ``region`` on the
    Python object, but the full credential config is recoverable from the
    filesystem's pickle state, so :func:`_native_s3_obstore_kwargs` translates it
    into obstore options (``skip_signature`` for anonymous, static keys, region,
    endpoint) to keep the fast obstore path. Configs that can't be represented as
    static options -- assume-role (rotating creds) or an unrecognized pickle
    shape -- return ``None`` there and route to the threaded path instead.

    **fsspec S3 (``PyFileSystem`` + ``FSSpecHandler``):** For ``s3`` / ``s3a``
    (e.g. ``s3fs`` with STS/Okta/custom endpoints), credentials are read from
    ``storage_options`` / common instance attributes first; if no static key
    is present, we install a :class:`_S3FSSessionCredentialProvider` callback
    so obstore can refresh session-backed (Okta/STS/profile) credentials on
    expiry instead of using a stale snapshot. If both passes come up empty we
    return ``None`` so the caller routes to the threaded path, keeping the
    user's fsspec filesystem authoritative.

    Other ``PyFileSystem`` handlers (non-fsspec or non-S3 fsspec protocols) are
    not converted here; see :func:`_obstore_filesystem_requires_threaded_download`.

    Args:
        filesystem: A PyArrow filesystem instance.

    Returns:
        A dict of keyword arguments for ``obstore.store.from_url``, or
        ``None`` when the filesystem was recognized as fsspec-S3 but we
        couldn't pull usable credentials (caller must fall back).
    """
    if filesystem is None:
        return {}

    # Unwrap RetryingPyFileSystem if present
    if isinstance(filesystem, RetryingPyFileSystem):
        filesystem = filesystem.unwrap()

    kwargs: Dict[str, Any] = {}

    S3FileSystem = getattr(pyarrow.fs, "S3FileSystem", None)
    GcsFileSystem = getattr(pyarrow.fs, "GcsFileSystem", None)
    AzureFileSystem = getattr(pyarrow.fs, "AzureFileSystem", None)
    PyFileSystem = getattr(pyarrow.fs, "PyFileSystem", None)
    FSSpecHandler = getattr(pyarrow.fs, "FSSpecHandler", None)

    if S3FileSystem is not None and isinstance(filesystem, S3FileSystem):
        return _native_s3_obstore_kwargs(filesystem)

    if GcsFileSystem is not None and isinstance(filesystem, GcsFileSystem):
        # obstore GCSConfig does not have a project_id field. The only useful
        # attribute PyArrow exposes on GcsFileSystem is `anonymous`, which maps
        # to obstore's `skip_signature`. All other credentials (service account,
        # application default credentials) are resolved by obstore from the
        # environment automatically.
        if getattr(filesystem, "anonymous", False):
            kwargs["skip_signature"] = True
        return kwargs

    if AzureFileSystem is not None and isinstance(filesystem, AzureFileSystem):
        for attr in ("account_name", "account_key"):
            val = getattr(filesystem, attr, None)
            if val:
                kwargs[attr] = val
        return kwargs

    if (
        PyFileSystem is not None
        and FSSpecHandler is not None
        and isinstance(filesystem, PyFileSystem)
        and isinstance(filesystem.handler, FSSpecHandler)
    ):
        # fsspec-backed FS (e.g. s3fs with Okta/STS) wrapped for PyArrow.
        fsspec_fs = getattr(filesystem.handler, "fs", None)
        if fsspec_fs is None:
            return None
        protocol = getattr(fsspec_fs, "protocol", None)
        if isinstance(protocol, tuple):
            protocol = protocol[0] if protocol else None
        if protocol not in ("s3", "s3a"):
            # Non-S3 fsspec — obstore can't use these. Callers usually filter
            # this out via _obstore_filesystem_requires_threaded_download, but
            # direct callers must also route to the threaded path.
            return None
        opts = getattr(fsspec_fs, "storage_options", None) or {}
        if not isinstance(opts, dict):
            opts = {}
        key = opts.get("key") or getattr(fsspec_fs, "key", None)
        secret = opts.get("secret") or getattr(fsspec_fs, "secret", None)
        token = opts.get("token") or getattr(fsspec_fs, "token", None)
        endpoint = opts.get("endpoint_url") or getattr(fsspec_fs, "endpoint_url", None)
        client_kwargs = opts.get("client_kwargs") or getattr(
            fsspec_fs, "client_kwargs", None
        )
        if isinstance(client_kwargs, dict):
            endpoint = endpoint or client_kwargs.get("endpoint_url")
            region_name = client_kwargs.get("region_name")
            if region_name:
                kwargs["region"] = region_name
        region_opt = opts.get("region_name")
        if region_opt:
            kwargs["region"] = region_opt
        if key:
            kwargs["access_key_id"] = key
        if secret:
            kwargs["secret_access_key"] = secret
        if token:
            kwargs["session_token"] = token
        if endpoint:
            kwargs["endpoint"] = endpoint
        anon = opts.get("anon") or getattr(fsspec_fs, "anon", False)
        if anon:
            kwargs["skip_signature"] = True

        # Static attrs didn't yield an access key and the user didn't opt into
        # anonymous — install a credential_provider so obstore refreshes the
        # session-backed keys (Okta/STS/profile-based) on expiry instead of
        # using a stale snapshot.
        if "access_key_id" not in kwargs and not anon:
            provider = _S3FSSessionCredentialProvider.from_fsspec_fs(fsspec_fs)
            if provider is None or not provider.can_fetch_credentials():
                return None
            kwargs["credential_provider"] = provider
        return kwargs

    # Unrecognized non-None filesystem — route to threaded so the user's FS
    # stays authoritative. Obstore with empty kwargs would silently use its
    # own credential chain, dropping any configuration on the user's FS.
    return None


def _obstore_filesystem_requires_threaded_download(
    filesystem: Optional["pyarrow.fs.FileSystem"],
) -> bool:
    """Return True if obstore must not be used so the user's FS stays authoritative.

    ``PyFileSystem`` instances that are not ``FSSpecHandler`` wrapping ``s3`` /
    ``s3a`` cannot have credentials merged into obstore's ``from_url`` in a
    reliable way. Using obstore with empty kwargs would ignore the user's
    filesystem (Okta, STS, custom protocols) and tends to produce confusing
    403s. The threaded download path passes the filesystem through to PyArrow
    ``open_input_stream``, matching pre-obstore behavior.

    ``None`` means no explicit filesystem (URI-driven resolution) — obstore is OK.
    """
    if filesystem is None:
        return False

    if isinstance(filesystem, RetryingPyFileSystem):
        filesystem = filesystem.unwrap()

    PyFileSystem = getattr(pyarrow.fs, "PyFileSystem", None)
    FSSpecHandler = getattr(pyarrow.fs, "FSSpecHandler", None)
    if PyFileSystem is None or not isinstance(filesystem, PyFileSystem):
        return False
    if FSSpecHandler is None or not isinstance(filesystem.handler, FSSpecHandler):
        return True

    fsspec_fs = getattr(filesystem.handler, "fs", None)
    if fsspec_fs is None:
        return True

    protocol = getattr(fsspec_fs, "protocol", None)
    if isinstance(protocol, tuple):
        protocol = protocol[0] if protocol else None
    if protocol in ("s3", "s3a"):
        return False
    return True


def _is_fsspec_s3_filesystem(filesystem: "pyarrow.fs.FileSystem") -> bool:
    """True if *filesystem* is a PyFileSystem wrapping an s3fs ``S3FileSystem``."""
    if isinstance(filesystem, RetryingPyFileSystem):
        filesystem = filesystem.unwrap()
    if not isinstance(filesystem, pyarrow.fs.PyFileSystem):
        return False
    if not isinstance(filesystem.handler, pyarrow.fs.FSSpecHandler):
        return False
    try:
        from s3fs import S3FileSystem
    except ImportError:
        return False
    return isinstance(filesystem.handler.fs, S3FileSystem)


# Per-process dedup for credential-extraction warnings. Keyed by ``id(fs)`` so
# each distinct filesystem object warns exactly once in a worker.
_warned_credential_fs_ids: set = set()


def _warn_fsspec_s3_credentials_unextractable(
    filesystem: "pyarrow.fs.FileSystem",
) -> None:
    """Emit a one-shot WARNING for fsspec-S3 filesystems with unextractable creds.

    Only call this for filesystems where ``_is_fsspec_s3_filesystem`` is True —
    the message hardcodes fsspec-specific advice (``storage_options``) and
    would be misleading for native filesystems like ``LocalFileSystem`` or
    ``HdfsFileSystem``.
    """
    fs_id = id(filesystem)
    if fs_id in _warned_credential_fs_ids:
        return
    _warned_credential_fs_ids.add(fs_id)

    unwrapped = (
        filesystem.unwrap()
        if isinstance(filesystem, RetryingPyFileSystem)
        else filesystem
    )
    handler = getattr(unwrapped, "handler", None)
    inner = getattr(handler, "fs", None)
    inner_class = type(inner).__name__ if inner is not None else "unknown"

    logger.warning(
        "Could not extract S3 credentials from user-supplied fsspec "
        "filesystem (inner: %s); falling back to the PyArrow threaded "
        "download path so the filesystem's own credential resolution stays "
        "authoritative. If this is unexpected, pass credentials via fsspec "
        "``storage_options`` (e.g. key/secret/token) so they can be "
        "forwarded to obstore.",
        inner_class,
    )


def _plan_obstore_routing(
    filesystem: Optional["pyarrow.fs.FileSystem"],
) -> "tuple[bool, Dict[str, Any]]":
    """Decide whether to use obstore and return the kwargs to forward.

    Returns ``(True, fs_kwargs)`` if obstore should be used, or ``(False, {})``
    if the caller must route to the threaded path. Emits a ``WARNING`` only
    when an fsspec-S3 filesystem was supplied but its credentials couldn't be
    extracted — routing native / unrecognized filesystems to the threaded
    path is the expected behavior and is logged at ``DEBUG``.
    """
    if _obstore_filesystem_requires_threaded_download(filesystem):
        return False, {}
    fs_kwargs = _extract_credentials_from_filesystem(filesystem)
    if fs_kwargs is None:
        assert filesystem is not None  # None filesystem always returns {}
        if _is_fsspec_s3_filesystem(filesystem):
            _warn_fsspec_s3_credentials_unextractable(filesystem)
        else:
            logger.debug(
                "Routing filesystem %s to the PyArrow threaded download path "
                "(obstore cannot forward credentials for this filesystem type).",
                type(filesystem).__name__,
            )
        return False, {}
    return True, fs_kwargs


# Per-process cache: AWS bucket name -> region (``None`` if discovery failed).
# Buckets do not change region, so caching for the process lifetime is safe.
_BUCKET_REGION_CACHE: Dict[str, Optional[str]] = {}
_BUCKET_REGION_CACHE_LOCK = threading.Lock()


def _discover_aws_bucket_region(bucket: str) -> Optional[str]:
    """Discover an AWS S3 bucket's region, cached per-process.

    Delegates to :func:`pyarrow.fs.resolve_s3_region`, which issues a HEAD
    probe and reads ``x-amz-bucket-region`` (AWS returns it for both 200 OK
    and 301 PermanentRedirect responses). PyArrow is already a required Ray
    Data dependency and caches region lookups in its C++ S3 layer; the extra
    per-process dict here also caches negative results, so a bucket that can't
    be resolved (network error, non-AWS endpoint, or a PyArrow build without
    S3 support) is probed at most once.

    Returns ``None`` when the region cannot be determined. Callers fall back
    to obstore's default region handling, which preserves prior behavior for
    non-AWS S3-compatible backends (MinIO, R2, etc.).
    """
    with _BUCKET_REGION_CACHE_LOCK:
        if bucket in _BUCKET_REGION_CACHE:
            return _BUCKET_REGION_CACHE[bucket]

    # Probe outside the lock so concurrent first-time lookups don't serialize
    # on a network round-trip.
    region: Optional[str] = None
    try:
        region = pyarrow.fs.resolve_s3_region(bucket)
    except Exception as e:
        # Includes AttributeError on PyArrow builds compiled without S3
        # support, where ``resolve_s3_region`` is absent.
        logger.debug("Failed to discover region for bucket %r: %s", bucket, e)

    with _BUCKET_REGION_CACHE_LOCK:
        # Another thread may have resolved the same bucket while we probed.
        # A real region must win over a ``None`` result: never let a failed
        # probe overwrite a region a concurrent thread already cached, or
        # later ``StoreRegistry.get`` calls would skip region injection and
        # cross-region downloads could fail intermittently.
        cached = _BUCKET_REGION_CACHE.get(bucket)
        if cached is not None:
            return cached
        _BUCKET_REGION_CACHE[bucket] = region
    return region


class StoreRegistry:
    """Cache of store_url -> ObjectStore instances.

    Ensures one store is created per unique (scheme, authority) combination
    and reuses it for all subsequent requests to the same host.
    """

    def __init__(
        self,
        retry_config: Optional[Dict[str, Any]] = None,
        **filesystem_kwargs: Any,
    ):
        """Create the registry.

        Args:
            retry_config: obstore ``RetryConfig`` for every store created by
                this registry. ``None`` (the default) uses the env-configurable
                budget from ``_obstore_retry_config_from_env``; pass an explicit
                dict to override it.
            **filesystem_kwargs: Forwarded to ``obstore.store.from_url``.
        """
        from obstore.store import from_url

        self._from_url = from_url
        self._retry_config = (
            retry_config
            if retry_config is not None
            else _obstore_retry_config_from_env()
        )
        self._filesystem_kwargs = filesystem_kwargs
        self._cache: Dict[str, Any] = {}

    def get(self, store_url: str) -> Any:
        if store_url not in self._cache:
            kwargs = dict(self._filesystem_kwargs)
            # obstore's S3Store defaults ``region`` to ``us-east-1`` and does
            # not auto-discover the bucket's real region, so cross-region
            # buckets fail with ``BareRedirect`` (AWS's PermanentRedirect
            # omits the ``Location`` header, defeating generic redirect
            # following). Probe ``x-amz-bucket-region`` once per bucket and
            # supply the discovered region explicitly. Skip when the caller
            # already provided a region or a custom endpoint (MinIO/R2/etc.).
            if (
                store_url.startswith(("s3://", "s3a://"))
                and "region" not in kwargs
                and "endpoint" not in kwargs
            ):
                bucket = store_url.split("://", 1)[1].split("/", 1)[0]
                if bucket:
                    region = _discover_aws_bucket_region(bucket)
                    if region:
                        kwargs["region"] = region
            env_client_options = _obstore_client_options_from_env()
            if env_client_options:
                # Caller-supplied options (e.g. from a user filesystem) win
                # over the environment defaults.
                kwargs["client_options"] = {
                    **env_client_options,
                    **(kwargs.get("client_options") or {}),
                }
            if store_url.startswith("http://"):
                # obstore's reqwest client rejects http:// by default. Auto-enable it
                # to maintain parity with PyArrow (which accepts http:// via fsspec),
                # but warn about unencrypted traffic.
                client_options = {
                    **(kwargs.get("client_options") or {}),
                    "allow_http": True,
                }
                kwargs["client_options"] = client_options
                if not getattr(self, "_warned_http", False):
                    self._warned_http = True
                    logger.warning(
                        "Downloading over unencrypted HTTP. "
                        "Consider using https:// instead."
                    )
            from_url_kwargs: Any = {
                "retry_config": self._retry_config,
                **kwargs,
            }
            self._cache[store_url] = self._from_url(store_url, **from_url_kwargs)
        return self._cache[store_url]


def _yield_threaded_download_bytes(
    block: pa.Table,
    uri_column_names: List[str],
    output_bytes_column_names: List[str],
    data_context: "DataContext",
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
) -> Iterator[pa.Table]:
    """Delegate to ``download_bytes_threaded`` after dropping internal size columns.

    ``AsyncPartitionActor`` may attach ``__ray_file_size__*`` columns that the
    threaded downloader does not handle.
    """
    size_cols = [
        f"{_FILE_SIZE_COLUMN_PREFIX}{name}"
        for name in uri_column_names
        if f"{_FILE_SIZE_COLUMN_PREFIX}{name}" in block.column_names
    ]
    if size_cols:
        block = block.drop(size_cols)

    from ray.data._internal.planner.plan_download_op import download_bytes_threaded

    yield from download_bytes_threaded(
        block,
        uri_column_names,
        output_bytes_column_names,
        data_context,
        filesystem,
    )


# Public entry point
def download_bytes_async(
    block: pa.Table,
    uri_column_names: List[str],
    output_bytes_column_names: List[str],
    data_context: "DataContext",
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
) -> Iterator[pa.Table]:
    """Download bytes for URI columns using obstore's async API.

    For URI schemes not supported by obstore, falls back to the threaded
    PyArrow download path via lazy import.

    Args:
        block: Input PyArrow table containing URI columns.
        uri_column_names: Names of columns containing URIs to download.
        output_bytes_column_names: Names for the output columns containing
            downloaded bytes.
        data_context: Ray Data context for configuration.
        filesystem: PyArrow filesystem to use for reading remote files.
            If None, the filesystem is auto-detected from the path scheme.

    Yields:
        pa.Table: PyArrow table with the downloaded bytes added as new columns.
    """
    if not isinstance(block, pa.Table):
        block = BlockAccessor.for_block(block).to_arrow()

    first_uris = block.column(uri_column_names[0]).to_pylist()
    if not first_uris:
        yield block
        return

    # Fall back to PyArrow for URI schemes obstore doesn't handle.
    if not _is_obstore_supported_url(first_uris[0]):
        logger.debug(
            "URI scheme not supported by obstore (first URI: %s); "
            "falling back to PyArrow threaded download.",
            first_uris[0],
        )
        yield from _yield_threaded_download_bytes(
            block,
            uri_column_names,
            output_bytes_column_names,
            data_context,
            filesystem,
        )
        return

    # Resolve credentials up front in sync context. This is the only safe place
    # to call ``_extract_credentials_from_filesystem`` for fsspec sessions that
    # need ``asyncio.run`` internally — once we're inside ``_download_uris_with_obstore``
    # we're already in an event loop and that path is unavailable.
    use_obstore, fs_kwargs = _plan_obstore_routing(filesystem)
    if not use_obstore:
        yield from _yield_threaded_download_bytes(
            block,
            uri_column_names,
            output_bytes_column_names,
            data_context,
            filesystem,
        )
        return

    output_block = block

    for uri_column_name, output_bytes_column_name in zip(
        uri_column_names, output_bytes_column_names
    ):
        uris = output_block.column(uri_column_name).to_pylist()

        if not uris:
            continue

        # Read pre-computed file sizes from AsyncPartitionActor if available.
        size_col = f"{_FILE_SIZE_COLUMN_PREFIX}{uri_column_name}"
        file_sizes = None
        if size_col in output_block.column_names:
            file_sizes = output_block.column(size_col).to_pylist()

        uri_bytes = asyncio.run(
            _download_uris_with_obstore(
                uris,
                uri_column_name,
                fs_kwargs=fs_kwargs,
                file_sizes=file_sizes,
            )
        )

        output_block = output_block.append_column(
            output_bytes_column_name, pa.array(uri_bytes)
        )

    # Drop internal file-size columns before yielding output.
    size_cols = [
        f"{_FILE_SIZE_COLUMN_PREFIX}{name}"
        for name in uri_column_names
        if f"{_FILE_SIZE_COLUMN_PREFIX}{name}" in output_block.column_names
    ]
    if size_cols:
        output_block = output_block.drop(size_cols)

    yield from _iter_arrow_table_for_target_max_block_size(
        output_block, data_context.target_max_block_size
    )


# Core async download logic
async def _download_uris_with_obstore(
    uris: List[Any],
    uri_column_name: str,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    file_sizes: Optional[List[Optional[int]]] = None,
    fs_kwargs: Optional[Dict[str, Any]] = None,
) -> List[Optional[bytes]]:
    """Download URIs concurrently using obstore's async API.

    Creates and caches one ``ObjectStore`` per unique (scheme, authority)
    combination, then fires all downloads concurrently via asyncio.gather.
    If a PyArrow *filesystem* is provided, its credentials are extracted
    and forwarded to obstore's store construction.

    When ``RAY_DATA_OBSTORE_RANGE_THRESHOLD`` is set to a positive value,
    files with a known size larger than the threshold are downloaded as
    parallel range chunks via ``get_range_async``. Unknown sizes use a simple
    GET, which avoids a full-batch HEAD pass for large URI lists.

    Args:
        uris: URIs to download. ``None`` entries stay ``None``; other
            non-string entries (NaN, ``pd.NA``, ``""``) yield ``None`` and are
            counted as invalid in the per-block warning.
        uri_column_name: Column name (used only for error logging).
        filesystem: Optional PyArrow filesystem whose credentials are
            forwarded to the obstore store. Ignored when *fs_kwargs* is given.
        file_sizes: Optional per-URI file sizes from AsyncPartitionActor.
            ``0`` or ``None`` entries mean "unknown size" and are downloaded
            with a simple GET when range splitting is enabled.
        fs_kwargs: Pre-extracted obstore kwargs from the planner. Preferred
            over *filesystem* because it sidesteps re-extracting credentials
            from inside the event loop (aiobotocore sessions need ``asyncio.run``).

    Returns:
        Downloaded bytes in the same order as *uris*.  ``None`` entries are
        URIs that can never be downloaded (missing object, permission denied,
        invalid URI). A transient failure that outlasts the retries raises
        ``DownloadError`` for the whole call, as does a block in which every
        URI is permission denied.
    """
    if fs_kwargs is None:
        # Direct-caller path (tests, internal helpers). Session-backed fsspec
        # may fail here because we may already be inside an event loop; the
        # planner path avoids this by pre-extracting upfront and passing
        # ``fs_kwargs``. Re-extract best-effort, but fail closed if the
        # filesystem is non-None and extraction signals "not extractable" —
        # silently handing obstore the ambient credential chain is exactly
        # the bug the new routing was designed to prevent.
        extracted = _extract_credentials_from_filesystem(filesystem)
        if extracted is None:
            raise RuntimeError(
                "_download_uris_with_obstore was called with a filesystem whose "
                f"credentials cannot be statically extracted ({type(filesystem).__name__}). "
                "Pass ``fs_kwargs`` explicitly, or route through "
                "``_plan_obstore_routing`` / ``download_bytes_async`` so "
                "non-extractable filesystems take the threaded PyArrow path."
            )
        fs_kwargs = extracted

    range_threshold = RAY_DATA_OBSTORE_RANGE_THRESHOLD
    range_chunk_size = RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE
    max_conc = RAY_DATA_OBSTORE_MAX_CONCURRENCY
    if range_threshold > 0:
        if max_conc <= 0:
            logger.warning(
                "RAY_DATA_OBSTORE_RANGE_THRESHOLD is set but "
                "RAY_DATA_OBSTORE_MAX_CONCURRENCY=%d is invalid. "
                "Range downloads require a positive concurrency limit to avoid "
                "socket exhaustion. Disabling range splitting.",
                max_conc,
            )
            range_threshold = 0
        elif range_chunk_size <= 0:
            logger.warning(
                "RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE=%d is invalid (must be > 0). "
                "Disabling range splitting to avoid empty range tasks and "
                "zero-filled bogus downloads.",
                range_chunk_size,
            )
            range_threshold = 0
    sem = asyncio.Semaphore(max_conc) if max_conc > 0 else None

    registry = StoreRegistry(**fs_kwargs)

    # 0 = unknown size. Unknown entries intentionally stay unknown so large
    # batches do not pay one HEAD request per URI before downloading.
    sizes = [0] * len(uris)
    if range_threshold > 0 and file_sizes is not None:
        for i, size in enumerate(file_sizes[: len(uris)]):
            sizes[i] = size if size is not None and size > 0 else 0
    if range_threshold <= 0:
        logger.debug(
            "Range splitting disabled (threshold=%d); downloading %d URIs via simple GET.",
            range_threshold,
            len(uris),
        )

    def fetch(index: int):
        uri = uris[index]
        if range_threshold > 0 and sizes[index] > range_threshold:
            assert sem is not None
            return _fetch_ranged(
                uri, sizes[index], registry, uri_column_name, sem, range_chunk_size
            )
        return _fetch_whole(uri, registry, uri_column_name, sem)

    results: List[Optional[bytes]] = [None] * len(uris)
    skipped: "Counter[str]" = Counter()
    pending: List[int] = []
    for index, uri in enumerate(uris):
        if uri is None:
            # A null input is a legitimate "no value"; it stays null quietly.
            continue
        if _is_usable_uri(uri):
            pending.append(index)
        else:
            skipped["invalid uri"] += 1

    # Permanent failures (missing object, access denied, invalid URI,
    # unrecognised store URL) become ``None``. Transient failures are
    # re-fetched a bounded number of times, re-using the bytes already
    # downloaded; if any still fail, raise.
    denied: List[_DownloadFailure] = []
    max_attempts, backoff_ms = _download_attempt_settings()
    for attempt in range(1, max_attempts + 1):
        outcomes = await asyncio.gather(*[fetch(index) for index in pending])
        # Every store in the block shares ``fs_kwargs``. If not a single store
        # could be built, the problem is that configuration (an option the
        # store rejects, bad credential settings, a negative duration) rather
        # than one URI, and it must surface as the real error instead of a
        # column of nulls. A store failing for some URIs is row-local. This
        # does not depend on URI order, unlike probing the first URI would.
        store_failures = [
            outcome
            for outcome in outcomes
            if isinstance(outcome, _DownloadFailure) and outcome.store_failure
        ]
        if pending and len(store_failures) == len(outcomes):
            raise store_failures[0].error
        transient: List[Tuple[int, _DownloadFailure]] = []
        for index, outcome in zip(pending, outcomes):
            if not isinstance(outcome, _DownloadFailure):
                results[index] = outcome
            elif outcome.permanent:
                skipped[outcome.reason] += 1
                if outcome.reason == "access denied":
                    denied.append(outcome)
                logger.debug(
                    "Skipping uri %r for column %r: %s",
                    outcome.uri,
                    uri_column_name,
                    outcome.error,
                )
            else:
                transient.append((index, outcome))
        if not transient:
            break
        first = transient[0][1]
        if attempt == max_attempts:
            _log_skipped_uris(uri_column_name, skipped, len(uris))
            raise _download_error(
                first.uri, uri_column_name, first.error, attempts=attempt
            ) from first.error
        delay = _retry_backoff_s(attempt, backoff_ms)
        logger.warning(
            "Re-fetching %d of %d URIs for column %r after transient download "
            "failures (attempt %d of %d, waiting %.1fs). First failure: %r: %s",
            len(transient),
            len(uris),
            uri_column_name,
            attempt,
            max_attempts,
            delay,
            first.uri,
            first.error,
        )
        if delay > 0:
            await asyncio.sleep(delay)
        pending = [index for index, _ in transient]

    _raise_if_all_denied(uri_column_name, uris, denied)
    _log_skipped_uris(uri_column_name, skipped, len(uris))
    return results


# Async helpers
async def _fetch_whole(
    uri: str,
    registry: StoreRegistry,
    uri_column_name: str,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> Union[bytes, _DownloadFailure]:
    """Download a single URI as a whole-file GET.

    Returns the bytes, or a :class:`_DownloadFailure` saying why not. A
    permanent failure (missing object, access denied, invalid URI,
    unrecognised store URL) becomes ``None`` for the row; a transient one is
    re-fetched by the caller and raises ``DownloadError`` once the attempts
    are used up. A store that cannot be built for this URI's store URL (an
    unrecognised or malformed store URL, or a scheme the block's shared
    options don't fit) is reported as a permanent ``store_failure``; the
    caller raises it only if no store in the block could be built, which
    points at the shared configuration rather than at one URI.
    """
    import obstore as obs

    try:
        store_url, path = _split_obstore_uri(uri)
    except Exception as e:
        return _DownloadFailure(uri, "invalid uri", e, permanent=True)
    try:
        store = registry.get(store_url)
    except Exception as e:
        return _DownloadFailure(
            uri, _store_failure_reason(e), e, permanent=True, store_failure=True
        )
    try:
        if semaphore is not None:
            async with semaphore:
                result = await obs.get_async(store, path)
                return bytes(await result.bytes_async())
        # No semaphore (RAY_DATA_OBSTORE_MAX_CONCURRENCY=0).
        # Concurrency is bounded only by the partition actor batch size.
        result = await obs.get_async(store, path)
        return bytes(await result.bytes_async())
    except Exception as e:
        return _DownloadFailure(
            uri, _skip_reason(e), e, permanent=_is_permanent_download_error(e)
        )


async def _fetch_ranged(
    uri: str,
    size: int,
    registry: StoreRegistry,
    uri_column_name: str,
    semaphore: asyncio.Semaphore,
    range_chunk_size: int,
) -> Union[bytes, _DownloadFailure]:
    """Download a single large file using parallel range requests.

    Classifies failures like :func:`_fetch_whole`. A failed chunk falls back
    to a simple GET only when the whole-file request could succeed where the
    range request did not (a store that does not honour range requests, a
    chunk of the wrong size). A missing, denied or unauthenticated object,
    or a chunk on which obstore already spent its retry budget
    (``GenericError``), is reported directly: the fallback would only repeat
    the same request against the same store, doubling the time to fail.
    """
    try:
        store_url, path = _split_obstore_uri(uri)
    except Exception as e:
        return _DownloadFailure(uri, "invalid uri", e, permanent=True)
    try:
        store = registry.get(store_url)
    except Exception as e:
        return _DownloadFailure(
            uri, _store_failure_reason(e), e, permanent=True, store_failure=True
        )
    result = bytearray(size)

    tasks = [
        asyncio.ensure_future(
            _fetch_chunk(
                uri,
                store,
                path,
                chunk_start,
                min(chunk_start + range_chunk_size, size),
                result,
                semaphore,
            )
        )
        for chunk_start in range(0, size, range_chunk_size)
    ]

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        # Cancel remaining chunk tasks to release semaphore permits
        # and avoid wasted downloads into a bytearray we'll discard.
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if isinstance(e, _RANGE_ERROR_TYPES_WITHOUT_FALLBACK):
            return _DownloadFailure(
                uri, _skip_reason(e), e, permanent=_is_permanent_download_error(e)
            )
        logger.debug(
            "Ranged download failed for %r, falling back to simple GET: %s", uri, e
        )
        return await _fetch_whole(uri, registry, uri_column_name, semaphore)
    return bytes(result)


async def _fetch_chunk(
    uri: str,
    store: Any,
    path: str,
    start: int,
    end: int,
    result: bytearray,
    semaphore: asyncio.Semaphore,
) -> None:
    """Download a single byte range and write it into *result* in place."""
    import obstore as obs

    async with semaphore:
        chunk = await obs.get_range_async(store, path, start=start, end=end)
        expected = end - start
        if len(chunk) != expected:
            raise IOError(
                f"Range request for {uri!r} returned {len(chunk)} "
                f"bytes, expected {expected}"
            )
        result[start:end] = bytes(chunk)
