# coding: utf-8
import asyncio
import logging
from typing import Any, Optional, Union

from ray._private import ray_constants
from ray.exceptions import GcsPassiveError
from ray.experimental.internal_kv import internal_kv_get_gcs_client

logger = logging.getLogger(__name__)


def is_refused_by_passive_gcs(
    exc: Exception, latch: Optional["PassiveLatch"] = None
) -> bool:
    """Returns whether the exception was a refusal by a passive GCS."""
    if not (
        ray_constants.RAY_ENABLE_GCS_LEADER_ELECTION
        and isinstance(exc, GcsPassiveError)
    ):
        return False
    if latch is not None:
        latch.note_passive()
    return True


class PassiveLatch:
    """Tracks passive GCS state with edge-triggered warning and info logging.

    Logs a warning on the rising edge of entering passive mode (the first rejection),
    and an info message when the component is promoted to leader / resumes.
    """

    def __init__(
        self,
        component_desc: str,
        module_logger: logging.Logger,
        *,
        action_desc_passive: Optional[str] = None,
        action_desc_promoted: Optional[str] = None,
    ):
        self._desc = component_desc
        self._logger = module_logger
        self._waiting_for_promotion = False
        self._action_desc_passive = (
            action_desc_passive
            or f"GCS is in passive mode and refused the {component_desc} registration. "
            "Retrying until this GCS is promoted."
        )
        self._action_desc_promoted = (
            action_desc_promoted
            or f"GCS was promoted to leader. Registered the {component_desc}."
        )

    @property
    def waiting_for_promotion(self) -> bool:
        return self._waiting_for_promotion

    @waiting_for_promotion.setter
    def waiting_for_promotion(self, value: bool) -> None:
        self._waiting_for_promotion = value

    def note_passive(self) -> bool:
        """Record passive state and log warning if this is the first refusal.

        Returns True on the rising edge (was not previously waiting).
        """
        if not self._waiting_for_promotion:
            self._waiting_for_promotion = True
            self._logger.warning(self._action_desc_passive)
            return True
        return False

    def promoted(self, *args, **kwargs) -> bool:
        """Clear the latch when promoted to leader, logging once on the transition.

        Returns True if this was a rising edge of promotion (was previously waiting).
        """
        if self._waiting_for_promotion:
            self._waiting_for_promotion = False
            self._logger.info(self._action_desc_promoted, *args, **kwargs)
            return True
        return False


async def _async_is_gcs_leader(gcs_client: Any, interval: float = 2.0) -> bool:
    """Asynchronously check if the GCS is currently leader without blocking the event loop.

    Args:
        gcs_client: The GcsClient instance to query.
        interval: Timeout in seconds for the aliveness probe.

    Returns:
        True if the GCS is currently the active leader (or if leader election
        is disabled), False if it is standing by in passive mode.
    """
    if not ray_constants.RAY_ENABLE_GCS_LEADER_ELECTION:
        return True

    # In production, GcsClient always provides async_check_alive and is_gcs_leader_local.
    # This mirrors the C++ Cython GcsClient.is_gcs_leader() logic asynchronously:
    # issue an async check_alive to refresh the cache, then read the local status.
    async_check_alive = getattr(gcs_client, "async_check_alive", None)
    if callable(async_check_alive):
        try:
            res = async_check_alive(node_ids=[], timeout=interval)
            if asyncio.iscoroutine(res):
                await res
        except Exception:
            # GCS may be unreachable or slow during a failover;
            # fall back to the cached local state, exactly matching sync is_gcs_leader().
            pass
        if hasattr(gcs_client, "is_gcs_leader_local"):
            return bool(gcs_client.is_gcs_leader_local())

    # Fallback only for unit test mocks that do not mock async_check_alive
    is_local_fn = getattr(gcs_client, "is_gcs_leader_local", None)
    if callable(is_local_fn):
        return bool(is_local_fn())
    is_leader_fn = getattr(gcs_client, "is_gcs_leader", None)
    if callable(is_leader_fn):
        res = is_leader_fn()
        return bool(res)

    return False


async def wait_until_gcs_leader(
    gcs_client: Any,
    poll_interval_s: float = 5.0,
    check_timeout_s: float = 2.0,
    *,
    latch: Optional[PassiveLatch] = None,
) -> None:
    """Asynchronously wait until the local GCS is promoted to leader.

    If RAY_ENABLE_GCS_LEADER_ELECTION is disabled, returns immediately.
    Logs warning once when parked, and info once when promoted if latch is provided.
    """
    if not ray_constants.RAY_ENABLE_GCS_LEADER_ELECTION:
        return

    while not await _async_is_gcs_leader(gcs_client, interval=check_timeout_s):
        if latch is not None:
            latch.note_passive()
        await asyncio.sleep(poll_interval_s)

    if latch is not None:
        latch.promoted()


def _resolve_gcs_client(gcs_client: Optional[Any]) -> Any:
    if gcs_client is not None:
        return gcs_client

    client = internal_kv_get_gcs_client()
    if client is None:
        raise RuntimeError(
            "No gcs_client provided and ray.experimental.internal_kv is not initialized."
        )
    return client


def _format_passive_skip_warning(
    key: Union[str, bytes], action: str = "writing"
) -> str:
    key_str = (
        key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
    )
    preposition = "to" if action == "writing" else "from"
    return f"GCS is in passive mode. Skipping {action} {key_str} {preposition} KV."


def put_kv_passive_safe(
    gcs_client: Optional[Any],
    key: Union[str, bytes],
    value: Union[str, bytes],
    overwrite: bool = True,
    *,
    namespace: Optional[Union[str, bytes]] = None,
    timeout: Optional[float] = None,
    latch: Optional[PassiveLatch] = None,
    warning_message: Optional[Union[str, bool]] = None,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Put a key-value pair to GCS KV store, returning False if refused by a passive GCS."""
    client = _resolve_gcs_client(gcs_client)
    raw_key = key
    if isinstance(key, str):
        key = key.encode()
    if isinstance(value, str):
        value = value.encode()
    if isinstance(namespace, str):
        namespace = namespace.encode()

    kwargs = {}
    if timeout is not None:
        kwargs["timeout"] = timeout

    try:
        client.internal_kv_put(key, value, overwrite, namespace=namespace, **kwargs)
        return True
    except Exception as e:
        if not is_refused_by_passive_gcs(e, latch=latch):
            raise
        if warning_message:
            msg = (
                _format_passive_skip_warning(raw_key, "writing")
                if warning_message is True
                else str(warning_message)
            )
            log = logger or logging.getLogger(__name__)
            log.warning(msg)
        return False


async def async_put_kv_passive_safe(
    gcs_client: Optional[Any],
    key: Union[str, bytes],
    value: Union[str, bytes],
    overwrite: bool = True,
    *,
    namespace: Optional[Union[str, bytes]] = None,
    timeout: Optional[float] = None,
    latch: Optional[PassiveLatch] = None,
    warning_message: Optional[Union[str, bool]] = None,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Asynchronously put a key-value pair to GCS KV store, returning False if refused."""
    client = _resolve_gcs_client(gcs_client)
    raw_key = key
    if isinstance(key, str):
        key = key.encode()
    if isinstance(value, str):
        value = value.encode()
    if isinstance(namespace, str):
        namespace = namespace.encode()

    kwargs = {}
    if timeout is not None:
        kwargs["timeout"] = timeout

    try:
        await client.async_internal_kv_put(
            key, value, overwrite, namespace=namespace, **kwargs
        )
        return True
    except Exception as e:
        if not is_refused_by_passive_gcs(e, latch=latch):
            raise
        if warning_message:
            msg = (
                _format_passive_skip_warning(raw_key, "writing")
                if warning_message is True
                else str(warning_message)
            )
            log = logger or logging.getLogger(__name__)
            log.warning(msg)
        return False


def del_kv_passive_safe(
    gcs_client: Optional[Any],
    key: Union[str, bytes],
    *,
    del_by_prefix: bool = False,
    namespace: Optional[Union[str, bytes]] = None,
    timeout: Optional[float] = None,
    latch: Optional[PassiveLatch] = None,
    warning_message: Optional[Union[str, bool]] = None,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Delete a key or prefix from GCS KV store, returning False if refused by a passive GCS."""
    client = _resolve_gcs_client(gcs_client)
    raw_key = key
    if isinstance(key, str):
        key = key.encode()
    if isinstance(namespace, str):
        namespace = namespace.encode()

    kwargs = {}
    if timeout is not None:
        kwargs["timeout"] = timeout

    try:
        client.internal_kv_del(key, del_by_prefix, namespace=namespace, **kwargs)
        return True
    except Exception as e:
        if not is_refused_by_passive_gcs(e, latch=latch):
            raise
        if warning_message:
            msg = (
                _format_passive_skip_warning(raw_key, "deleting")
                if warning_message is True
                else str(warning_message)
            )
            log = logger or logging.getLogger(__name__)
            log.warning(msg)
        return False
