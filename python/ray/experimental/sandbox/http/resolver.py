"""Creating and resolving the named detached ``SandboxHost`` actors.

Shared by the REST app (``app.py``) and the gRPC facade, and free of FastAPI
so the facade runs without the Serve extra.
"""

import hashlib
import logging
import threading
import uuid
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from ray.experimental.sandbox.http.host import SandboxHost
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

SANDBOX_ID_PREFIX = "sb-"

# 22 hex chars (the gRPC facade's id shape too): long enough that random ids
# never collide in practice, so a fresh create skips get_if_exists and a
# collision could only fail loudly, never attach a second client to someone
# else's sandbox.
_SANDBOX_ID_HEX_CHARS = 22


def _new_random_sandbox_id() -> str:
    return f"{SANDBOX_ID_PREFIX}{uuid.uuid4().hex[:_SANDBOX_ID_HEX_CHARS]}"


def _sandbox_name_for_token(client_token: str) -> str:
    digest = hashlib.sha256(client_token.encode("utf-8")).hexdigest()
    return f"{SANDBOX_ID_PREFIX}{digest[:_SANDBOX_ID_HEX_CHARS]}"


def _is_unschedulable(exc: BaseException) -> bool:
    """True when Ray reports the actor can never fit the cluster's resources.

    Matched by class name for the same reason as ``_is_actor_gone``.
    """
    names = {type(exc).__name__, *(base.__name__ for base in type(exc).__mro__)}
    return any("ActorUnschedulableError" in name for name in names)


def _is_actor_unavailable(exc: BaseException) -> bool:
    """True when a remote call failed because the actor is *transiently*
    unreachable (a restart or network blip), not because it is gone.

    Ray makes ``ActorUnavailableError`` a subclass of ``RayActorError``, so
    this must be checked before ``_is_actor_gone`` — otherwise a recoverable
    blip is misread as a permanent death. Matched by class name for the same
    importability reason as ``_is_actor_gone``.
    """
    names = {type(exc).__name__, *(base.__name__ for base in type(exc).__mro__)}
    return any("ActorUnavailableError" in name for name in names)


def _is_actor_gone(exc: BaseException) -> bool:
    """True when a remote call failed because the actor no longer exists.

    Matched by class name because Ray re-raises remote failures as
    dynamically-built subclasses, and so this module stays importable (and
    unit-testable) without a live Ray context. Transient unreachability
    (``ActorUnavailableError``) is handled separately by
    ``_is_actor_unavailable`` and must be ruled out first.
    """
    names = {type(exc).__name__, *(base.__name__ for base in type(exc).__mro__)}
    return any("RayActorError" in name or "ActorDiedError" in name for name in names)


@PublicAPI(stability="alpha")
class RayActorHandleResolver:
    """Creates and resolves the named detached SandboxHost actors.

    The default (and only production) resolver; tests inject a fake with the
    same methods so the HTTP layer runs without a Ray cluster.

    Every method may wait on a GCS round trip, so the async layers call them
    from worker threads and the class is thread-safe. Handles of actors this
    resolver created under fresh random names are cached: such a name is
    never reused, so a cached handle can only go stale by its actor dying,
    which surfaces as "not found" on the next call. Deterministic names (a
    client token, a named sandbox) always resolve through ``ray.get_actor``,
    whose own cache follows actor deaths and re-creations.
    """

    # Bounds the handle cache, far above the sandboxes one process serves.
    _MAX_CACHED_HANDLES = 65536

    def __init__(self, settings: SandboxAPISettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._host_class: Any = None
        self._handles: "OrderedDict[str, Any]" = OrderedDict()

    def _actor_class(self) -> Any:
        # One ActorClass for the resolver's lifetime: a fresh
        # ray.remote(SandboxHost) per create re-exports the class each time
        # and made creates about 12x slower.
        with self._lock:
            if self._host_class is None:
                import ray

                self._host_class = ray.remote(SandboxHost)
            return self._host_class

    def create(
        self,
        name: str,
        actor_options: Dict[str, Any],
        ctor_kwargs: Dict[str, Any],
        get_if_exists: bool = True,
    ) -> Any:
        """Create the named detached host actor.

        Args:
            name: The sandbox id, used as the actor name.
            actor_options: Extra ``.options()`` (resources).
            ctor_kwargs: ``SandboxHost`` constructor arguments.
            get_if_exists: Atomic get-or-create, for idempotent names: two
                replicas racing on one client token converge on one actor
                (``boot()`` is idempotent). A fresh random name passes False,
                which skips a GCS lookup and makes its handle cacheable.

        Returns:
            The actor handle.
        """
        handle = (
            self._actor_class()
            .options(
                name=name,
                namespace=self._settings.namespace,
                lifetime="detached",
                get_if_exists=get_if_exists,
                **actor_options,
            )
            .remote(**ctor_kwargs)
        )
        if not get_if_exists:
            with self._lock:
                self._handles[name] = handle
                while len(self._handles) > self._MAX_CACHED_HANDLES:
                    self._handles.popitem(last=False)
        return handle

    def get(self, name: str) -> Optional[Any]:
        with self._lock:
            handle = self._handles.get(name)
            if handle is not None:
                self._handles.move_to_end(name)
                return handle
        import ray

        try:
            return ray.get_actor(name, namespace=self._settings.namespace)
        except ValueError:
            return None

    def forget(self, name: str) -> None:
        """Drop the cached handle of a sandbox whose actor is gone."""
        with self._lock:
            self._handles.pop(name, None)

    def list_names(self) -> List[str]:
        from ray.util import list_named_actors

        names: List[str] = []
        for entry in list_named_actors(all_namespaces=True):
            if entry.get("namespace") == self._settings.namespace and entry.get(
                "name", ""
            ).startswith(SANDBOX_ID_PREFIX):
                names.append(entry["name"])
        return names

    def kill(self, handle: Any) -> None:
        import ray

        try:
            ray.kill(handle)
        except Exception as exc:
            logger.debug("Failed to kill sandbox actor: %s", exc)
