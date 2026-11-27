"""Sandbox backend on Agent Substrate, through the ate-env-api service.

``SubstrateSandboxBackend`` runs ``SandboxRuntime`` sandboxes as Agent
Substrate environments instead of gVisor sandboxes on the Ray node. Select it
with ``RAY_SANDBOX_BACKEND=substrate`` or ``SandboxRuntime(backend="substrate")``.

It needs the env-rl (sandboxenv-rl) Python SDK, which Ray does not install::

    pip install ate-env-rl

Copy restores and ``checkpoint(leave_running=True)`` need ``Client.fork``
(the ``ForkEnvironment`` RPC), which no ate-env-rl release has yet: it is in
env-rl from commit 6fd75328, newer than v0.2.0 (``MIN_FORK_SDK_RELEASE`` names
the release once there is one). They also check, once per client, that the
server lists ``fork_environment`` in ``GetServerInfo``. The SDK needs
``grpcio>=1.85.1`` and ``protobuf>=7.35``. The SDK is imported only
when the backend first talks to ate-env-api: importing
``ray.experimental.sandbox`` and the default gVisor backend never import it.

Configuration, from the environment of the process that runs the backend:

* ``RAY_SUBSTRATE_ENDPOINT``: ate-env-api ``host:port``; default
  ``ate-env-api.ate-env.svc:7777``.
* ``RAY_SUBSTRATE_AUTH_TOKEN_FILE``, ``RAY_SUBSTRATE_AUTH_TOKEN`` or
  ``SUBSTRATE_ENV_AUTH_TOKEN``: bearer token; the first one set wins.
* ``RAY_SUBSTRATE_ATESPACE``: atespace of the environments; default ``ate-env``.
* ``RAY_SUBSTRATE_TEMPLATE``: base template when no per-domain one applies.
* ``RAY_SUBSTRATE_TEMPLATE_BY_DOMAIN``: per-domain templates, such as
  ``0-65:c3-template,66-120:n2d-template`` (domains named ``d<k>``).
* ``NODE_NAME``: Kubernetes node of this pod (downward API ``spec.nodeName``).
* ``RAY_SUBSTRATE_DOMAIN_BY_NODE``: node to placement domain map: inline
  ``node-a=d0,node-b=d1``, a JSON object, or ``@/path`` to a file holding
  either. ``RAY_SUBSTRATE_DOMAIN_BY_NODE_FILE`` takes the path directly.
* ``RAY_SUBSTRATE_DOMAIN``: placement domain for every create of this process.
* ``RAY_SUBSTRATE_WORKER_SELECTOR``: worker selector, ``k=v,k2=v2`` or JSON;
  overrides the domain settings.
* ``RAY_SUBSTRATE_RESTORE_MODE``: ``copy`` (default) or ``resume``: what
  ``restore`` does when the call doesn't say (see below).
* ``RAY_SUBSTRATE_SUSPEND_MODE``: ``local`` (default) or ``durable``.
* ``RAY_SUBSTRATE_RPC_TIMEOUT_S``: deadline of a control call (create,
  suspend, delete, status, files) that has no timeout of its own; default 300.
  ``exec`` without ``timeout`` has no deadline, as on gVisor.
* ``RAY_SUBSTRATE_ID_PREFIX``: environment id prefix; default ``rsb``.

For tests, ``RAY_SUBSTRATE_CLIENT_FACTORY`` names a callable, as
``package.module:attr`` or ``/path/to/file.py:attr``, that takes the settings
and returns a client to use instead of ``ate_env_rl.Client``. It is a test
hook, not a supported setting.

Placement: a create carries the worker selector ``{"ate-env/domain": dK}``,
where dK is the domain of ``NODE_NAME`` in the node map, so the sandbox runs
on the node of the Ray worker that asked for it. A map without this node is a
``SandboxCreationError``. With no map and no override there is no selector.
Copies that ``restore`` makes get the restoring worker's selector; they keep
the checkpoint's template, so restore on nodes of the same CPU family.

Options: the Substrate template decides resources, network and isolation.
``create`` raises ``SandboxCreationError`` for options the backend can't
honor: ``mounts``, ``cidr_allowlist``, ``dns``, ``capabilities`` (other than
Docker's defaults, ``DOCKER_DEFAULT_CAPABILITIES``, which the HTTP API passes
when a request names none: those count as unset), ``gpu_ids``, a non-zero
``disk``, ``oom_score_adj``, and a ``network`` other than the default
``"none"``. The default ``network="none"`` means "as the template
says": use a template without egress where sandboxes must have none.
``cpu`` and ``memory`` are logged once and not enforced: callers always pass
them (``Sandbox`` uses them for its Ray reservation), and they size the
sandbox rather than isolate it. ``rootless`` and the private
``_oci_spec_transform_fn`` and ``_ignore_cgroups`` have no Substrate meaning
and are ignored. ``env``, ``workdir`` and ``shell`` are honored. Images must be
pinned by digest (``repo@sha256:...``).

Checkpoints and restores. A checkpoint is a suspended Substrate environment
that belongs to the checkpoint:

* ``checkpoint(leave_running=False)`` suspends the sandbox, which becomes the
  checkpoint. Its id stops naming a usable sandbox in this process: exec on it
  raises ``SandboxNotFoundError``, and ``delete`` of it does nothing, so the
  checkpoint survives. (Another process that knows the id can still use it.)
* ``checkpoint(leave_running=True)`` copies the sandbox (``ForkEnvironment``)
  into a new environment, born suspended, which is the checkpoint. Later
  commands in the sandbox don't change it. The fork suspends the sandbox for
  the length of the call (its background processes pause), and its next call
  wakes it.
* ``restore`` makes a new sandbox, copied from the checkpoint with one
  ``ForkEnvironment`` call, and leaves the checkpoint in place: restore it as
  often as you like. Concurrent copies of one checkpoint serialize: Substrate
  refuses a fork of a source that another fork holds (ABORTED), and the
  backend retries with jittered exponential backoff until the restore's
  ``timeout_seconds`` (plus grace) runs out. For a fan-out of N copies of one
  checkpoint, give ``timeout_seconds`` of about N times the fork latency (the
  top-level ``sandbox.restore()`` defaults to 30 s too). Coalescing them on the
  server is the planned improvement
  (https://github.com/gke-labs/env-rl/issues/69). Deleting a checkpoint
  environment that copies keep busy is retried the same way. A
  fork that fails on the client side (a timeout) may still finish on the
  server, so the backend deletes that copy once it exists.
* ``restore(..., resume_in_place=True)`` (or ``RAY_SUBSTRATE_RESTORE_MODE=resume``)
  hands the checkpoint environment itself back, without any call: Substrate
  wakes it on its next command. That uses the checkpoint up, and another
  restore of it raises ``SandboxError``. It fits a loop that suspends a
  sandbox between steps and resumes it once per step.
* ``SandboxRuntime.delete_checkpoint(checkpoint)`` deletes a checkpoint's
  environment and its files. Nothing else deletes it, except a new checkpoint
  at the same ``checkpoint_path``, which replaces the earlier one there, and
  resume-in-place, which turns it back into a sandbox. Flows that copy should
  delete the checkpoints they no longer need.
* ``checkpoint`` refuses a ``checkpoint_path`` that is a non-empty directory
  without a checkpoint, as on gVisor.

``checkpoint`` returns a ``CheckpointPath``. Without a ``checkpoint_path`` it
is a ``substrate://<atespace>/<env-id>?c=...`` handle that carries the
sandbox's config and restores from any node (``remote=True``). With one, the
backend writes there a ``manifest.json`` (``create_manifest``'s format plus a
``substrate`` section) and an empty ``state`` directory, so that
``ray.experimental.sandbox.restore()`` and ``Sandbox(restore_from=...)``
accept it. ``remote`` then says whether that path is on storage that every
node mounts, as for gVisor: the environment itself is reachable from any node.
``restore`` ignores its own ``remote`` argument. With the default local
suspend mode, a snapshot reaches object storage in the background: until it
does, losing the Substrate node loses the checkpoint.

Coordination through ``checkpoint_path``: writing, resuming in place and
deleting a checkpoint hold an ``flock`` on ``<checkpoint_path>/.substrate.lock``
while they read and change ``manifest.json``, and resume-in-place uses the
checkpoint up by renaming that file. So a resume never mixes a new checkpoint
at the path with the old one, and one checkpoint resumes once. A copy re-reads
the manifest before each fork attempt: it copies the old checkpoint or the new
one (whichever is there at that attempt), stops when the checkpoint was used
up, and is deleted if the checkpoint was resumed in place while it forked. That works for
the processes of one node, or on shared storage with POSIX locks and atomic
rename (NFS, not gcsfuse). Copies only read the manifest and take no lock.
Handles coordinate within one process only.

Commands: a string runs as ``[shell, "-c", command]`` with the ``shell`` of
the call or of create (``/bin/bash`` by default), as on gVisor; a list runs as
is. The command reaches the guest through ``sh -c``, so the image needs
``/bin/sh``. ``user`` is not supported. With ``combine_output`` the order of
stdout and stderr is lost. ``write_file(append=True)`` reads the size and then
writes at it: two calls, not atomic against other writers. ``pause`` suspends
(Substrate has no pause without a snapshot); ``resume`` does nothing, since
the next command wakes the sandbox.
"""

import asyncio
import atexit
import base64
import collections
import concurrent.futures
import errno
import fcntl
import importlib.util
import inspect
import json
import logging
import os
import random
import shlex
import shutil
import threading
import time
import urllib.parse
import uuid
import weakref
from typing import Any, Callable, Coroutine, Dict, List, Mapping, Optional, Tuple, Union

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
    validate_checkpoint_dir,
)
from ray.experimental.sandbox.backend.checkpoint_utils import (
    CheckpointPath,
    create_manifest,
)
from ray.experimental.sandbox.config import (
    DOCKER_DEFAULT_CAPABILITIES,
    SandboxConfig,
    parse_memory_bytes,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxExecError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)

logger = logging.getLogger(__name__)

HANDLE_SCHEME = "substrate://"
MANIFEST = "manifest.json"
CONSUMED_SUFFIX = ".consumed"
LOCK_FILE = ".substrate.lock"
FORK_COALESCING_ISSUE = "https://github.com/gke-labs/env-rl/issues/69"
DOMAIN_LABEL = "ate-env/domain"
DEFAULT_ENDPOINT = "ate-env-api.ate-env.svc:7777"
DEFAULT_ATESPACE = "ate-env"
DEFAULT_SHELL = "/bin/bash"
DEFAULT_RPC_TIMEOUT_S = 300.0
# The first ate-env-rl release with Client.fork (ForkEnvironment), env-rl
# commit 6fd75328. None until env-rl releases it: until then the check is on
# hasattr(Client, "fork") alone.
MIN_FORK_SDK_RELEASE: Optional[str] = None
FORK_SDK_REQUIREMENT = (
    f"ate-env-rl>={MIN_FORK_SDK_RELEASE}"
    if MIN_FORK_SDK_RELEASE
    else "an ate-env-rl built from env-rl commit 6fd75328 or later (newer than v0.2.0)"
)
SERVER_FORK_FEATURE = "fork_environment"
TOKEN_ENVS = ("RAY_SUBSTRATE_AUTH_TOKEN", "SUBSTRATE_ENV_AUTH_TOKEN")
SDK_MISSING = (
    "pip install ate-env-rl (sandboxenv-rl SDK) to use the Substrate sandbox backend"
)
# ate-env-api SuspendMode values.
_SUSPEND_DURABLE = 1
_SUSPEND_LOCAL = 2
# Extra seconds a caller waits past an operation's own timeout, so that the
# server's answer wins a close race.
_GRACE_S = 30.0
# Entries kept in each per-process map; the least recently used go first.
_MAP_CAP = 65536
# Ids checkpointed with leave_running=False are kept far longer, with a
# warning when one is dropped (its delete() would then delete the checkpoint).
_RETIRED_CAP = 1 << 20
# Pauses between fork attempts that Substrate answered with ABORTED.
_FORK_BACKOFF_FIRST_S = 0.05
_FORK_BACKOFF_MAX_S = 2.0
# A fork attempt starts only with at least this long left before the deadline.
_FORK_MIN_ATTEMPT_S = 1.0
# First pause between the deletes of copies that a failed fork may still make.
_EVENTUAL_DELETE_FIRST_S = 1.0

# Options the backend can't honor, with the value that means "not set".
_UNSUPPORTED = {
    "mounts": None,
    "cidr_allowlist": None,
    "dns": None,
    "capabilities": None,
    "gpu_ids": None,
    "oom_score_adj": None,
}
# Options that are logged once and otherwise ignored, with their defaults.
_IGNORED = {"cpu": 0.0, "memory": 0, "rootless": True}
# The config that a checkpoint carries to its restores.
_CARRIED = ("image", "shell", "env", "workdir", "cpu", "memory")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def parse_template_by_domain(spec: Optional[str]) -> List[Tuple[int, int, str]]:
    """Parse ``"0-65:a,66-74:b"`` into ``[(0, 65, "a"), (66, 74, "b")]``."""
    out = []
    for part in (spec or "").split(","):
        if not part.strip():
            continue
        rng, name = part.split(":", 1)
        lo, _, hi = rng.strip().partition("-")
        out.append((int(lo), int(hi or lo), name.strip()))
    return out


def template_for_domain(
    table: List[Tuple[int, int, str]], domain: Optional[str], default: Optional[str]
) -> Optional[str]:
    """Return the template of domain ``d<k>`` in ``table``, else ``default``."""
    if table and domain and domain.startswith("d") and domain[1:].isdigit():
        k = int(domain[1:])
        for lo, hi, name in table:
            if lo <= k <= hi:
                return name
    return default


def _parse_pairs(text: str) -> Dict[str, str]:
    text = text.strip()
    if not text:
        return {}
    if text.startswith("{"):
        return {str(k): str(v) for k, v in json.loads(text).items()}
    out = {}
    for raw in text.replace("\n", ",").split(","):
        item = raw.split("#", 1)[0].strip()
        if not item:
            continue
        key, sep, value = item.partition("=")
        if not sep:
            raise ValueError(f"expected key=value, got {item!r}")
        out[key.strip()] = value.strip()
    return out


def parse_domain_by_node(spec: Optional[str]) -> Dict[str, str]:
    """Parse a node to domain map: inline ``a=d0,b=d1``, JSON, or ``@/file``."""
    if not spec:
        return {}
    spec = spec.strip()
    if spec.startswith("@"):
        with open(spec[1:], encoding="utf-8") as f:
            spec = f.read()
    return _parse_pairs(spec)


def _read_token(e: Mapping[str, str]) -> Optional[str]:
    path = e.get("RAY_SUBSTRATE_AUTH_TOKEN_FILE")
    if path:
        with open(path, encoding="utf-8") as f:
            return f.read().strip() or None
    return next((e[k] for k in TOKEN_ENVS if e.get(k)), None)


class SubstrateSettings:
    """Configuration of the Substrate backend; ``from_env`` reads it."""

    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        auth_token: Optional[str] = None,
        atespace: str = DEFAULT_ATESPACE,
        template: Optional[str] = None,
        template_by_domain: Optional[List[Tuple[int, int, str]]] = None,
        node_name: str = "",
        domain_by_node: Optional[Dict[str, str]] = None,
        domain: Optional[str] = None,
        worker_selector: Optional[Dict[str, str]] = None,
        restore_mode: str = "copy",
        suspend_mode: str = "local",
        rpc_timeout: float = DEFAULT_RPC_TIMEOUT_S,
        id_prefix: str = "rsb",
        client_factory: Optional[str] = None,
    ):
        self.endpoint = endpoint
        self.auth_token = auth_token
        self.atespace = atespace
        self.template = template
        self.template_by_domain = template_by_domain or []
        self.node_name = node_name
        self.domain_by_node = domain_by_node or {}
        self.domain = domain
        self.worker_selector = worker_selector
        self.restore_mode = restore_mode.lower()
        if self.restore_mode not in ("copy", "resume"):
            raise ValueError(
                "RAY_SUBSTRATE_RESTORE_MODE must be copy or resume, "
                f"not {restore_mode!r}"
            )
        self.suspend_mode = suspend_mode.lower()
        if self.suspend_mode not in ("local", "durable"):
            raise ValueError(
                "RAY_SUBSTRATE_SUSPEND_MODE must be local or durable, "
                f"not {suspend_mode!r}"
            )
        self.rpc_timeout = float(rpc_timeout)
        self.id_prefix = id_prefix
        self.client_factory = client_factory

    @classmethod
    def from_env(
        cls, environ: Optional[Mapping[str, str]] = None
    ) -> "SubstrateSettings":
        """Read the settings from ``environ`` (default ``os.environ``)."""
        e = os.environ if environ is None else environ
        dmap = e.get("RAY_SUBSTRATE_DOMAIN_BY_NODE", "")
        if not dmap and e.get("RAY_SUBSTRATE_DOMAIN_BY_NODE_FILE"):
            dmap = "@" + e["RAY_SUBSTRATE_DOMAIN_BY_NODE_FILE"]
        selector = e.get("RAY_SUBSTRATE_WORKER_SELECTOR", "")
        return cls(
            endpoint=e.get("RAY_SUBSTRATE_ENDPOINT") or DEFAULT_ENDPOINT,
            auth_token=_read_token(e),
            atespace=e.get("RAY_SUBSTRATE_ATESPACE") or DEFAULT_ATESPACE,
            template=e.get("RAY_SUBSTRATE_TEMPLATE") or None,
            template_by_domain=parse_template_by_domain(
                e.get("RAY_SUBSTRATE_TEMPLATE_BY_DOMAIN")
            ),
            node_name=e.get("NODE_NAME", ""),
            domain_by_node=parse_domain_by_node(dmap),
            domain=e.get("RAY_SUBSTRATE_DOMAIN") or None,
            worker_selector=_parse_pairs(selector) if selector.strip() else None,
            restore_mode=e.get("RAY_SUBSTRATE_RESTORE_MODE") or "copy",
            suspend_mode=e.get("RAY_SUBSTRATE_SUSPEND_MODE") or "local",
            rpc_timeout=float(
                e.get("RAY_SUBSTRATE_RPC_TIMEOUT_S") or DEFAULT_RPC_TIMEOUT_S
            ),
            id_prefix=e.get("RAY_SUBSTRATE_ID_PREFIX") or "rsb",
            client_factory=e.get("RAY_SUBSTRATE_CLIENT_FACTORY") or None,
        )

    @property
    def places(self) -> bool:
        """Whether any placement setting is set."""
        return bool(self.worker_selector or self.domain or self.domain_by_node)

    def placement(self) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """Return ``(worker_selector, template)`` for a create.

        Raises:
            SandboxCreationError: A node map is set but NODE_NAME is unset or
                not in it.
        """
        if self.worker_selector is not None:
            selector = dict(self.worker_selector)
        elif self.domain:
            selector = {DOMAIN_LABEL: self.domain}
        elif self.domain_by_node:
            if not self.node_name:
                raise SandboxCreationError(
                    "RAY_SUBSTRATE_DOMAIN_BY_NODE is set but NODE_NAME is not; "
                    "set NODE_NAME from the downward API (spec.nodeName)"
                )
            dom = self.domain_by_node.get(self.node_name)
            if dom is None:
                raise SandboxCreationError(
                    f"node {self.node_name!r} is not in "
                    f"RAY_SUBSTRATE_DOMAIN_BY_NODE ({len(self.domain_by_node)} nodes)"
                )
            selector = {DOMAIN_LABEL: dom}
        else:
            selector = None
        dom = (selector or {}).get(DOMAIN_LABEL)
        return selector, template_for_domain(
            self.template_by_domain, dom, self.template
        )


# ---------------------------------------------------------------------------
# Process-wide state: one event loop thread, one client per endpoint
# ---------------------------------------------------------------------------


class _BoundedDict:
    """A thread-safe dict that drops its least recently used entries past ``cap``."""

    def __init__(self, cap: int = _MAP_CAP, name: Optional[str] = None):
        self._data: "collections.OrderedDict[Any, Any]" = collections.OrderedDict()
        self._lock = threading.Lock()
        self.cap = cap
        self.name = name

    def get(self, key: Any, default: Any = None) -> Any:
        with self._lock:
            if key not in self._data:
                return default
            self._data.move_to_end(key)
            return self._data[key]

    def __contains__(self, key: Any) -> bool:
        with self._lock:
            return key in self._data

    def __setitem__(self, key: Any, value: Any) -> None:
        dropped = []
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            while len(self._data) > self.cap:
                dropped.append(self._data.popitem(last=False)[0])
        if dropped and self.name:
            logger.warning(
                "Substrate backend: forgot %d %s, such as %s",
                len(dropped),
                self.name,
                dropped[0],
            )

    def pop(self, key: Any, default: Any = None) -> Any:
        with self._lock:
            return self._data.pop(key, default)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


def _init_process_state() -> None:
    """Set (or, in a forked child, reset) every piece of per-process state."""
    global _lock, _loop, _loop_thread, _clients, _client_locks, _warned
    global _meta, _retired, _used_handles, _handle_locks, _fork_checked
    _lock = threading.Lock()
    _loop = None
    _loop_thread = None
    _clients = {}
    _client_locks = {}
    _warned = set()
    # Config of the sandboxes this process knows, for execs and checkpoints.
    _meta = _BoundedDict()
    # Ids checkpointed with leave_running=False: their checkpoint owns them.
    _retired = _BoundedDict(
        _RETIRED_CAP, name="checkpointed sandboxes (delete() of one now deletes it)"
    )
    # substrate:// handles used up in this process: "resumed" or "deleted".
    _used_handles = _BoundedDict()
    # Clients whose SDK and server were checked for ForkEnvironment.
    _fork_checked = weakref.WeakSet()
    # Locks of the substrate:// handles of this process, while in use.
    _handle_locks = weakref.WeakValueDictionary()


_init_process_state()
if hasattr(os, "register_at_fork"):
    # The loop thread and the grpc channel don't survive os.fork(): a child
    # starts over with its own.
    os.register_at_fork(after_in_child=_init_process_state)


def _warn_once(key: str, msg: str, *args: Any) -> None:
    with _lock:
        if key in _warned:
            return
        _warned.add(key)
    logger.warning(msg, *args)


def _get_loop() -> asyncio.AbstractEventLoop:
    """Return this process's background event loop, starting it once."""
    global _loop, _loop_thread
    with _lock:
        if _loop is None:
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=loop.run_forever, name="ray-substrate-backend", daemon=True
            )
            thread.start()
            _loop, _loop_thread = loop, thread
        return _loop


def _submit(coro: Coroutine) -> concurrent.futures.Future:
    return asyncio.run_coroutine_threadsafe(coro, _get_loop())


_factory_modules: Dict[str, Any] = {}


def _load_factory(spec: str) -> Any:
    """Load ``package.module:attr`` or ``/path/to/file.py:attr``."""
    target, _, attr = spec.rpartition(":")
    if target.endswith(".py") or os.sep in target:
        with _lock:
            module = _factory_modules.get(target)
        if module is None:
            name = (
                "_substrate_client_factory_"
                + uuid.uuid5(uuid.NAMESPACE_URL, target).hex
            )
            module_spec = importlib.util.spec_from_file_location(name, target)
            module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(module)
            with _lock:
                module = _factory_modules.setdefault(target, module)
        return getattr(module, attr)
    from ray.experimental.sandbox.backend.registry import load_import_path

    return load_import_path(spec)


async def _make_client(settings: SubstrateSettings) -> Any:
    if settings.client_factory:
        return _load_factory(settings.client_factory)(settings)
    try:
        import ate_env_rl
    except ImportError as err:
        raise ImportError(SDK_MISSING) from err
    # Built on the loop thread: a grpc.aio channel belongs to the loop that
    # creates it. One channel (one HTTP/2 connection) per process.
    return ate_env_rl.Client(settings.endpoint, auth_token=settings.auth_token)


def _shared_client(settings: SubstrateSettings) -> Any:
    key = (settings.client_factory, settings.endpoint, settings.auth_token)
    with _lock:
        client = _clients.get(key)
        if client is not None:
            return client
        key_lock = _client_locks.setdefault(key, threading.Lock())
    # One creator per key, so that racing threads don't build extra channels.
    with key_lock:
        with _lock:
            client = _clients.get(key)
        if client is None:
            client = _submit(_make_client(settings)).result(settings.rpc_timeout)
            with _lock:
                _clients[key] = client
    return client


def shutdown(timeout: float = 5.0) -> None:
    """Close this process's clients and stop its event loop (runs at exit)."""
    global _loop, _loop_thread
    with _lock:
        loop, thread, clients = _loop, _loop_thread, list(_clients.values())
        _clients.clear()
        _loop, _loop_thread = None, None
    if loop is None:
        return

    async def _close():
        for client in clients:
            close = getattr(client, "close", None)
            if close is not None:
                try:
                    await close()
                except Exception:
                    pass

    try:
        asyncio.run_coroutine_threadsafe(_close(), loop).result(timeout)
    except Exception:
        pass
    loop.call_soon_threadsafe(loop.stop)
    if thread is not None:
        thread.join(timeout)


atexit.register(shutdown)


def _sdk_version() -> str:
    try:
        from importlib.metadata import version

        return version("ate-env-rl")
    except Exception:
        return "(unknown version)"


# ---------------------------------------------------------------------------
# Errors and statuses, matched by name so that a fake client works too
# ---------------------------------------------------------------------------


def _code_name(err: BaseException) -> Optional[str]:
    code = getattr(err, "code", None)
    if callable(code):  # a raw grpc.RpcError
        try:
            code = code()
        except Exception:
            return None
    return getattr(code, "name", None)


def _is_not_found(err: BaseException) -> bool:
    return type(err).__name__ == "NotFoundError" or _code_name(err) == "NOT_FOUND"


def _is_rejected(err: BaseException) -> bool:
    """A definite refusal by the server: a fork that made no copy."""
    return _code_name(err) in (
        "INVALID_ARGUMENT",
        "FAILED_PRECONDITION",
        "PERMISSION_DENIED",
        "ALREADY_EXISTS",
    ) or type(err).__name__ in (
        "InvalidArgumentError",
        "FailedPreconditionError",
        "PermissionDeniedError",
    )


def _is_aborted(err: BaseException) -> bool:
    if _code_name(err) == "ABORTED":
        return True
    # Fallback for an error without a status code: ate-env-api relays
    # Substrate's "another operation is in progress" for a busy source.
    return "another operation is in progress" in str(err)


def _map_error(err: BaseException, default: type, what: str) -> SandboxError:
    if isinstance(err, SandboxError):
        return err
    if _is_not_found(err):
        return SandboxNotFoundError(f"{what}: {err}")
    if isinstance(err, (TimeoutError, concurrent.futures.TimeoutError)):
        return SandboxTimeoutError(f"{what}: timed out {err}")
    return default(f"{what}: {type(err).__name__}: {err}")


def _sandbox_status(status: Any) -> SandboxStatus:
    """Map an ate-env-api EnvironmentStatus (enum or int) to SandboxStatus."""
    names = {
        1: "RESUMING",
        2: "RUNNING",
        3: "SUSPENDING",
        4: "SUSPENDED",
        5: "PAUSING",
        6: "PAUSED",
        7: "CRASHED",
        8: "DELETING",
    }
    name = getattr(status, "name", None) or names.get(int(status), "")
    return {
        "RUNNING": SandboxStatus.RUNNING,
        "RESUMING": SandboxStatus.PENDING,
        "SUSPENDING": SandboxStatus.PAUSED,
        "SUSPENDED": SandboxStatus.PAUSED,
        "PAUSING": SandboxStatus.PAUSED,
        "PAUSED": SandboxStatus.PAUSED,
        "CRASHED": SandboxStatus.ERROR,
        "DELETING": SandboxStatus.TERMINATED,
    }.get(name, SandboxStatus.ERROR)


# ---------------------------------------------------------------------------
# Checkpoint handles and files
# ---------------------------------------------------------------------------


def _carried(config: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: config[k] for k in _CARRIED if config.get(k) not in (None, {}, "")}


def _make_handle(atespace: str, env_id: str, config: Mapping[str, Any]) -> str:
    handle = f"{HANDLE_SCHEME}{atespace}/{env_id}"
    carried = _carried(config)
    if carried:
        blob = base64.urlsafe_b64encode(json.dumps(carried).encode()).decode()
        handle += "?c=" + blob.rstrip("=")
    return handle


def _parse_handle(handle: str) -> Dict[str, Any]:
    rest, _, query = handle[len(HANDLE_SCHEME) :].partition("?")
    atespace, _, env_id = rest.partition("/")
    if not atespace or not env_id:
        raise SandboxError(f"Malformed Substrate checkpoint handle {handle!r}")
    config: Dict[str, Any] = {}
    blob = urllib.parse.parse_qs(query).get("c", [""])[0]
    if blob:
        blob += "=" * (-len(blob) % 4)
        config = json.loads(base64.urlsafe_b64decode(blob.encode()))
    return {"env_id": env_id, "atespace": atespace, "config": config}


def _write_json(path: str, data: Any) -> None:
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    for attempt in range(2):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except OSError as err:
            # On NFS, a read that races another client's os.replace can get
            # ESTALE once: the new file is there on a second look.
            if err.errno != errno.ESTALE or attempt:
                raise
    return None


class _FileLock:
    """Exclusive flock with a deadline: across processes and threads."""

    def __init__(self, path: str, timeout: float):
        self.path = path
        self.timeout = timeout
        self._fd: Optional[int] = None

    def __enter__(self) -> "_FileLock":
        self._fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        deadline = time.monotonic() + self.timeout
        delay = 0.005
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    os.close(self._fd)
                    raise SandboxTimeoutError(
                        f"Timed out after {self.timeout}s waiting for {self.path}"
                    ) from None
                time.sleep(delay)
                delay = min(delay * 2, 0.1)

    def __exit__(self, *exc: Any) -> None:
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)


class _HandleLock:
    """A threading lock that a WeakValueDictionary can hold."""

    def __init__(self):
        self._lock = threading.Lock()

    def __enter__(self) -> "_HandleLock":
        self._lock.acquire()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._lock.release()


def _unlink(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _new_env_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:20]}"


def _command_line(command: Union[str, List[str]], shell: str) -> str:
    """Return the ``sh -c`` line that runs a command as gVisor would."""
    if isinstance(command, str):
        return f"{shlex.quote(shell)} -c {shlex.quote(command)}"
    argv = [str(a) for a in command]
    if len(argv) == 3 and argv[0] in ("/bin/sh", "sh") and argv[1] == "-c":
        return argv[2]
    return shlex.join(argv)


def _nonzero_size(value: Any) -> bool:
    try:
        return bool(parse_memory_bytes(value))
    except (TypeError, ValueError):
        return bool(value)


def _used_up(checkpoint: str, how: str) -> SandboxError:
    if how == "deleted":
        return SandboxError(f"Substrate checkpoint {checkpoint!r} was deleted")
    return SandboxError(
        f"Substrate checkpoint {checkpoint!r} was already resumed in place "
        "(single use)"
    )


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class _NoopImageManager:
    """Substrate pulls images itself; nothing is cached on the Ray node."""

    images_dir = ""

    def pull_image(self, image: str, timeout_seconds: float = 120.0, **kwargs) -> str:
        return image

    def release_image(self, *args, **kwargs) -> None:
        return None


class SubstrateSandboxBackend(BaseSandboxBackend):
    """Sandbox backend on Agent Substrate; see the module docstring.

    Thread-safe. All instances in a process share one event loop thread and
    one client per endpoint and token, so one instance per thread is cheap.

    Args:
        image_manager: Unused; Substrate pulls images itself.
        settings: Configuration; None reads the environment.
        client: A client object to use instead of the shared one.
    """

    needs_image_manager = False
    checkpoint_format = "substrate"

    def __init__(
        self,
        image_manager: Optional[Any] = None,
        *,
        settings: Optional[SubstrateSettings] = None,
        client: Optional[Any] = None,
    ):
        super().__init__(image_manager=image_manager or _NoopImageManager())
        self.settings = settings or SubstrateSettings.from_env()
        self._client = client
        if client is None and not self.settings.client_factory:
            if importlib.util.find_spec("ate_env_rl") is None:
                raise ImportError(SDK_MISSING)

    # --- plumbing ---

    @property
    def client(self) -> Any:
        """The client this backend talks to ate-env-api with.

        The process-wide one is looked up on each use rather than kept, so
        that a backend built before ``os.fork()`` uses the child's client.
        """
        if self._client is not None:
            return self._client
        return _shared_client(self.settings)

    def _require_fork(self) -> None:
        """Check, once per client, that the SDK and the server can fork."""
        client = self.client
        with _lock:
            try:
                if client in _fork_checked:
                    return
            except TypeError:  # not weakly referenceable: check every time
                pass
        if not hasattr(client, "fork"):
            raise SandboxError(
                f"ate-env-rl {_sdk_version()} has no Client.fork "
                "(ForkEnvironment), which copy restores and "
                f"checkpoint(leave_running=True) need: install {FORK_SDK_REQUIREMENT}, "
                "or restore with resume_in_place=True"
            )
        try:
            info = self._run(client.server_info())
        except Exception as err:
            raise SandboxError(
                "Could not check that ate-env-api serves ForkEnvironment "
                f"(GetServerInfo: {type(err).__name__}: {err})"
            ) from err
        if SERVER_FORK_FEATURE not in tuple(getattr(info, "features", ()) or ()):
            raise SandboxError(
                f"ate-env-api at {self.settings.endpoint} does not serve "
                f"ForkEnvironment (its features lack {SERVER_FORK_FEATURE!r}), which "
                "copy restores and checkpoint(leave_running=True) need: upgrade "
                "ate-env-api, or restore with resume_in_place=True"
            )
        with _lock:
            try:
                _fork_checked.add(client)
            except TypeError:
                pass

    def _require_suspend_mode(self) -> None:
        try:
            params = inspect.signature(self.client.suspend).parameters
        except (TypeError, ValueError):
            return
        if "mode" not in params and not any(
            p.kind == p.VAR_KEYWORD for p in params.values()
        ):
            raise SandboxError(
                f"ate-env-rl {_sdk_version()} has no Client.suspend(mode=...): "
                "install a newer ate-env-rl"
            )

    def _run(
        self, coro: Coroutine, timeout: Optional[float] = None, bounded: bool = True
    ) -> Any:
        """Run ``coro`` on the loop and wait at most ``timeout`` seconds.

        ``timeout`` None means ``RAY_SUBSTRATE_RPC_TIMEOUT_S``; with
        ``bounded=False`` it means no deadline.
        """
        fut = _submit(coro)
        if timeout is None and bounded:
            timeout = self.settings.rpc_timeout
        try:
            return fut.result(timeout)
        except concurrent.futures.TimeoutError:
            fut.cancel()
            raise

    def _deadline(self, timeout: Optional[float]) -> float:
        if timeout is None or timeout <= 0:
            return self.settings.rpc_timeout
        return timeout + _GRACE_S

    def _env(self, sandbox_id: str) -> Any:
        return self.client.env(sandbox_id, atespace=self.settings.atespace)

    def _delete_in_background(self, env_id: str, atespace: Optional[str] = None):
        async def _delete():
            try:
                await self.client.delete(
                    env_id, atespace=atespace or self.settings.atespace
                )
            except Exception:
                pass

        return _submit(_delete())

    async def _delete_until_gone(
        self, env_id: str, atespace: str, budget: float
    ) -> Optional[BaseException]:
        """Delete ``env_id``, retrying ABORTED (a busy source) for ``budget`` s.

        A checkpoint environment can be busy with the forks of copies. Returns
        None once it is gone, else the last error.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget
        delay = _FORK_BACKOFF_FIRST_S
        while True:
            try:
                await self.client.delete(env_id, atespace=atespace)
                return None
            except Exception as err:
                if _is_not_found(err):
                    return None
                pause = delay * random.uniform(0.5, 1.5)
                if not _is_aborted(err) or loop.time() + pause >= deadline:
                    return err
                await asyncio.sleep(pause)
                delay = min(delay * 2, _FORK_BACKOFF_MAX_S)

    def _release_in_background(self, env_id: str, atespace: Optional[str]):
        """Delete a checkpoint environment that no checkpoint names any more."""
        atespace = atespace or self.settings.atespace

        async def _release():
            if await self._delete_until_gone(
                env_id, atespace, self.settings.rpc_timeout
            ):
                self._delete_eventually([env_id], atespace)

        return _submit(_release())

    def _delete_eventually(self, env_ids: List[str], atespace: str):
        """Delete copies that a failed fork call may still make.

        A fork that timed out on the client can still finish on the server,
        so a single delete can come too early. Retry, with growing pauses, for
        ``RAY_SUBSTRATE_RPC_TIMEOUT_S`` (NotFound until then is expected).
        """
        horizon = self.settings.rpc_timeout

        async def _delete():
            pending, waited, pause = set(env_ids), 0.0, _EVENTUAL_DELETE_FIRST_S
            while pending:
                for env_id in list(pending):
                    try:
                        await self.client.delete(env_id, atespace=atespace)
                        pending.discard(env_id)
                    except Exception:
                        pass
                if waited >= horizon:
                    return
                await asyncio.sleep(pause)
                waited += pause
                pause = min(pause * 2, horizon)

        return _submit(_delete())

    def _check_live(self, sandbox_id: str) -> None:
        with _lock:
            retired = sandbox_id in _retired
        if retired:
            raise SandboxNotFoundError(
                f"Sandbox {sandbox_id} was checkpointed with leave_running=False "
                "and belongs to its checkpoint now; restore the checkpoint"
            )

    # --- lifecycle ---

    def _validate_config(self, config: SandboxConfig) -> None:
        # Any set value counts, capabilities=[] ("none") too; no mounts is {}.
        # Docker's default capabilities, which the HTTP API passes when a
        # request names none, count as unset: the template decides.
        bad = [
            name
            for name, unset in _UNSUPPORTED.items()
            if getattr(config, name, unset) not in (unset, {})
            and not (
                name == "capabilities"
                and set(config.capabilities) == set(DOCKER_DEFAULT_CAPABILITIES)
            )
        ]
        if (getattr(config, "network", "none") or "none") != "none":
            bad.append(f"network={config.network!r}")
        if _nonzero_size(getattr(config, "disk", 0)):
            bad.append("disk")
        if bad:
            raise SandboxCreationError(
                f"The Substrate sandbox backend can't honor {', '.join(bad)}: "
                "the Substrate template decides network, isolation and resources"
            )
        ignored = [
            name
            for name, default in _IGNORED.items()
            if getattr(config, name, default) not in (default, None)
        ]
        if ignored:
            _warn_once(
                "ignored:" + ",".join(ignored),
                "SubstrateSandboxBackend does not enforce %s: the Substrate "
                "template decides resources",
                ignored,
            )

    def check_resources(self, cpu: float = 0.0, memory: Union[str, int, float] = 0):
        """Nothing to check on this node: the Substrate template sizes sandboxes."""
        return None

    def create_sandbox(self, config: SandboxConfig) -> str:
        self._validate_config(config)
        selector, template = self.settings.placement()
        if "@sha256:" not in config.image:
            _warn_once(
                "unpinned",
                "image %r is not pinned by digest; ate-env-api needs repo@sha256:...",
                config.image,
            )
        env_id = _new_env_id(self.settings.id_prefix)
        timeout = config.timeout_seconds
        try:
            env = self._run(
                self.client.create(
                    env_id,
                    atespace=self.settings.atespace,
                    template_name=template,
                    rootfs_image=config.image,
                    worker_selector=selector,
                ),
                timeout=timeout if timeout and timeout > 0 else None,
            )
        except concurrent.futures.TimeoutError as err:
            # The server may still finish the create; don't leak it.
            self._delete_in_background(env_id)
            raise SandboxCreationError(
                f"Substrate create of {env_id} timed out after {timeout}s"
            ) from err
        except Exception as err:
            raise _map_error(
                err, SandboxCreationError, f"Substrate create {env_id} ({config.image})"
            ) from err
        with _lock:
            _meta[env.id] = {
                "image": config.image,
                "shell": config.shell or DEFAULT_SHELL,
                "env": dict(config.env or {}),
                "workdir": config.workdir,
                "cpu": config.cpu,
                "memory": config.memory,
            }
        return env.id

    def delete_sandbox(self, sandbox_id: str) -> None:
        with _lock:
            if sandbox_id in _retired:
                return  # its checkpoint owns it
            _meta.pop(sandbox_id, None)
        try:
            self._run(self.client.delete(sandbox_id, atespace=self.settings.atespace))
        except Exception as err:
            if _is_not_found(err):
                return
            raise _map_error(
                err, SandboxError, f"Substrate delete {sandbox_id}"
            ) from err

    def get_status(self, sandbox_id: str) -> SandboxStatus:
        try:
            info = self._run(
                self.client.get(sandbox_id, atespace=self.settings.atespace)
            )
        except Exception as err:
            if _is_not_found(err):
                return SandboxStatus.TERMINATED
            raise _map_error(err, SandboxError, f"Substrate get {sandbox_id}") from err
        return _sandbox_status(info.status)

    def get_termination_reason(self, sandbox_id: str) -> Optional[str]:
        if self.get_status(sandbox_id) == SandboxStatus.ERROR:
            return "Substrate environment crashed"
        return None

    # --- commands and files ---

    def exec_command(
        self,
        sandbox_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
        combine_output: bool = False,
    ) -> ExecResult:
        if user is not None:
            raise SandboxExecError(
                "The Substrate sandbox backend does not support exec(user=...)"
            )
        self._check_live(sandbox_id)
        with _lock:
            meta = _meta.get(sandbox_id) or {}
        line = _command_line(command, shell or meta.get("shell") or DEFAULT_SHELL)
        env = {**(meta.get("env") or {}), **(env or {})}
        cwd = cwd or meta.get("workdir") or ""
        t0 = time.monotonic()
        try:
            res = self._run(
                self._env(sandbox_id).shell(
                    line, cwd=cwd, env=env or None, timeout=timeout
                ),
                # Without a timeout, exec waits as long as the command runs,
                # as on gVisor.
                timeout=timeout + _GRACE_S if timeout and timeout > 0 else None,
                bounded=False,
            )
        except Exception as err:
            raise _map_error(
                err, SandboxExecError, f"Substrate exec in {sandbox_id}"
            ) from err
        duration = time.monotonic() - t0
        timed_out = res.timed_out
        if timed_out is None:
            timed_out = bool(timeout) and res.exit_code == 137 and duration >= timeout
        if timed_out:
            raise SandboxTimeoutError(
                f"Command timed out after {timeout}s in {sandbox_id}"
            )
        if combine_output:
            output = (res.stdout + res.stderr).encode()
            return ExecResult(res.exit_code, "", "", duration, output=output)
        return ExecResult(res.exit_code, res.stdout, res.stderr, duration)

    def write_file(
        self,
        sandbox_id: str,
        path: str,
        content: Union[str, bytes],
        append: bool = False,
    ) -> None:
        self._check_live(sandbox_id)
        data = content.encode() if isinstance(content, str) else bytes(content)
        offset = 0
        if append:
            size = self.exec_command(
                sandbox_id,
                [
                    "/bin/sh",
                    "-c",
                    f"stat -c %s {shlex.quote(path)} 2>/dev/null || echo 0",
                ],
            )
            offset = int((size.stdout or "0").strip() or "0")
        try:
            self._run(self._env(sandbox_id).write_file(path, data, seek_offset=offset))
        except Exception as err:
            raise _map_error(
                err, SandboxError, f"Substrate write {path} in {sandbox_id}"
            ) from err

    def read_file(self, sandbox_id: str, path: str) -> bytes:
        self._check_live(sandbox_id)
        try:
            return self._run(self._env(sandbox_id).read_file_bytes(path))
        except Exception as err:
            raise _map_error(
                err, SandboxError, f"Substrate read {path} in {sandbox_id}"
            ) from err

    # --- checkpoint ---

    def _suspend(self, sandbox_id: str, timeout: Optional[float]) -> None:
        self._require_suspend_mode()
        mode = (
            _SUSPEND_DURABLE
            if self.settings.suspend_mode == "durable"
            else _SUSPEND_LOCAL
        )
        try:
            self._run(
                self.client.suspend(
                    sandbox_id, atespace=self.settings.atespace, mode=mode
                ),
                timeout=self._deadline(timeout),
            )
        except Exception as err:
            raise _map_error(
                err, SandboxError, f"Substrate suspend {sandbox_id}"
            ) from err

    def _fork(
        self,
        env_id: str,
        atespace: str,
        timeout: Optional[float],
        worker_selector: Optional[Dict[str, str]] = None,
        source: Optional[Callable[[], Tuple[str, str]]] = None,
    ) -> str:
        """Copy ``env_id`` once (ForkEnvironment, count 1); returns the copy.

        Substrate refuses a fork of a source that another operation holds,
        such as a concurrent fork of the same checkpoint, with ABORTED. Retry
        that with jittered exponential backoff until the call's deadline
        (``timeout`` plus grace, or ``RAY_SUBSTRATE_RPC_TIMEOUT_S``), so
        concurrent copies of one checkpoint serialize. ``source``, if given,
        is called before each attempt and returns the ``(env_id, atespace)``
        to fork now (it raises if there is nothing left to fork).
        """
        self._require_fork()
        copy_id = _new_env_id(self.settings.id_prefix)
        budget = self._deadline(timeout)
        deadline = time.monotonic() + budget
        delay = _FORK_BACKOFF_FIRST_S
        while True:
            if source is not None:
                env_id, atespace = source()
            kwargs: Dict[str, Any] = {"atespace": atespace, "ids": [copy_id]}
            if worker_selector:
                kwargs["worker_selector"] = worker_selector
            try:
                (env,) = self._run(
                    self.client.fork(env_id, 1, **kwargs),
                    timeout=max(deadline - time.monotonic(), _FORK_MIN_ATTEMPT_S),
                )
            except Exception as err:
                aborted = _is_aborted(err)
                pause = delay * random.uniform(0.5, 1.5)
                # Another attempt only with time left for it to finish.
                if (
                    aborted
                    and time.monotonic() + pause + _FORK_MIN_ATTEMPT_S < deadline
                ):
                    time.sleep(pause)
                    delay = min(delay * 2, _FORK_BACKOFF_MAX_S)
                    continue
                if source is not None and _is_not_found(err):
                    # Replaced meanwhile: copy the new one, while time is left.
                    if source()[0] != env_id and time.monotonic() < deadline:
                        continue
                if not (aborted or _is_not_found(err) or _is_rejected(err)):
                    # A timeout or a lost answer: the server may still make
                    # the copy, so delete it once it exists.
                    self._delete_eventually([copy_id], atespace)
                if aborted:
                    raise SandboxError(
                        f"Substrate fork of {env_id}: the source stayed busy for "
                        f"{budget:.0f}s (concurrent copies of one checkpoint "
                        f"serialize on Substrate; see {FORK_COALESCING_ISSUE}): "
                        f"{err}"
                    ) from err
                raise _map_error(
                    err, SandboxError, f"Substrate fork of {env_id}"
                ) from err
            if env.id != copy_id:
                self._delete_eventually([env.id, copy_id], atespace)
                raise SandboxError(f"ForkEnvironment returned {env.id}, not {copy_id}")
            return env.id

    def checkpoint_sandbox(
        self,
        sandbox_id: str,
        checkpoint_path: Optional[str] = None,
        leave_running: bool = True,
        compression: str = "none",
        exclude_committed_zero_pages: bool = True,
        direct: bool = False,
        timeout_seconds: float = 30.0,
        remote: bool = False,
        **kwargs,
    ) -> Dict[str, Any]:
        """Checkpoint a sandbox; see the module docstring for the semantics.

        Returns:
            ``{"checkpoint_path": CheckpointPath, "env_id": <checkpoint env>}``.
        """
        self._check_live(sandbox_id)
        if checkpoint_path:
            self._check_checkpoint_dir(checkpoint_path)
        atespace = self.settings.atespace
        config = dict(_meta.get(sandbox_id) or {})
        if leave_running:
            # The checkpoint is a copy, born suspended. The fork suspends the
            # sandbox too, for the length of the call: its next call wakes it.
            env_id = self._fork(sandbox_id, atespace, timeout_seconds)
        else:
            self._suspend(sandbox_id, timeout_seconds)
            env_id = sandbox_id
            _retired[sandbox_id] = True
        record = {
            "env_id": env_id,
            "atespace": atespace,
            "source": sandbox_id,
            "config": _carried(config),
            # Tells this checkpoint from a later one of the same environment
            # at the same path (resumed in place, then checkpointed again).
            "generation": uuid.uuid4().hex,
        }
        if not checkpoint_path:
            path = CheckpointPath(
                _make_handle(atespace, env_id, config),
                remote=True,
                cpu=config.get("cpu"),
                memory=config.get("memory"),
            )
        else:
            node_id = self._write_checkpoint(
                checkpoint_path, record, config, leave_running, remote
            )
            path = CheckpointPath(
                checkpoint_path,
                node_id=node_id,
                remote=remote,
                cpu=config.get("cpu"),
                memory=config.get("memory"),
            )
        return {"checkpoint_path": path, "env_id": env_id}

    @staticmethod
    def _check_checkpoint_dir(checkpoint_path: str) -> None:
        """Refuse a non-empty directory that holds no checkpoint, as gVisor does."""
        try:
            names = set(os.listdir(checkpoint_path))
        except FileNotFoundError:
            return
        except NotADirectoryError:
            raise SandboxError(f"{checkpoint_path!r} is not a directory") from None
        # A lone lock file is what a restore in flight leaves after
        # delete_checkpoint.
        if names - {LOCK_FILE} and not names & {MANIFEST, MANIFEST + CONSUMED_SUFFIX}:
            raise SandboxError(
                f"Refusing to checkpoint into {checkpoint_path!r}: it is not empty "
                "and holds no checkpoint"
            )

    def _checkpoint_lock(self, checkpoint: str):
        """The lock of a checkpoint path, for changes to its manifest.

        Writing a checkpoint, resuming one in place and deleting one change
        ``manifest.json`` after reading it, so they hold an ``flock`` on
        ``<path>/.substrate.lock``. A copy only reads the manifest, which
        ``os.replace`` swaps whole, so it takes no lock (see ``_copy``).
        Handles change nothing on disk: their lock is in-process.
        """
        if checkpoint.startswith(HANDLE_SCHEME):
            with _lock:
                lock = _handle_locks.get(checkpoint)
                if lock is None:
                    lock = _handle_locks[checkpoint] = _HandleLock()
            return lock
        return _FileLock(os.path.join(checkpoint, LOCK_FILE), self.settings.rpc_timeout)

    def _write_checkpoint(
        self,
        checkpoint_path: str,
        record: Dict[str, Any],
        config: Dict[str, Any],
        leave_running: bool,
        remote: bool,
    ) -> Optional[str]:
        os.makedirs(os.path.join(checkpoint_path, "state"), exist_ok=True)
        manifest_file = os.path.join(checkpoint_path, MANIFEST)
        manifest = create_manifest(
            sandbox_id=record["source"],
            config=SandboxConfig(
                image=config.get("image") or "unknown",
                cpu=config.get("cpu") or 0.0,
                memory=config.get("memory") or 0,
                env=dict(config.get("env") or {}),
                workdir=config.get("workdir"),
                shell=config.get("shell") or DEFAULT_SHELL,
            ),
            cwd=config.get("workdir") or "/",
            leave_running=leave_running,
            remote=remote,
        )
        manifest["backend"] = "substrate"
        manifest["substrate"] = record
        with self._checkpoint_lock(checkpoint_path):
            old = _read_json(manifest_file)
            _write_json(manifest_file, manifest)
            _unlink(manifest_file + CONSUMED_SUFFIX)
        # A new checkpoint at a path replaces the one there, as on gVisor.
        old_record = (old or {}).get("substrate") or {}
        if old_record.get("env_id") and old_record["env_id"] != record["env_id"]:
            _retired.pop(old_record["env_id"], None)
            self._release_in_background(
                old_record["env_id"], old_record.get("atespace")
            )
        return manifest.get("node_id")

    # --- restore ---

    def _load(self, checkpoint: str) -> Dict[str, Any]:
        """Return the record of a checkpoint (handle or path)."""
        if checkpoint.startswith(HANDLE_SCHEME):
            record = _parse_handle(checkpoint)
            used = _used_handles.get(checkpoint)
            if used is not None:
                raise _used_up(checkpoint, used)
            return record
        manifest_file = os.path.join(checkpoint, MANIFEST)
        manifest = _read_json(manifest_file)
        if manifest is None:
            if os.path.exists(manifest_file + CONSUMED_SUFFIX):
                raise _used_up(checkpoint, "resumed")
            raise SandboxError(f"No checkpoint at {checkpoint!r}")
        if manifest.get("backend") != "substrate" or not manifest.get("substrate"):
            raise SandboxError(f"{checkpoint!r} is not a Substrate checkpoint")
        return manifest["substrate"]

    def _use_up(self, checkpoint: str, how: str) -> Dict[str, Any]:
        """Mark a checkpoint used (``resumed`` or ``deleted``), once.

        Returns the record it used up.
        """
        self._load(checkpoint)  # there, before taking its lock
        with self._checkpoint_lock(checkpoint):
            record = self._load(checkpoint)
            if checkpoint.startswith(HANDLE_SCHEME):
                _used_handles[checkpoint] = how
            else:
                manifest_file = os.path.join(checkpoint, MANIFEST)
                os.rename(manifest_file, manifest_file + CONSUMED_SUFFIX)
        return record

    def _copy(self, checkpoint: str, timeout: float) -> Tuple[str, Dict[str, Any]]:
        """Return a new copy of a checkpoint, and the record it copied.

        The manifest is read without a lock (it is replaced whole), before
        each fork attempt, so a copy waiting behind other forks follows a new
        checkpoint at the path and stops when the checkpoint is used up. After
        the fork it is read once more: if the checkpoint was resumed in place
        meanwhile, the copy may hold the resumed sandbox's later state, so it
        is deleted and the restore fails.
        """
        selector = self.settings.placement()[0] if self.settings.places else None
        current: Dict[str, Any] = {}

        def source() -> Tuple[str, str]:
            current["record"] = self._load(checkpoint)
            record = current["record"]
            return record["env_id"], record.get("atespace") or self.settings.atespace

        copy = self._fork("", "", timeout, selector, source=source)
        record = current["record"]
        atespace = record.get("atespace") or self.settings.atespace
        try:
            after = self._load(checkpoint)
        except SandboxError as err:
            self._delete_eventually([copy], atespace)
            raise SandboxError(
                f"Substrate checkpoint {checkpoint!r} was used up while it was "
                f"being copied; deleted the copy: {err}"
            ) from err
        if after["env_id"] == record["env_id"] and after.get(
            "generation"
        ) != record.get("generation"):
            # The same environment was resumed and checkpointed again during
            # the fork, which may have caught it mid-step.
            self._delete_eventually([copy], atespace)
            raise SandboxError(
                f"Substrate checkpoint {checkpoint!r} was resumed and checkpointed "
                "again while it was being copied; deleted the copy"
            )
        return copy, record

    def restore_sandbox(
        self,
        checkpoint_path: str,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        background: Optional[bool] = None,
        direct: bool = False,
        timeout_seconds: float = 30.0,
        remote: Optional[bool] = None,
        resume_in_place: Optional[bool] = None,
        **kwargs,
    ) -> str:
        """Restore a checkpoint; see the module docstring for the semantics.

        Args:
            checkpoint_path: The ``CheckpointPath``, path, or handle that
                ``checkpoint`` returned.
            cpu: Not enforced: the template decides resources.
            memory: Not enforced.
            background: Ignored.
            direct: Ignored.
            timeout_seconds: How long a copy may take, waiting for other
                copies of the same checkpoint included (plus grace).
            remote: Ignored: a Substrate checkpoint restores on any node that
                can read its path or handle.
            resume_in_place: Hand back the checkpointed environment itself,
                without a call, and use the checkpoint up, instead of making
                a copy. None follows ``RAY_SUBSTRATE_RESTORE_MODE`` (default
                copy).
            **kwargs: ``env`` adds to the checkpoint's environment variables.
                ``image``, if given, must be the checkpoint's. ``dns``,
                ``cidr_allowlist``, ``capabilities``, ``mounts``, and a
                non-zero ``disk`` raise ``SandboxError``. Others, such as
                gVisor's ``_oci_spec_transform_fn``, are ignored.

        Returns:
            The restored sandbox's id.
        """
        bad = [
            k
            for k in ("dns", "cidr_allowlist", "capabilities", "mounts")
            if kwargs.get(k) is not None
        ]
        if _nonzero_size(kwargs.get("disk")):
            bad.append("disk")
        if bad:
            raise SandboxError(
                f"The Substrate sandbox backend can't honor {', '.join(bad)} "
                "on restore"
            )
        checkpoint = str(checkpoint_path)
        image = kwargs.get("image")
        recorded = (self._load(checkpoint).get("config") or {}).get("image")
        if image is not None and recorded and image != recorded:
            raise SandboxError(
                f"Checkpoint {checkpoint!r} is of image {recorded!r}, not {image!r}"
            )
        if resume_in_place is None:
            resume_in_place = self.settings.restore_mode == "resume"
        if resume_in_place:
            record = self._use_up(checkpoint, "resumed")
            new_id = record["env_id"]
            _retired.pop(new_id, None)
            # No RPC: Substrate wakes the environment on its next call.
        else:
            new_id, record = self._copy(checkpoint, timeout_seconds)
        config = dict(record.get("config") or {})
        if kwargs.get("env"):
            config["env"] = {**(config.get("env") or {}), **kwargs["env"]}
        _meta[new_id] = config
        return new_id

    def delete_checkpoint(self, checkpoint_path: str) -> None:
        """Delete a checkpoint's environment and files; later restores fail."""
        checkpoint = str(checkpoint_path)
        if not checkpoint.startswith(HANDLE_SCHEME):
            validate_checkpoint_dir(checkpoint)
        record = self._use_up(checkpoint, "deleted")
        env_id = record["env_id"]
        atespace = record.get("atespace") or self.settings.atespace
        _retired.pop(env_id, None)
        _meta.pop(env_id, None)
        budget = self.settings.rpc_timeout
        try:
            err = self._run(
                self._delete_until_gone(env_id, atespace, budget),
                timeout=budget + _GRACE_S,
            )
        except Exception as run_err:
            err = run_err
        if not checkpoint.startswith(HANDLE_SCHEME):
            # The checkpoint is gone either way: its files too.
            shutil.rmtree(checkpoint, ignore_errors=True)
        if err is not None:
            self._delete_eventually([env_id], atespace)
            raise _map_error(
                err,
                SandboxError,
                f"Substrate delete of checkpoint {checkpoint} (still retrying in "
                "the background)",
            ) from err

    def pause_sandbox(
        self, sandbox_id: str, timeout_seconds: Optional[float] = None
    ) -> None:
        self._check_live(sandbox_id)
        self._suspend(sandbox_id, timeout_seconds)

    def resume_sandbox(
        self, sandbox_id: str, timeout_seconds: Optional[float] = None
    ) -> None:
        # Substrate wakes a suspended environment on its next call.
        return None

    # --- extras ---

    def server_info(self, timeout: float = 30.0) -> Any:
        """Return ate-env-api's GetServerInfo answer."""
        return self._run(self.client.server_info(), timeout=timeout)


__all__ = [
    "SubstrateSandboxBackend",
    "SubstrateSettings",
    "parse_domain_by_node",
    "parse_template_by_domain",
    "shutdown",
    "template_for_domain",
]
