"""Manages per-environment Python worker fork server lifecycles for RuntimeEnvAgent."""

import asyncio
import atexit
import base64
import hashlib
import json
import logging
import os
import re
import shlex
import sys
import time
from collections import defaultdict
from contextlib import suppress
from typing import Callable, Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from ray._common.utils import get_or_create_event_loop
from ray._private.ray_constants import env_bool
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.uri_cache import URICache
from ray._private.workers.fork_server import ForkServerProcess
from ray.runtime_env import RuntimeEnv

# Maximum UTF-8 byte length of a NUL-terminated Linux AF_UNIX sun_path (108 - 1),
# matching ray._private.utils.validate_socket_filepath and worker_pool.cc.
_MAX_UNIX_SOCKET_PATH_BYTES = 107

_FORK_SERVER_ALLOWED_RUNTIME_ENV_KEYS: FrozenSet[str] = frozenset(
    {
        "env_vars",
        "config",
        "preload_modules",
        "working_dir",
        "excludes",
        "py_modules",
        "pip",
        "uv",
        "conda",
        "py_executable",
        "worker_process_setup_hook",
    }
)

_EXEC_BOUND_ENV_VAR_PREFIXES: Tuple[str, ...] = (
    "LD_",
    "MALLOC_",
    "TCMALLOC_",
    "PYTHON",
    "OMP_",
    "GOMP_",
    "KMP_",
    "MKL_",
    "OPENBLAS_",
)

_NON_EXEC_BOUND_ENV_VARS: FrozenSet[str] = frozenset(
    {
        "PYTHONPATH",
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
    }
)

_EXEC_BOUND_ENV_VARS_EXACT: FrozenSet[str] = frozenset(
    {
        "GLIBC_TUNABLES",
        "JE_MALLOC_CONF",
        "JE_ARROW_MALLOC_CONF",
        "_RJEM_MALLOC_CONF",
        "ASAN_OPTIONS",
        "TSAN_OPTIONS",
        "UBSAN_OPTIONS",
        "LSAN_OPTIONS",
        "MSAN_OPTIONS",
        "GRPC_POLL_STRATEGY",
        "GRPC_VERBOSITY",
        "GRPC_TRACE",
    }
)

_ENV_VAR_REF_RE = re.compile(
    r"\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))"
)


def is_exec_bound_env_var(key: str) -> bool:
    """Returns True if `key` must be set before execve() to take effect."""
    if key in _NON_EXEC_BOUND_ENV_VARS:
        return False
    return (
        key.startswith(_EXEC_BOUND_ENV_VAR_PREFIXES)
        or key in _EXEC_BOUND_ENV_VARS_EXACT
    )


def _expand_env_vars_snapshot(env_vars: Dict[str, str]) -> Dict[str, str]:
    """Expands `env_vars` against agent `os.environ` without mutating process state."""
    if not env_vars:
        return {}
    merged = {
        k: v
        for k, v in os.environ.items()
        if k not in ("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", "MALLOC_CONF")
    }
    resolved: Dict[str, str] = {}
    for key, value in env_vars.items():
        if not isinstance(value, str):
            continue

        def _replace_match(match: re.Match) -> str:
            braced, bare = match.group(1), match.group(2)
            name = braced or bare
            if name in merged:
                return merged[name]
            if braced is not None and re.fullmatch(r"[A-Z0-9_]+", braced):
                return ""
            return match.group(0)

        out = _ENV_VAR_REF_RE.sub(_replace_match, value)
        merged[key] = out
        resolved[key] = out
    return resolved


class ForkServerManager:
    """Manages default and per-runtime_env ForkServerProcess instances on Linux."""

    def __init__(
        self,
        *,
        sockets_dir: str,
        logs_dir: Optional[str],
        logger: logging.Logger,
        uris_parser: Callable[[RuntimeEnv], List[Tuple[str, str]]],
        pypath_plugins: Sequence[RuntimeEnvPlugin],
        custom_plugin_names: FrozenSet[str],
        env_cache_contains: Callable[[str], bool],
        workers_cgroup_path: str = "",
        worker_stdout_fd: Optional[int] = None,
        worker_stderr_fd: Optional[int] = None,
        node_id: Optional[str] = None,
    ):
        self._sockets_dir = sockets_dir
        self._logs_dir = logs_dir or None
        self._logger = logger
        self._uris_parser = uris_parser
        self._pypath_plugins = tuple(pypath_plugins)
        self._custom_plugin_names = custom_plugin_names
        self._env_cache_contains = env_cache_contains
        self._workers_cgroup_path = workers_cgroup_path
        self._worker_stdout_fd = worker_stdout_fd
        self._worker_stderr_fd = worker_stderr_fd
        raw_node_id = os.environ.get("RAY_NODE_ID", "") if node_id is None else node_id
        self._node_id_prefix = raw_node_id[:8]

        self.enabled = sys.platform.startswith("linux") and env_bool(
            "RAY_enable_worker_fork_server", False
        )
        self._shutting_down = False
        self._socket_path_len_warned = False
        self._default_server: Optional[ForkServerProcess] = None
        self._default_monitor_task: Optional[asyncio.Task] = None
        self._servers: Dict[str, ForkServerProcess] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._lock_waiters: Dict[str, int] = defaultdict(int)
        self._failures: Dict[Optional[str], int] = {}
        self._cap_warn_at = 0.0
        self._idle_timers: Dict[str, asyncio.TimerHandle] = {}
        self._raylet_env_refs: Dict[str, int] = defaultdict(int)
        # Maps serialized_env -> per-env fs_key, or None when mapped to default fs_0.
        self._env_to_fs_key: Dict[str, Optional[str]] = {}
        self._fs_key_to_uris: Dict[str, Set[str]] = {}
        self._bypass_logged: Set[str] = set()

        self._max_active_servers = max(
            0, int(os.environ.get("RAY_max_active_fork_servers", "32"))
        )
        self._max_idle_envs = max(
            0, int(os.environ.get("RAY_runtime_env_max_idle_fork_servers", "0"))
        )
        self._idle_ttl_s = max(
            0.0, float(os.environ.get("RAY_runtime_env_fork_server_idle_ttl_s", "60"))
        )
        self._monitor_interval_s = max(
            0.01, float(os.environ.get("RAY_fork_server_monitor_interval_s", "0.5"))
        )
        self._respawn_backoff_base_s = max(
            0.0, float(os.environ.get("RAY_fork_server_respawn_backoff_base_s", "0.5"))
        )
        self._respawn_backoff_max_s = max(
            self._respawn_backoff_base_s,
            float(os.environ.get("RAY_fork_server_respawn_backoff_max_s", "30.0")),
        )
        self._healthy_uptime_s = max(
            0.0, float(os.environ.get("RAY_fork_server_healthy_uptime_s", "10.0"))
        )
        self._max_restarts = max(
            0, int(os.environ.get("RAY_fork_server_max_restarts", "5"))
        )

        if self.enabled:
            if os.environ.get("PYTHONPATH"):
                os.environ["PYTHONPATH"] = os.pathsep.join(
                    os.path.abspath(p) if p and "$" not in p else p
                    for p in os.environ["PYTHONPATH"].split(os.pathsep)
                )
            atexit.register(self.shutdown)
            if self._max_active_servers > 0 and self._socket_path(None) is not None:
                loop = get_or_create_event_loop()
                # Launch fs_0 non-blockingly and supervise readiness asynchronously so
                # slow preloads never stall RuntimeEnvAgent HTTP startup.
                if self._start_server(None) is None:
                    self._record_failure(None)
                self._default_monitor_task = loop.create_task(
                    self._supervise_default_server()
                )

    def wrap_uri_cache(self, uri_cache: URICache) -> None:
        """Hooks `uri_cache` deletion to stop fork servers before their URIs are deleted."""
        if not self.enabled:
            return
        orig_delete_fn = uri_cache._delete_fn

        def _wrapped_delete(uri: str, logger: logging.Logger) -> float:
            self._evict_servers_using_uri(uri, logger)
            return orig_delete_fn(uri, logger)

        uri_cache._delete_fn = _wrapped_delete

    def on_raylet_ref_added(self, serialized_env: str) -> None:
        """Records an active raylet worker reference and cancels any idle eviction timer."""
        self._raylet_env_refs[serialized_env] += 1
        if self.enabled:
            known_fs_key = self._env_to_fs_key.get(serialized_env)
            if known_fs_key is not None:
                self._cancel_idle_timer(known_fs_key)

    def on_raylet_ref_removed(self, serialized_env: str) -> None:
        """Decrements a raylet worker reference and stops/idles unused fork servers."""
        cur = self._raylet_env_refs.get(serialized_env, 0)
        if cur <= 0:
            return
        if cur > 1:
            self._raylet_env_refs[serialized_env] = cur - 1
            return
        del self._raylet_env_refs[serialized_env]
        if not self.enabled:
            return
        fs_key = (
            self._env_to_fs_key.get(serialized_env)
            if self._env_cache_contains(serialized_env)
            else self._env_to_fs_key.pop(serialized_env, None)
        )
        if fs_key is None:
            return
        if fs_key in self._servers:
            self._on_server_became_idle(fs_key)
        elif not self._has_active_refs(fs_key):
            self._failures.pop(fs_key, None)
            self._maybe_prune_lock(fs_key)

    def on_env_cache_evicted(self, serialized_env: str) -> None:
        """Cleans up auxiliary metadata when an env is removed from RuntimeEnvAgent cache."""
        self._bypass_logged.discard(serialized_env)
        if self._raylet_env_refs.get(serialized_env, 0) <= 0:
            self._env_to_fs_key.pop(serialized_env, None)

    def is_bypassed(self, serialized_env: str) -> bool:
        """Returns True if `serialized_env` was already determined ineligible."""
        return serialized_env in self._bypass_logged

    def get_ready_socket_fast(self, serialized_env: str) -> Optional[str]:
        """Returns a ready socket path on warm cache hits without opening setup logs."""
        if not self.enabled or self._shutting_down:
            return None
        if serialized_env not in self._env_to_fs_key:
            return None
        fs_key = self._env_to_fs_key[serialized_env]
        if fs_key is None:
            server = self._default_server
            if (
                server is not None
                and server.ready_at is not None
                and server.is_alive()
                and os.path.exists(server.socket_path)
            ):
                self._refresh_healthy_uptime(None, server)
                return server.socket_path
            return None
        server = self._servers.get(fs_key)
        if (
            server is not None
            and server.ready_at is not None
            and server.is_alive()
            and os.path.exists(server.socket_path)
            and server.has_valid_cwd()
        ):
            self._cancel_idle_timer(fs_key)
            self._refresh_healthy_uptime(fs_key, server)
            return server.socket_path
        return None

    @staticmethod
    def compute_setup_deadline(
        setup_start: float, setup_timeout_seconds: Optional[int]
    ) -> Optional[float]:
        """Returns a monotonic deadline clamped 1.5s below `setup_timeout_seconds`."""
        if setup_timeout_seconds is None or setup_timeout_seconds <= 0:
            return None
        # Reserve 1.5s for ForkServerProcess.stop() (up to 1.0s SIGTERM grace) and
        # context serialization so slow preloads fall back before wait_for cancels setup.
        return setup_start + setup_timeout_seconds - 1.5

    @staticmethod
    def _clamped_ready_timeout_s(setup_deadline: Optional[float]) -> float:
        default_timeout_s = float(
            os.environ.get("RAY_fork_server_ready_timeout_s", "120.0")
        )
        if setup_deadline is None:
            return default_timeout_s
        return min(default_timeout_s, setup_deadline - time.monotonic())

    @staticmethod
    async def _stop_server_async(server: ForkServerProcess) -> None:
        await asyncio.get_running_loop().run_in_executor(None, server.stop)

    def _should_use_fork_server(
        self,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = None,
    ) -> bool:
        if not self.enabled or context.override_worker_entrypoint:
            return False
        env_keys = set(runtime_env.keys()) - RuntimeEnv.extensions_fields
        if not (env_keys <= _FORK_SERVER_ALLOWED_RUNTIME_ENV_KEYS):
            return False
        if self._custom_plugin_names and (env_keys & self._custom_plugin_names):
            return False
        user_py_exec = runtime_env.get("py_executable")
        if isinstance(user_py_exec, str) and user_py_exec.strip():
            try:
                is_multi_arg = len(shlex.split(user_py_exec)) != 1
            except ValueError:
                is_multi_arg = True
            if is_multi_arg:
                if logger is not None:
                    logger.info(
                        "Bypassing worker fork server because runtime_env "
                        "py_executable %r is a multi-argument wrapper command.",
                        user_py_exec,
                    )
                return False
        return True

    def _build_spec(
        self,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> Tuple[Optional[str], Optional[RuntimeEnvContext], Set[str]]:
        uris_list = sorted({uri for uri, _ in self._uris_parser(runtime_env)})
        pypath_ctx = RuntimeEnvContext()
        for plugin in self._pypath_plugins:
            if plugin.name in runtime_env and runtime_env[plugin.name] is not None:
                plugin_uris = plugin.get_uris(runtime_env)
                if plugin_uris:
                    plugin.modify_context(plugin_uris, runtime_env, pypath_ctx, logger)

        expanded_ctx_vars = _expand_env_vars_snapshot(context.env_vars)
        exec_env_vars = {
            k: expanded_ctx_vars[k]
            for k in sorted(expanded_ctx_vars)
            if is_exec_bound_env_var(k)
        }
        preload_modules = list(runtime_env.preload_modules() or [])
        fs_env_vars = {**pypath_ctx.env_vars, **exec_env_vars}
        # Preserve user PYTHONPATH pre-fork when preload_modules is set so pre-fork
        # imports can resolve modules on user PYTHONPATH.
        if preload_modules and "PYTHONPATH" in expanded_ctx_vars:
            fs_env_vars["PYTHONPATH"] = expanded_ctx_vars["PYTHONPATH"]

        py_exec = context.py_executable or sys.executable
        if (
            not uris_list
            and py_exec == sys.executable
            and not context.command_prefix
            and not fs_env_vars
            and not preload_modules
        ):
            return None, None, set()

        fs_key = json.dumps(
            {
                "uris": uris_list,
                "py_executable": py_exec,
                "command_prefix": list(context.command_prefix),
                "env_vars": fs_env_vars,
                "preload_modules": preload_modules,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        fs_context = RuntimeEnvContext(
            command_prefix=list(context.command_prefix),
            env_vars=fs_env_vars,
            py_executable=py_exec,
            override_worker_entrypoint=context.override_worker_entrypoint,
        )
        return fs_key, fs_context, set(uris_list)

    def _socket_name(self, fs_key: Optional[str]) -> str:
        if fs_key is None:
            return f"fs_{self._node_id_prefix}_0" if self._node_id_prefix else "fs_0"
        key_input = (
            f"{self._node_id_prefix}:{fs_key}" if self._node_id_prefix else fs_key
        )
        digest = hashlib.sha256(key_input.encode("utf-8")).digest()
        tag = base64.urlsafe_b64encode(digest).decode("ascii")[:9]
        return f"fs_{tag}"

    def _socket_path(
        self, fs_key: Optional[str], logger: Optional[logging.Logger] = None
    ) -> Optional[str]:
        sock_path = os.path.join(self._sockets_dir, self._socket_name(fs_key))
        sock_len = len(sock_path.encode("utf-8"))
        if sock_len > _MAX_UNIX_SOCKET_PATH_BYTES:
            if not self._socket_path_len_warned:
                self._socket_path_len_warned = True
                (logger or self._logger).warning(
                    "Fork server AF_UNIX socket path %r (%d bytes) exceeds the %d-byte "
                    "limit; bypassing fork server (use a shorter --temp-dir).",
                    sock_path,
                    sock_len,
                    _MAX_UNIX_SOCKET_PATH_BYTES,
                )
            return None
        return sock_path

    def _has_active_refs(self, fs_key: str) -> bool:
        return any(
            count > 0 and self._env_to_fs_key.get(env_str) == fs_key
            for env_str, count in self._raylet_env_refs.items()
        )

    def _cancel_idle_timer(self, fs_key: str) -> None:
        handle = self._idle_timers.pop(fs_key, None)
        if handle is not None:
            handle.cancel()

    def _maybe_prune_lock(self, fs_key: str) -> None:
        if (
            self._lock_waiters.get(fs_key, 0) > 0
            or fs_key in self._servers
            or fs_key in self._idle_timers
            or self._has_active_refs(fs_key)
        ):
            return
        lock = self._locks.get(fs_key)
        if lock is not None and not lock.locked():
            self._locks.pop(fs_key, None)

    def _refresh_healthy_uptime(
        self, fs_key: Optional[str], server: ForkServerProcess
    ) -> None:
        if (
            server.ready_at is not None
            and time.monotonic() - server.ready_at >= self._healthy_uptime_s
        ):
            self._failures.pop(fs_key, None)

    def _failures_exceeded(self, fs_key: Optional[str]) -> bool:
        return self._failures.get(fs_key, 0) >= max(1, self._max_restarts)

    def _record_failure(
        self, fs_key: Optional[str], logger: Optional[logging.Logger] = None
    ) -> int:
        failures = self._failures.get(fs_key, 0) + 1
        self._failures[fs_key] = failures
        if failures == max(1, self._max_restarts):
            label = (
                "Default fork server"
                if fs_key is None
                else "Fork server for runtime_env"
            )
            (logger or self._logger).error(
                "%s failed %d consecutive time(s); disabling respawn and falling back "
                "to execvpe.",
                label,
                failures,
            )
        return failures

    @staticmethod
    def _emit_startup_warnings(
        server: ForkServerProcess, logger: logging.Logger
    ) -> None:
        for warning_line in server.read_startup_warnings():
            logger.warning("%s", warning_line)

    def _live_server_count(self) -> int:
        default_live = int(
            self._default_server is not None and self._default_server.is_alive()
        )
        return default_live + sum(1 for srv in self._servers.values() if srv.is_alive())

    def _make_server_slot(self, logger: Optional[logging.Logger] = None) -> bool:
        if self._max_active_servers <= 0:
            return False
        for key, srv in list(self._servers.items()):
            if srv.is_alive():
                continue
            self._servers.pop(key, None)
            self._fs_key_to_uris.pop(key, None)
            self._cancel_idle_timer(key)
            if not srv.stopped:
                self._refresh_healthy_uptime(key, srv)
                if self._has_active_refs(key):
                    self._record_failure(key)
                else:
                    self._failures.pop(key, None)
            srv.stop()
            self._maybe_prune_lock(key)

        while (
            self._live_server_count() >= self._max_active_servers and self._idle_timers
        ):
            self._evict_idle_server(next(iter(self._idle_timers)))

        live_count = self._live_server_count()
        if live_count >= self._max_active_servers:
            now = time.monotonic()
            if now - self._cap_warn_at >= 10.0:
                self._cap_warn_at = now
                (logger or self._logger).warning(
                    "Active fork server count (%d) reached max_active_fork_servers "
                    "(%d); bypassing fork server and using execvpe fallback.",
                    live_count,
                    self._max_active_servers,
                )
            return False
        return True

    def _start_server(
        self,
        fs_key: Optional[str],
        preload_modules: Optional[List[str]] = None,
        fs_context: Optional[RuntimeEnvContext] = None,
        uris: Optional[Set[str]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> Optional[ForkServerProcess]:
        if self._shutting_down:
            return None
        log = logger or self._logger
        if fs_key is None:
            if self._default_server is not None:
                self._default_server.stop()
                self._default_server = None
        else:
            self._cancel_idle_timer(fs_key)
            self._fs_key_to_uris.pop(fs_key, None)
            existing = self._servers.pop(fs_key, None)
            if existing is not None:
                existing.stop()

        socket_path = self._socket_path(fs_key, log)
        if socket_path is None:
            return None

        setup_log_path = None
        if self._logs_dir:
            sock_name = os.path.basename(socket_path)
            if fs_key is None:
                log_name = (
                    f"fork_server_default_{self._node_id_prefix}.log"
                    if self._node_id_prefix
                    else "fork_server_default.log"
                )
            else:
                log_name = f"fork_server_{sock_name[3:]}.log"
            setup_log_path = os.path.join(self._logs_dir, log_name)

        try:
            server = ForkServerProcess(
                socket_path=socket_path,
                preload_modules=preload_modules,
                context=fs_context,
                cgroup_path=self._workers_cgroup_path or None,
                wait_ready=False,
                logs_dir=self._logs_dir,
                setup_log_path=setup_log_path,
                worker_stdout_fd=self._worker_stdout_fd,
                worker_stderr_fd=self._worker_stderr_fd,
            )
            if fs_key is None:
                self._default_server = server
            else:
                self._servers[fs_key] = server
                if uris:
                    self._fs_key_to_uris[fs_key] = set(uris)
                else:
                    self._fs_key_to_uris.pop(fs_key, None)
            log.info(
                "Started fork server at pid %s (socket=%s)",
                server.process.pid,
                socket_path,
            )
            return server
        except Exception as e:
            log.warning(
                "Failed to start fork server for runtime_env, falling back to exec: %s",
                e,
            )
            return None

    async def _supervise_default_server(self) -> None:
        while not self._shutting_down:
            server = self._default_server
            if server is not None:
                if server.ready_at is None:
                    try:
                        await server.wait_ready_async()
                        self._emit_startup_warnings(server, self._logger)
                        continue
                    except Exception as e:
                        self._emit_startup_warnings(server, self._logger)
                        if self._shutting_down:
                            return
                        if not server.stopped:
                            self._logger.warning(
                                "Default fork server failed readiness: %s", e
                            )
                elif server.is_alive():
                    self._refresh_healthy_uptime(None, server)
                    await asyncio.sleep(self._monitor_interval_s)
                    continue
                elif not server.stopped:
                    self._refresh_healthy_uptime(None, server)
                    self._logger.warning(
                        "Default fork server (pid=%s) exited unexpectedly with code %s.",
                        server.process.pid,
                        server.process.returncode,
                    )
                if self._default_server is server:
                    self._default_server = None
                was_stopped = server.stopped
                await self._stop_server_async(server)
                if not was_stopped:
                    self._record_failure(None)

            if self._shutting_down or self._failures_exceeded(None):
                return
            failures = self._failures.get(None, 0)
            if failures > 0:
                backoff_s = min(
                    self._respawn_backoff_max_s,
                    self._respawn_backoff_base_s * (2 ** (failures - 1)),
                )
                await asyncio.sleep(backoff_s)
                if self._shutting_down:
                    return
            if self._socket_path(None) is None:
                return
            if not self._make_server_slot(self._logger):
                await asyncio.sleep(self._monitor_interval_s)
                continue
            if self._start_server(None) is None:
                self._record_failure(None)

    def _evict_servers_using_uri(self, uri: str, logger: logging.Logger) -> None:
        matching_keys = [
            fs_key for fs_key, uris in list(self._fs_key_to_uris.items()) if uri in uris
        ]
        for fs_key in matching_keys:
            self._cancel_idle_timer(fs_key)
            server = self._servers.pop(fs_key, None)
            self._fs_key_to_uris.pop(fs_key, None)
            self._failures.pop(fs_key, None)
            if server is not None:
                logger.info(
                    "Stopping fork server (pid=%s) before URICache evicts %s.",
                    server.process.pid,
                    uri,
                )
                server.stop()
            self._maybe_prune_lock(fs_key)

    def _on_server_became_idle(self, fs_key: str) -> None:
        if self._has_active_refs(fs_key):
            return
        if fs_key not in self._servers:
            self._cancel_idle_timer(fs_key)
            self._failures.pop(fs_key, None)
            self._maybe_prune_lock(fs_key)
            return
        if self._max_idle_envs <= 0 or self._idle_ttl_s <= 0:
            self._evict_idle_server(fs_key)
            return
        self._cancel_idle_timer(fs_key)
        loop = get_or_create_event_loop()
        self._idle_timers[fs_key] = loop.call_later(
            self._idle_ttl_s,
            self._evict_idle_server,
            fs_key,
        )
        while len(self._idle_timers) > self._max_idle_envs:
            self._evict_idle_server(next(iter(self._idle_timers)))

    def _evict_idle_server(self, fs_key: str) -> None:
        self._cancel_idle_timer(fs_key)
        if self._has_active_refs(fs_key):
            return
        server = self._servers.pop(fs_key, None)
        self._fs_key_to_uris.pop(fs_key, None)
        if server is not None:
            server.stop()
        self._failures.pop(fs_key, None)
        self._maybe_prune_lock(fs_key)

    async def _resolve_default_socket(
        self, setup_deadline: Optional[float]
    ) -> Optional[str]:
        server = self._default_server
        if server is None or not server.is_alive():
            return None
        if server.ready_at is None and not os.path.exists(server.socket_path):
            wait_s = self._clamped_ready_timeout_s(setup_deadline)
            if wait_s <= 0:
                return None
            try:
                await server.wait_ready_async(timeout_s=wait_s)
                self._emit_startup_warnings(server, self._logger)
            except Exception:
                self._emit_startup_warnings(server, self._logger)
                return None
        return (
            server.socket_path
            if server.is_alive() and os.path.exists(server.socket_path)
            else None
        )

    async def _acquire_lock_with_deadline(
        self,
        fs_key: str,
        lock: asyncio.Lock,
        setup_deadline: Optional[float],
        logger: logging.Logger,
    ) -> bool:
        if setup_deadline is None or (
            not lock.locked() and self._lock_waiters[fs_key] <= 1
        ):
            await lock.acquire()
            return True
        remaining = setup_deadline - time.monotonic()
        if remaining <= 0:
            if not self._has_active_refs(fs_key):
                self._on_server_became_idle(fs_key)
            return False
        acquire_task = asyncio.create_task(lock.acquire())
        try:
            done, _ = await asyncio.wait({acquire_task}, timeout=remaining)
            if acquire_task in done:
                acquire_task.result()
                return True
        except BaseException:
            acquire_task.cancel()
            with suppress(asyncio.CancelledError):
                await acquire_task
            if (
                acquire_task.done()
                and not acquire_task.cancelled()
                and acquire_task.exception() is None
            ):
                lock.release()
            raise
        acquire_task.cancel()
        with suppress(asyncio.CancelledError):
            await acquire_task
        # Guard against acquire completing on the same event-loop tick as timeout.
        if (
            acquire_task.done()
            and not acquire_task.cancelled()
            and acquire_task.exception() is None
        ):
            lock.release()
        if not self._has_active_refs(fs_key):
            self._on_server_became_idle(fs_key)
        logger.warning(
            "Timed out waiting for fork server lock within setup_timeout_seconds; "
            "falling back to exec."
        )
        return False

    async def _drop_unhealthy_server_async(
        self, fs_key: str, server: ForkServerProcess, logger: logging.Logger
    ) -> Optional[ForkServerProcess]:
        alive_and_sock = server.is_alive() and os.path.exists(server.socket_path)
        if alive_and_sock and server.has_valid_cwd():
            self._refresh_healthy_uptime(fs_key, server)
            return server
        self._servers.pop(fs_key, None)
        self._fs_key_to_uris.pop(fs_key, None)
        if not alive_and_sock and not server.stopped:
            self._refresh_healthy_uptime(fs_key, server)
            failures = self._record_failure(fs_key, logger)
            logger.warning(
                "Fork server for runtime_env exited unexpectedly (code=%s, "
                "failures=%d); attempting lazy respawn.",
                server.process.returncode,
                failures,
            )
        elif alive_and_sock and not server.stopped:
            logger.info(
                "Fork server (pid=%s) working directory changed or was deleted; "
                "restarting fork server.",
                server.process.pid,
            )
        await self._stop_server_async(server)
        return None

    async def resolve_socket(
        self,
        serialized_env: str,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: logging.Logger,
        setup_deadline: Optional[float] = None,
    ) -> Optional[str]:
        """Returns a ready fork server Unix socket path for `runtime_env`, or None."""
        try:
            return await self._resolve_socket_inner(
                serialized_env, runtime_env, context, logger, setup_deadline
            )
        except Exception as e:
            logger.warning(
                "Unexpected error resolving fork server socket; falling back to "
                "exec: %s",
                e,
            )
            return None

    async def _resolve_socket_inner(
        self,
        serialized_env: str,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: logging.Logger,
        setup_deadline: Optional[float],
    ) -> Optional[str]:
        if self._shutting_down:
            return None
        bypass_logger = None if serialized_env in self._bypass_logged else logger
        if not self._should_use_fork_server(runtime_env, context, logger=bypass_logger):
            self._bypass_logged.add(serialized_env)
            self._env_to_fs_key.pop(serialized_env, None)
            return None

        fs_key, fs_context, uris = self._build_spec(runtime_env, context, logger)
        if fs_key is None:
            self._env_to_fs_key[serialized_env] = None
            return await self._resolve_default_socket(setup_deadline)

        self._env_to_fs_key[serialized_env] = fs_key
        self._cancel_idle_timer(fs_key)
        lock = self._locks.setdefault(fs_key, asyncio.Lock())
        self._lock_waiters[fs_key] += 1
        acquired = False
        try:
            acquired = await self._acquire_lock_with_deadline(
                fs_key, lock, setup_deadline, logger
            )
            if not acquired or self._shutting_down:
                return None

            self._cancel_idle_timer(fs_key)
            server = self._servers.get(fs_key)
            if server is not None:
                server = await self._drop_unhealthy_server_async(fs_key, server, logger)

            if server is None:
                if self._clamped_ready_timeout_s(setup_deadline) <= 0:
                    logger.warning(
                        "Skipping cold fork server start: insufficient "
                        "setup_timeout_seconds budget remaining; falling back to exec."
                    )
                    if not self._has_active_refs(fs_key):
                        self._on_server_became_idle(fs_key)
                    return None
                if (
                    not self._has_active_refs(fs_key)
                    or self._failures_exceeded(fs_key)
                    or not self._make_server_slot(logger)
                ):
                    return None
                server = self._start_server(
                    fs_key,
                    preload_modules=runtime_env.preload_modules(),
                    fs_context=fs_context,
                    uris=uris,
                    logger=logger,
                )
                if server is None:
                    if self._socket_path(fs_key, logger) is not None:
                        self._record_failure(fs_key, logger)
                    return None
                wait_s = max(0.0, self._clamped_ready_timeout_s(setup_deadline))
                try:
                    await server.wait_ready_async(timeout_s=wait_s)
                except BaseException as e:
                    self._emit_startup_warnings(server, logger)
                    was_stopped = server.stopped
                    if self._servers.get(fs_key) is server:
                        self._servers.pop(fs_key, None)
                        self._fs_key_to_uris.pop(fs_key, None)
                    await self._stop_server_async(server)
                    if not isinstance(e, Exception):
                        raise
                    if not was_stopped:
                        self._record_failure(fs_key, logger)
                        logger.warning(
                            "Failed to wait for fork server, falling back to exec: %s",
                            e,
                        )
                    return None
                self._emit_startup_warnings(server, logger)
                if not server.has_valid_cwd():
                    logger.info(
                        "Fork server (pid=%s) working directory changed during "
                        "startup; stopping stale server and falling back to exec.",
                        server.process.pid,
                    )
                    if self._servers.get(fs_key) is server:
                        self._servers.pop(fs_key, None)
                        self._fs_key_to_uris.pop(fs_key, None)
                    await self._stop_server_async(server)
                    return None

            if not self._has_active_refs(fs_key):
                self._on_server_became_idle(fs_key)
                if fs_key not in self._servers:
                    return None
            return server.socket_path
        finally:
            if acquired:
                lock.release()
            remaining_waiters = self._lock_waiters.get(fs_key, 0) - 1
            if remaining_waiters > 0:
                self._lock_waiters[fs_key] = remaining_waiters
            else:
                self._lock_waiters.pop(fs_key, None)
                self._maybe_prune_lock(fs_key)

    def shutdown(self) -> None:
        """Stops all running fork servers and cancels background monitoring."""
        self._shutting_down = True
        atexit.unregister(self.shutdown)
        if self._default_monitor_task is not None:
            with suppress(RuntimeError):
                self._default_monitor_task.cancel()
            self._default_monitor_task = None
        while self._idle_timers:
            _, handle = self._idle_timers.popitem()
            handle.cancel()
        if self._default_server is not None:
            self._default_server.stop()
            self._default_server = None
        while self._servers:
            _, server = self._servers.popitem()
            server.stop()
        self._fs_key_to_uris.clear()
        self._bypass_logged.clear()
        self._locks.clear()
        self._lock_waiters.clear()
