import asyncio
import atexit
import hashlib
import logging
import logging.handlers
import os
import sys
import time
import traceback
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set, Tuple, Union

import ray
import ray._private.runtime_env.agent.runtime_env_consts as runtime_env_consts
from ray._common.utils import get_or_create_event_loop
from ray._private.ray_constants import (
    DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS,
    env_bool,
)
from ray._private.ray_logging import setup_component_logger
from ray._private.runtime_env.conda import CondaPlugin
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.default_impl import get_image_uri_plugin_cls
from ray._private.runtime_env.image_uri import ContainerPlugin
from ray._private.runtime_env.java_jars import JavaJarsPlugin
from ray._private.runtime_env.nsight import NsightPlugin
from ray._private.runtime_env.pip import PipPlugin
from ray._private.runtime_env.plugin import (
    RuntimeEnvPlugin,
    RuntimeEnvPluginManager,
    create_for_plugin_if_needed,
)
from ray._private.runtime_env.py_executable import PyExecutablePlugin
from ray._private.runtime_env.py_modules import PyModulesPlugin
from ray._private.runtime_env.redaction import redact_serialized_runtime_env
from ray._private.runtime_env.rocprof_sys import RocProfSysPlugin
from ray._private.runtime_env.uv import UvPlugin
from ray._private.runtime_env.working_dir import WorkingDirPlugin
from ray._private.workers.fork_server import ForkServerProcess
from ray._raylet import GcsClient
from ray.core.generated import runtime_env_agent_pb2
from ray.core.generated.runtime_env_common_pb2 import (
    RuntimeEnvState as ProtoRuntimeEnvState,
)
from ray.runtime_env import RuntimeEnv, RuntimeEnvConfig

default_logger = logging.getLogger(__name__)

# TODO(edoakes): this is used for unit tests. We should replace it with a
# better pluggability mechanism once available.
SLEEP_FOR_TESTING_S = os.environ.get("RAY_RUNTIME_ENV_SLEEP_FOR_TESTING_S")


@dataclass
class CreatedEnvResult:
    # Whether or not the env was installed correctly.
    success: bool
    # If success is True, will be a serialized RuntimeEnvContext
    # If success is False, will be an error message.
    result: str
    # The time to create a runtime env in ms.
    creation_time_ms: int


# e.g., "working_dir"
UriType = str


class _SetupFileHandler(logging.handlers.RotatingFileHandler):
    """A rotating file handler that stays closed once closed.

    FileHandler.emit (and RotatingFileHandler.shouldRollover) lazily reopen
    the file whenever the stream is None. Without this guard, a thread that
    read the handler from the logger's handler list before it was detached —
    e.g. executor-backed plugin work logging after a setup timeout — would
    silently reopen the file on a late emit, leaking a descriptor with no
    owner left to close it. Handler.handle() and FileHandler.close() take
    the same per-handler lock, so checking the flag in emit() cannot race
    with close().
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._closed_by_pool = False

    def close(self) -> None:
        self.acquire()
        try:
            self._closed_by_pool = True
            # The handler lock is reentrant, so the nested acquire in
            # FileHandler.close is fine.
            super().close()
        finally:
            self.release()

    def emit(self, record: logging.LogRecord) -> None:
        if self._closed_by_pool:
            return
        super().emit(record)


@dataclass
class _PooledHandler:
    """A log handler shared by concurrent setups, closed at zero references."""

    handler: logging.Handler
    ref_count: int = 0


class SetupLoggerFactory:
    """Creates loggers scoped to a single runtime env setup.

    Loggers used to be cached per job and never closed, so the agent's open
    file descriptors grew with the cumulative number of jobs the node had ever
    handled until setup failed with EMFILE (#54935). Instead, each setup gets
    its own logger for the duration of the setup only:

    - File handlers are pooled by absolute log path and reference counted, so
      concurrent setups writing the same file share one handler (and one
      rotation owner). The file is closed when the last setup using it exits.
    - The logger is constructed directly instead of via `logging.getLogger`,
      so the global logging registry never holds a reference to it.

    Only file *descriptors* are scoped to the setup; the log files themselves
    are left on disk for the log monitor and dashboard to read.
    """

    def __init__(
        self,
        *,
        logging_level: Union[int, str],
        logging_format: str,
        log_dir: str,
        max_bytes: int,
        backup_count: int,
    ):
        if isinstance(logging_level, str):
            logging_level = logging.getLevelName(logging_level.upper())
        self._logging_level = logging_level
        self._formatter = logging.Formatter(logging_format)
        self._log_dir = log_dir
        self._max_bytes = max_bytes
        self._backup_count = backup_count
        # Absolute log path -> pooled handler. Only ever touched from the
        # event loop thread with no `await` in between, so no lock is needed;
        # executor-backed plugin work holds the logger, never this pool.
        self._pooled_handlers: Dict[str, _PooledHandler] = {}

    def _acquire(self, path: str) -> logging.Handler:
        entry = self._pooled_handlers.get(path)
        if entry is None:
            handler = _SetupFileHandler(
                path,
                maxBytes=self._max_bytes,
                backupCount=self._backup_count,
            )
            handler.setLevel(self._logging_level)
            handler.setFormatter(self._formatter)
            entry = _PooledHandler(handler=handler)
            self._pooled_handlers[path] = entry
        entry.ref_count += 1
        return entry.handler

    def _release(self, path: str) -> None:
        entry = self._pooled_handlers[path]
        entry.ref_count -= 1
        if entry.ref_count == 0:
            del self._pooled_handlers[path]
            entry.handler.close()

    @contextmanager
    def setup_logger(self, job_id: str, log_files: List[str]):
        """Yield a logger for one setup, releasing its file handles on exit.

        The `finally` below runs on every exit path: success, exception,
        timeout, and cancellation (`asyncio.wait_for` awaits the cancelled
        coroutine before raising, so the context manager unwinds first).
        """
        filenames = [f"runtime_env_setup-{job_id}.log", *log_files]
        # Not `logging.getLogger`: see the class docstring.
        logger = logging.Logger(f"runtime_env_{job_id}")
        logger.setLevel(self._logging_level)
        logger.propagate = False
        acquired: List[str] = []
        try:
            for filename in filenames:
                if not filename or not self._log_dir:
                    # Mirror setup_component_logger's fallback to stderr.
                    # Stream handlers own no file descriptor, so they are
                    # not pooled.
                    handler = logging.StreamHandler()
                    handler.setLevel(self._logging_level)
                    handler.setFormatter(self._formatter)
                    logger.addHandler(handler)
                    continue
                path = os.path.abspath(os.path.join(self._log_dir, filename))
                if path in acquired:
                    continue
                handler = self._acquire(path)
                acquired.append(path)
                logger.addHandler(handler)
            yield logger
        finally:
            # Atomically swap the handler list before closing anything, for
            # late writes from executor-backed plugin work (e.g. after a
            # setup timeout): a concurrent Logger.callHandlers sees either
            # the old list object (handlers still open at this point) or the
            # new one — never an empty list, which would fall through to
            # logging.lastResort on stderr, and never a half-detached state.
            # A writer iterating the old snapshot may still emit after the
            # close below; _SetupFileHandler refuses to emit once closed, so
            # that write cannot reopen the file and re-leak the descriptor.
            logger.handlers = [logging.NullHandler()]
            for path in acquired:
                self._release(path)


class ReferenceTable:
    """
    The URI reference table which is used for GC.
    When the reference count is decreased to zero,
    the URI should be removed from this table and
    added to cache if needed.
    """

    def __init__(
        self,
        uris_parser: Callable[[RuntimeEnv], Tuple[str, UriType]],
        unused_uris_callback: Callable[[List[Tuple[str, UriType]]], None],
        unused_runtime_env_callback: Callable[[str], None],
    ):
        # Runtime Environment reference table. The key is serialized runtime env and
        # the value is reference count.
        self._runtime_env_reference: Dict[str, int] = defaultdict(int)
        # URI reference table. The key is URI parsed from runtime env and the value
        # is reference count.
        self._uri_reference: Dict[str, int] = defaultdict(int)
        self._uris_parser = uris_parser
        self._unused_uris_callback = unused_uris_callback
        self._unused_runtime_env_callback = unused_runtime_env_callback
        # send the `DeleteRuntimeEnvIfPossible` RPC when the client exits. The URI won't
        # be leaked now because the reference count will be reset to zero when the job
        # finished.
        self._reference_exclude_sources: Set[str] = {
            "client_server",
        }

    def _increase_reference_for_uris(self, uris):
        default_logger.debug(f"Increase reference for uris {uris}.")
        for uri, _ in uris:
            self._uri_reference[uri] += 1

    def _decrease_reference_for_uris(self, uris):
        default_logger.debug(f"Decrease reference for uris {uris}.")
        unused_uris = list()
        for uri, uri_type in uris:
            if self._uri_reference[uri] > 0:
                self._uri_reference[uri] -= 1
                if self._uri_reference[uri] == 0:
                    unused_uris.append((uri, uri_type))
                    del self._uri_reference[uri]
            else:
                default_logger.warning(f"URI {uri} does not exist.")
        if unused_uris:
            default_logger.info(f"Unused uris {unused_uris}.")
            self._unused_uris_callback(unused_uris)
        return unused_uris

    def _increase_reference_for_runtime_env(self, serialized_env: str):
        default_logger.debug(
            f"Increase reference for runtime env "
            f"{redact_serialized_runtime_env(serialized_env)}."
        )
        self._runtime_env_reference[serialized_env] += 1

    def _decrease_reference_for_runtime_env(self, serialized_env: str):
        """Decrease reference count for the given [serialized_env]. Throw exception if we cannot decrement reference."""
        redacted_serialized_env = redact_serialized_runtime_env(serialized_env)
        default_logger.debug(
            f"Decrease reference for runtime env {redacted_serialized_env}."
        )
        unused = False
        if self._runtime_env_reference[serialized_env] > 0:
            self._runtime_env_reference[serialized_env] -= 1
            if self._runtime_env_reference[serialized_env] == 0:
                unused = True
                del self._runtime_env_reference[serialized_env]
        else:
            default_logger.warning(
                f"Runtime env {redacted_serialized_env} does not exist."
            )
            raise ValueError(
                f"{redacted_serialized_env} cannot decrement "
                "reference since the reference count is 0"
            )
        if unused:
            default_logger.info(f"Unused runtime env {redacted_serialized_env}.")
            self._unused_runtime_env_callback(serialized_env)

    def increase_reference(
        self, runtime_env: RuntimeEnv, serialized_env: str, source_process: str
    ) -> None:
        if source_process in self._reference_exclude_sources:
            return
        self._increase_reference_for_runtime_env(serialized_env)
        uris = self._uris_parser(runtime_env)
        self._increase_reference_for_uris(uris)

    def decrease_reference(
        self, runtime_env: RuntimeEnv, serialized_env: str, source_process: str
    ) -> None:
        """Decrease reference count for runtime env and uri. Throw exception if decrement reference count fails."""
        if source_process in self._reference_exclude_sources:
            return
        self._decrease_reference_for_runtime_env(serialized_env)
        uris = self._uris_parser(runtime_env)
        self._decrease_reference_for_uris(uris)

    @property
    def runtime_env_refs(self) -> Dict[str, int]:
        """Return the runtime_env -> ref count mapping.

        Returns:
            The mapping of serialized runtime env -> ref count.
        """
        return self._runtime_env_reference


class RuntimeEnvAgent:
    """An RPC server to create and delete runtime envs.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config.
    """

    def __init__(
        self,
        runtime_env_dir: str,
        logging_params: dict,
        gcs_client: GcsClient,
        temp_dir: str,
        address: str,
        runtime_env_agent_port: int,
        workers_cgroup_path: str = "",
    ):
        """Initialize the runtime env agent.

        Args:
            runtime_env_dir: Directory used to store runtime env resources.
            logging_params: Keyword arguments forwarded to
                :func:`setup_component_logger` to configure the agent logger.
            gcs_client: GCS client used to fetch package data.
            temp_dir: Temporary directory used by plugins (e.g. container plugin).
            address: IP address that the agent is listening on, used for logging.
            runtime_env_agent_port: Port that the agent is listening on, used for
                logging.
            workers_cgroup_path: Path to the node's ``user/workers`` cgroup v2 leaf
                when resource isolation is enabled, or empty string otherwise.
        """
        super().__init__()

        self._logger = default_logger
        self._logging_params = logging_params
        self._logger = setup_component_logger(
            logger_name=default_logger.name, **self._logging_params
        )
        # Don't propagate logs to the root logger, because these logs
        # might contain sensitive information. Instead, these logs should
        # be confined to the runtime env agent log file `self.LOG_FILENAME`.
        self._logger.propagate = False

        self._logger.info("Starting runtime env agent at pid %s", os.getpid())
        self._logger.info(f"Parent raylet pid is {os.environ.get('RAY_RAYLET_PID')}")

        self._runtime_env_dir = runtime_env_dir
        self._sockets_dir = os.path.join(
            os.path.dirname(os.path.abspath(self._runtime_env_dir)), "sockets"
        )
        self._enable_worker_fork_server = sys.platform.startswith("linux") and env_bool(
            "RAY_enable_worker_fork_server", False
        )
        self._workers_cgroup_path = workers_cgroup_path
        self._shutting_down = False
        self._default_fork_server: Optional[ForkServerProcess] = None
        self._default_fs_monitor_task: Optional[asyncio.Task] = None
        self._fork_servers: Dict[str, ForkServerProcess] = {}
        self._fork_server_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._fork_server_failures: Dict[Optional[str], int] = defaultdict(int)
        self._fork_server_cap_warn_at: float = 0.0
        self._idle_fork_servers: Dict[str, asyncio.TimerHandle] = {}
        self._max_active_fork_servers = max(
            0, int(os.environ.get("RAY_max_active_fork_servers", "32"))
        )
        self._fork_server_max_idle_envs = max(
            0, int(os.environ.get("RAY_runtime_env_max_idle_fork_servers", "0"))
        )
        self._fork_server_idle_ttl_s = max(
            0.0, float(os.environ.get("RAY_runtime_env_fork_server_idle_ttl_s", "60"))
        )
        self._fork_server_monitor_interval_s = max(
            0.01, float(os.environ.get("RAY_fork_server_monitor_interval_s", "0.5"))
        )
        self._fork_server_respawn_backoff_base_s = max(
            0.0, float(os.environ.get("RAY_fork_server_respawn_backoff_base_s", "0.5"))
        )
        self._fork_server_respawn_backoff_max_s = max(
            self._fork_server_respawn_backoff_base_s,
            float(os.environ.get("RAY_fork_server_respawn_backoff_max_s", "30.0")),
        )
        self._fork_server_healthy_uptime_s = max(
            0.0, float(os.environ.get("RAY_fork_server_healthy_uptime_s", "10.0"))
        )
        self._fork_server_max_restarts = max(
            0, int(os.environ.get("RAY_fork_server_max_restarts", "5"))
        )

        self._setup_logger_factory = SetupLoggerFactory(
            logging_level=self._logging_params["logging_level"],
            logging_format=self._logging_params["logging_format"],
            log_dir=self._logging_params["log_dir"],
            max_bytes=self._logging_params["max_bytes"],
            backup_count=self._logging_params["backup_count"],
        )
        # Cache the results of creating envs to avoid repeatedly calling into
        # conda and other slow calls.
        self._env_cache: Dict[str, CreatedEnvResult] = dict()
        # Maps a serialized runtime env to a lock that is used
        # to prevent multiple concurrent installs of the same env.
        self._env_locks: Dict[str, asyncio.Lock] = dict()
        self._gcs_client = gcs_client

        self._pip_plugin = PipPlugin(self._runtime_env_dir)
        self._uv_plugin = UvPlugin(self._runtime_env_dir)
        self._conda_plugin = CondaPlugin(self._runtime_env_dir)
        self._py_modules_plugin = PyModulesPlugin(
            self._runtime_env_dir, self._gcs_client
        )
        self._py_executable_plugin = PyExecutablePlugin()
        self._java_jars_plugin = JavaJarsPlugin(self._runtime_env_dir, self._gcs_client)
        self._working_dir_plugin = WorkingDirPlugin(
            self._runtime_env_dir, self._gcs_client
        )
        self._container_plugin = ContainerPlugin(temp_dir)
        # TODO(jonathan-anyscale): change the plugin to ProfilerPlugin
        # and unify with nsight and other profilers.
        self._nsight_plugin = NsightPlugin(self._runtime_env_dir)
        self._rocprof_sys_plugin = RocProfSysPlugin(self._runtime_env_dir)
        self._image_uri_plugin = get_image_uri_plugin_cls()(temp_dir)

        # TODO(architkulkarni): "base plugins" and third-party plugins should all go
        # through the same code path.  We should never need to refer to
        # self._xxx_plugin, we should just iterate through self._plugins.
        self._base_plugins: List[RuntimeEnvPlugin] = [
            self._working_dir_plugin,
            self._uv_plugin,
            self._pip_plugin,
            self._conda_plugin,
            self._py_modules_plugin,
            self._py_executable_plugin,
            self._java_jars_plugin,
            self._container_plugin,
            self._nsight_plugin,
            self._rocprof_sys_plugin,
            self._image_uri_plugin,
        ]
        self._plugin_manager = RuntimeEnvPluginManager()
        for plugin in self._base_plugins:
            self._plugin_manager.add_plugin(plugin)

        self._reference_table = ReferenceTable(
            self.uris_parser,
            self.unused_uris_processor,
            self.unused_runtime_env_processor,
        )

        self._logger.info(
            "Listening to address %s, port %d", address, runtime_env_agent_port
        )

        try:
            self._node_ip = ray.util.get_node_ip_address()
            self._node_prefix = f"[Node {self._node_ip}] "
        except Exception as e:
            self._logger.warning(f"Failed to get node IP address, using fallback: {e}")
            self._node_prefix = "[Node unknown] "

        if self._enable_worker_fork_server:
            atexit.register(self._cleanup_fork_servers)
            if self._max_active_fork_servers > 0:
                loop = get_or_create_event_loop()
                # Wait up to 10s (within raylet's 15s WaitForPersistedPort deadline) so
                # fork_server_default.sock is ready before WorkerPool prestarts workers;
                # slower RAY_preload_python_modules imports finish in the background.
                server = self._start_fork_server(None)
                if server is None:
                    self._record_fork_server_failure(None)
                elif not loop.is_running():
                    init_wait_s = float(
                        os.environ.get("RAY_default_fork_server_init_wait_s", "10.0")
                    )
                    try:
                        loop.run_until_complete(
                            server.wait_ready(timeout_s=init_wait_s)
                        )
                    except TimeoutError:
                        self._logger.warning(
                            "Default fork server did not finish preloading within %.1fs; "
                            "continuing initialization in background.",
                            init_wait_s,
                        )
                    except Exception as e:
                        self._default_fork_server = None
                        server.stop()
                        self._record_fork_server_failure(None)
                        self._logger.warning(
                            "Default fork server failed startup, falling back to exec: %s",
                            e,
                        )
                self._default_fs_monitor_task = loop.create_task(
                    self._supervise_default_fork_server()
                )

    def _should_start_fork_server(
        self, runtime_env: RuntimeEnv, context: RuntimeEnvContext
    ) -> bool:
        return self._enable_worker_fork_server and not (
            runtime_env.has_py_container()
            or runtime_env.image_uri()
            or context.override_worker_entrypoint
            or runtime_env.nsight()
            or runtime_env.rocprof_sys()
        )

    def _fork_server_cache_key(self, runtime_env: RuntimeEnv) -> Optional[str]:
        env_without_vars = {
            k: v for k, v in runtime_env.items() if k not in ("env_vars", "config")
        }
        if not env_without_vars:
            return None
        return RuntimeEnv(_validate=False, **env_without_vars).serialize()

    def _fork_server_socket_path(self, fs_key: Optional[str]) -> str:
        if fs_key is None:
            return os.path.join(self._sockets_dir, "fork_server_default.sock")
        digest = hashlib.sha256(fs_key.encode("utf-8")).hexdigest()[:16]
        return os.path.join(self._sockets_dir, f"fork_server_{digest}.sock")

    def _has_active_fork_server_refs(self, fs_key: str) -> bool:
        return any(
            count > 0
            and (
                env_str == fs_key
                or self._fork_server_cache_key(RuntimeEnv.deserialize(env_str))
                == fs_key
            )
            for env_str, count in self._reference_table.runtime_env_refs.items()
        )

    def _cancel_idle_timer(self, fs_key: str) -> None:
        handle = self._idle_fork_servers.pop(fs_key, None)
        if handle is not None:
            handle.cancel()

    def _refresh_healthy_uptime(
        self, fs_key: Optional[str], server: ForkServerProcess
    ) -> None:
        if (
            server.ready_at is not None
            and time.monotonic() - server.ready_at >= self._fork_server_healthy_uptime_s
        ):
            self._fork_server_failures[fs_key] = 0

    def _record_fork_server_failure(
        self, fs_key: Optional[str], logger: Optional[logging.Logger] = None
    ) -> int:
        self._fork_server_failures[fs_key] += 1
        failures = self._fork_server_failures[fs_key]
        if failures == self._fork_server_max_restarts:
            label = (
                "Default fork server"
                if fs_key is None
                else "Fork server for runtime_env"
            )
            (logger or self._logger).error(
                "%s failed %d consecutive times; disabling respawn and falling back to execvpe.",
                label,
                failures,
            )
        return failures

    def _live_fork_server_count(self) -> int:
        default_live = int(
            self._default_fork_server is not None
            and self._default_fork_server.is_alive()
        )
        return default_live + sum(
            1 for srv in self._fork_servers.values() if srv.is_alive()
        )

    def _make_fork_server_slot(self, logger: Optional[logging.Logger] = None) -> bool:
        if self._max_active_fork_servers <= 0:
            return False
        for key, srv in list(self._fork_servers.items()):
            if srv.is_alive():
                continue
            self._fork_servers.pop(key, None)
            self._cancel_idle_timer(key)
            if not srv._stopped:
                self._refresh_healthy_uptime(key, srv)
                if self._has_active_fork_server_refs(key):
                    self._record_fork_server_failure(key)
                else:
                    self._fork_server_failures.pop(key, None)
            srv.stop()

        while (
            self._live_fork_server_count() >= self._max_active_fork_servers
            and self._idle_fork_servers
        ):
            self._evict_idle_fork_server(next(iter(self._idle_fork_servers)))

        live_count = self._live_fork_server_count()
        if live_count >= self._max_active_fork_servers:
            now = time.monotonic()
            if now - self._fork_server_cap_warn_at >= 10.0:
                self._fork_server_cap_warn_at = now
                (logger or self._logger).warning(
                    "Active fork server count (%d) reached max_active_fork_servers "
                    "(%d); bypassing fork server and using execvpe fallback.",
                    live_count,
                    self._max_active_fork_servers,
                )
            return False
        return True

    def _start_fork_server(
        self,
        fs_key: Optional[str],
        runtime_env: Optional[RuntimeEnv] = None,
        context: Optional[RuntimeEnvContext] = None,
        logger: Optional[logging.Logger] = None,
    ) -> Optional[ForkServerProcess]:
        log = logger or self._logger
        if fs_key is None:
            if self._default_fork_server is not None:
                self._default_fork_server.stop()
                self._default_fork_server = None
        else:
            self._cancel_idle_timer(fs_key)
            existing = self._fork_servers.pop(fs_key, None)
            if existing is not None:
                existing.stop()

        socket_path = self._fork_server_socket_path(fs_key)
        try:
            fs_context = None
            if context is not None:
                # Recompute only plugin PYTHONPATH entries on a clean context so
                # per-worker runtime_env["env_vars"] never enter the shared server.
                pypath_ctx = RuntimeEnvContext()
                if runtime_env is not None:
                    for plugin in (self._working_dir_plugin, self._py_modules_plugin):
                        if (
                            plugin.name in runtime_env
                            and runtime_env[plugin.name] is not None
                        ):
                            plugin.modify_context(
                                plugin.get_uris(runtime_env),
                                runtime_env,
                                pypath_ctx,
                                log,
                            )
                fs_context = RuntimeEnvContext(
                    command_prefix=list(context.command_prefix),
                    env_vars=pypath_ctx.env_vars,
                    py_executable=context.py_executable,
                    override_worker_entrypoint=context.override_worker_entrypoint,
                    java_jars=list(context.java_jars),
                )
            server = ForkServerProcess(
                socket_path=socket_path,
                preload_modules=runtime_env.preload_modules() if runtime_env else None,
                context=fs_context,
                cgroup_path=self._workers_cgroup_path or None,
            )
            if fs_key is None:
                self._default_fork_server = server
            else:
                self._fork_servers[fs_key] = server
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

    async def _supervise_default_fork_server(self) -> None:
        while not self._shutting_down:
            server = self._default_fork_server
            if server is not None:
                if server.ready_at is None:
                    try:
                        await server.wait_ready()
                        continue
                    except Exception as e:
                        if self._shutting_down or server._stopped:
                            continue
                        self._logger.warning(
                            "Default fork server failed readiness: %s", e
                        )
                elif server.is_alive():
                    self._refresh_healthy_uptime(None, server)
                    await asyncio.sleep(self._fork_server_monitor_interval_s)
                    continue
                elif not server._stopped:
                    self._refresh_healthy_uptime(None, server)
                    self._logger.warning(
                        "Default fork server (pid=%s) exited unexpectedly with code %s.",
                        server.process.pid,
                        server.process.returncode,
                    )
                if self._default_fork_server is server:
                    self._default_fork_server = None
                was_stopped = server._stopped
                server.stop()
                if not was_stopped:
                    self._record_fork_server_failure(None)

            failures = self._fork_server_failures[None]
            if failures >= self._fork_server_max_restarts:
                return
            if failures > 0:
                backoff_s = min(
                    self._fork_server_respawn_backoff_max_s,
                    self._fork_server_respawn_backoff_base_s * (2 ** (failures - 1)),
                )
                await asyncio.sleep(backoff_s)
                if self._shutting_down:
                    return
            if not self._make_fork_server_slot(self._logger):
                await asyncio.sleep(self._fork_server_monitor_interval_s)
                continue
            if self._start_fork_server(None) is None:
                self._record_fork_server_failure(None)

    def _on_fork_server_became_idle(self, fs_key: str) -> None:
        if self._has_active_fork_server_refs(fs_key):
            return
        if self._fork_server_max_idle_envs <= 0 or self._fork_server_idle_ttl_s <= 0:
            self._evict_idle_fork_server(fs_key)
            return
        self._cancel_idle_timer(fs_key)
        loop = get_or_create_event_loop()
        self._idle_fork_servers[fs_key] = loop.call_later(
            self._fork_server_idle_ttl_s,
            self._evict_idle_fork_server,
            fs_key,
        )
        while len(self._idle_fork_servers) > self._fork_server_max_idle_envs:
            self._evict_idle_fork_server(next(iter(self._idle_fork_servers)))

    async def _resolve_fork_server_socket(
        self,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        expect_active_ref: bool,
        logger: logging.Logger,
    ) -> Optional[str]:
        if not self._should_start_fork_server(runtime_env, context):
            return None

        fs_key = self._fork_server_cache_key(runtime_env)
        if fs_key is None:
            server = self._default_fork_server
            if server is None or not server.is_alive():
                return None
            if server.ready_at is None and not os.path.exists(server.socket_path):
                try:
                    await server.wait_ready()
                except Exception:
                    return None
            return server.socket_path if server.is_alive() else None

        self._cancel_idle_timer(fs_key)
        async with self._fork_server_locks[fs_key]:
            self._cancel_idle_timer(fs_key)
            server = self._fork_servers.get(fs_key)
            if server is not None and not (
                server.is_alive() and os.path.exists(server.socket_path)
            ):
                self._fork_servers.pop(fs_key, None)
                if not server._stopped:
                    self._refresh_healthy_uptime(fs_key, server)
                    failures = self._record_fork_server_failure(fs_key, logger)
                    logger.warning(
                        "Fork server for runtime_env exited unexpectedly (code=%s, "
                        "failures=%d); attempting lazy respawn.",
                        server.process.returncode,
                        failures,
                    )
                server.stop()
                server = None

            if server is None:
                if (
                    (
                        expect_active_ref
                        and not self._has_active_fork_server_refs(fs_key)
                    )
                    or self._fork_server_failures[fs_key]
                    >= self._fork_server_max_restarts
                    or not self._make_fork_server_slot(logger)
                ):
                    return None
                server = self._start_fork_server(fs_key, runtime_env, context, logger)
                if server is None:
                    self._record_fork_server_failure(fs_key, logger)
                    return None
                try:
                    await server.wait_ready()
                except BaseException as e:
                    was_stopped = server._stopped
                    if self._fork_servers.get(fs_key) is server:
                        self._fork_servers.pop(fs_key, None)
                    server.stop()
                    if not isinstance(e, Exception):
                        raise
                    if not was_stopped:
                        self._record_fork_server_failure(fs_key, logger)
                        logger.warning(
                            "Failed to wait for fork server, falling back to exec: %s",
                            e,
                        )
                    return None
                if not server.is_alive():
                    was_stopped = server._stopped
                    if self._fork_servers.get(fs_key) is server:
                        self._fork_servers.pop(fs_key, None)
                    server.stop()
                    if not was_stopped:
                        self._record_fork_server_failure(fs_key, logger)
                    return None
            else:
                self._refresh_healthy_uptime(fs_key, server)

            if expect_active_ref and not self._has_active_fork_server_refs(fs_key):
                self._on_fork_server_became_idle(fs_key)
                if fs_key not in self._fork_servers:
                    return None
            return server.socket_path

    def _evict_idle_fork_server(self, fs_key: str) -> None:
        self._cancel_idle_timer(fs_key)
        if self._has_active_fork_server_refs(fs_key):
            return
        server = self._fork_servers.pop(fs_key, None)
        if server is not None:
            server.stop()
        self._fork_server_failures.pop(fs_key, None)

    def _cleanup_fork_servers(self) -> None:
        self._shutting_down = True
        if self._default_fs_monitor_task is not None:
            self._default_fs_monitor_task.cancel()
            self._default_fs_monitor_task = None
        while self._idle_fork_servers:
            _, handle = self._idle_fork_servers.popitem()
            handle.cancel()
        if self._default_fork_server is not None:
            self._default_fork_server.stop()
            self._default_fork_server = None
        while self._fork_servers:
            _, server = self._fork_servers.popitem()
            server.stop()

    def uris_parser(self, runtime_env: RuntimeEnv):
        result = list()
        for name, plugin_setup_context in self._plugin_manager.plugins.items():
            plugin = plugin_setup_context.class_instance
            uris = plugin.get_uris(runtime_env)
            for uri in uris:
                result.append((uri, UriType(name)))
        return result

    def unused_uris_processor(self, unused_uris: List[Tuple[str, UriType]]) -> None:
        for uri, uri_type in unused_uris:
            self._plugin_manager.plugins[str(uri_type)].uri_cache.mark_unused(uri)

    def unused_runtime_env_processor(self, unused_runtime_env: str) -> None:
        if self._enable_worker_fork_server:
            fs_key = self._fork_server_cache_key(
                RuntimeEnv.deserialize(unused_runtime_env)
            )
            if fs_key in self._fork_servers:
                self._on_fork_server_became_idle(fs_key)
            elif fs_key is not None and not self._has_active_fork_server_refs(fs_key):
                self._fork_server_failures.pop(fs_key, None)

        def delete_runtime_env():
            del self._env_cache[unused_runtime_env]
            self._logger.info(
                "Runtime env %s removed from env-level cache.",
                redact_serialized_runtime_env(unused_runtime_env),
            )

        if unused_runtime_env in self._env_cache:
            if not self._env_cache[unused_runtime_env].success:
                loop = get_or_create_event_loop()
                # Cache the bad runtime env result by ttl seconds.
                loop.call_later(
                    runtime_env_consts.BAD_RUNTIME_ENV_CACHE_TTL_SECONDS,
                    delete_runtime_env,
                )
            else:
                delete_runtime_env()

    async def GetOrCreateRuntimeEnv(self, request):
        # Log only this copy: the serialized env carries `env_vars` values.
        redacted_serialized_env = redact_serialized_runtime_env(
            request.serialized_runtime_env
        )
        self._logger.debug(
            f"Got request from {request.source_process} to increase "
            f"reference for runtime env: {redacted_serialized_env}."
        )

        async def _setup_runtime_env(
            runtime_env: RuntimeEnv,
            runtime_env_config: RuntimeEnvConfig,
        ):
            log_files = runtime_env_config.get("log_files", [])
            # Use a separate logger scoped to this setup. Its file handles are
            # released when the setup exits (on success, failure, timeout, and
            # cancellation alike), not kept for the lifetime of the job.
            with self._setup_logger_factory.setup_logger(
                request.job_id.decode(), log_files
            ) as per_job_logger:
                context = RuntimeEnvContext(env_vars=dict(runtime_env.env_vars()))

                # Warn about unrecognized fields in the runtime env.
                for name, _ in runtime_env.plugins():
                    if name not in self._plugin_manager.plugins:
                        per_job_logger.warning(
                            f"runtime_env field {name} is not recognized by "
                            "Ray and will be ignored.  In the future, unrecognized "
                            "fields in the runtime_env will raise an exception."
                        )

                # Creates each runtime env URI by their priority. `working_dir` is
                # special because it needs to be created before other plugins. All
                # other plugins are created in the priority order (smaller priority
                # value -> earlier to create), with a special environment variable
                # being set to the working dir.
                # ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}

                # First create working dir...
                working_dir_ctx = self._plugin_manager.plugins[WorkingDirPlugin.name]
                await create_for_plugin_if_needed(
                    runtime_env,
                    working_dir_ctx.class_instance,
                    working_dir_ctx.uri_cache,
                    context,
                    per_job_logger,
                )

                # Then within the working dir, create the other plugins.
                working_dir_uri_or_none = runtime_env.working_dir_uri()
                with self._working_dir_plugin.with_working_dir_env(
                    working_dir_uri_or_none
                ):
                    """Run setup for each plugin unless it has already been cached."""
                    for (
                        plugin_setup_context
                    ) in self._plugin_manager.sorted_plugin_setup_contexts():
                        plugin = plugin_setup_context.class_instance
                        if plugin.name != WorkingDirPlugin.name:
                            uri_cache = plugin_setup_context.uri_cache
                            await create_for_plugin_if_needed(
                                runtime_env, plugin, uri_cache, context, per_job_logger
                            )

                expect_active_ref = (
                    request.source_process
                    not in self._reference_table._reference_exclude_sources
                )
                context.fork_server_socket_path = (
                    await self._resolve_fork_server_socket(
                        runtime_env, context, expect_active_ref, per_job_logger
                    )
                )

                return context

        async def _create_runtime_env_with_retry(
            runtime_env: RuntimeEnv,
            setup_timeout_seconds: int,
            runtime_env_config: RuntimeEnvConfig,
        ) -> Tuple[bool, str, str]:
            """Create runtime env with retry times. This function won't raise exceptions.

            Args:
                runtime_env: The instance of RuntimeEnv class.
                setup_timeout_seconds: The timeout of runtime environment creation for
                    each attempt.
                runtime_env_config: The configuration for the runtime environment.

            Returns:
                Tuple[bool, str, str]: A tuple containing:
                    - result (bool): Whether the creation was successful
                    - runtime_env_context (str): The serialized context if successful, None otherwise
                    - error_message (str): Error message if failed, None otherwise
            """
            self._logger.info(
                f"Creating runtime env: {redacted_serialized_env} "
                f"with timeout "
                f"{setup_timeout_seconds} seconds."
            )
            num_retries = runtime_env_consts.RUNTIME_ENV_RETRY_TIMES
            error_message = None
            serialized_context = None
            for i in range(num_retries):
                # Only sleep when retrying.
                if i != 0:
                    await asyncio.sleep(
                        runtime_env_consts.RUNTIME_ENV_RETRY_INTERVAL_MS / 1000
                    )

                try:
                    runtime_env_setup_task = _setup_runtime_env(
                        runtime_env, runtime_env_config
                    )
                    runtime_env_context = await asyncio.wait_for(
                        runtime_env_setup_task, timeout=setup_timeout_seconds
                    )
                    serialized_context = runtime_env_context.serialize()
                    error_message = None
                    break
                except Exception as e:
                    err_msg = (
                        "Failed to create runtime env " f"{redacted_serialized_env}."
                    )
                    self._logger.exception(err_msg)
                    error_message = "".join(
                        traceback.format_exception(type(e), e, e.__traceback__)
                    )
                    if isinstance(e, asyncio.TimeoutError):
                        hint = (
                            f"Failed to install runtime_env within the "
                            f"timeout of {setup_timeout_seconds} seconds. Consider "
                            "increasing the timeout in the runtime_env config. "
                            "For example: \n"
                            '    runtime_env={"config": {"setup_timeout_seconds":'
                            " 1800}, ...}\n"
                            "If not provided, the default timeout is "
                            f"{DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS} seconds. "
                        )
                        error_message = hint + error_message

            if error_message:
                self._logger.error(
                    "runtime_env creation failed %d times, giving up.",
                    num_retries,
                )
                return False, None, error_message
            else:
                self._logger.info(
                    "Successfully created runtime env: %s, context: %s",
                    redacted_serialized_env,
                    redact_serialized_runtime_env(serialized_context),
                )
                return True, serialized_context, None

        try:
            serialized_env = request.serialized_runtime_env
            runtime_env = RuntimeEnv.deserialize(serialized_env)
        except Exception as e:
            self._logger.exception(
                f"[Increase] Failed to parse runtime env: {redacted_serialized_env}"
            )

            error_message = "".join(
                traceback.format_exception(type(e), e, e.__traceback__)
            )

            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=runtime_env_agent_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=f"{self._node_prefix}{error_message}",
            )

        # Increase reference
        self._reference_table.increase_reference(
            runtime_env, serialized_env, request.source_process
        )
        if self._enable_worker_fork_server:
            fs_key = self._fork_server_cache_key(runtime_env)
            if fs_key is not None:
                self._cancel_idle_timer(fs_key)

        if serialized_env not in self._env_locks:
            # async lock to prevent the same env being concurrently installed
            self._env_locks[serialized_env] = asyncio.Lock()

        async with self._env_locks[serialized_env]:
            if serialized_env in self._env_cache:
                result = self._env_cache[serialized_env]
                if result.success:
                    context_str = result.result
                    if self._enable_worker_fork_server:
                        cached_ctx = RuntimeEnvContext.deserialize(context_str)
                        expect_active_ref = (
                            request.source_process
                            not in self._reference_table._reference_exclude_sources
                        )
                        cached_ctx.fork_server_socket_path = (
                            await self._resolve_fork_server_socket(
                                runtime_env, cached_ctx, expect_active_ref, self._logger
                            )
                        )
                        updated_context_str = cached_ctx.serialize()
                        if (
                            updated_context_str != context_str
                            and self._env_cache.get(serialized_env) is result
                        ):
                            self._env_cache[serialized_env] = CreatedEnvResult(
                                True, updated_context_str, result.creation_time_ms
                            )
                        context_str = updated_context_str
                    self._logger.info(
                        "Runtime env already created "
                        f"successfully. Env: {redacted_serialized_env}, "
                        f"context: {redact_serialized_runtime_env(context_str)}"
                    )
                    return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                        status=runtime_env_agent_pb2.AGENT_RPC_STATUS_OK,
                        serialized_runtime_env_context=context_str,
                    )
                else:
                    error_message = result.result
                    self._logger.info(
                        "Runtime env already failed. "
                        f"Env: {redacted_serialized_env}, "
                        f"err: {error_message}"
                    )
                    # Recover the reference.
                    self._reference_table.decrease_reference(
                        runtime_env, serialized_env, request.source_process
                    )
                    return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                        status=runtime_env_agent_pb2.AGENT_RPC_STATUS_FAILED,
                        error_message=f"{self._node_prefix}{error_message}",
                    )

            if SLEEP_FOR_TESTING_S:
                self._logger.info(f"Sleeping for {SLEEP_FOR_TESTING_S}s.")
                time.sleep(int(SLEEP_FOR_TESTING_S))

            runtime_env_config = RuntimeEnvConfig.from_proto(request.runtime_env_config)

            # accroding to the document of `asyncio.wait_for`,
            # None means disable timeout logic
            setup_timeout_seconds = (
                None
                if runtime_env_config["setup_timeout_seconds"] == -1
                else runtime_env_config["setup_timeout_seconds"]
            )

            start = time.perf_counter()
            (
                successful,
                serialized_context,
                error_message,
            ) = await _create_runtime_env_with_retry(
                runtime_env,
                setup_timeout_seconds,
                runtime_env_config,
            )
            creation_time_ms = int(round((time.perf_counter() - start) * 1000, 0))
            if not successful:
                # Recover the reference.
                self._reference_table.decrease_reference(
                    runtime_env, serialized_env, request.source_process
                )
            # Add the result to env cache.
            self._env_cache[serialized_env] = CreatedEnvResult(
                successful,
                serialized_context if successful else error_message,
                creation_time_ms,
            )
            # Reply the RPC
            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=runtime_env_agent_pb2.AGENT_RPC_STATUS_OK
                if successful
                else runtime_env_agent_pb2.AGENT_RPC_STATUS_FAILED,
                serialized_runtime_env_context=serialized_context,
                error_message=f"{self._node_prefix}{error_message}"
                if not successful
                else "",
            )

    async def DeleteRuntimeEnvIfPossible(self, request):
        # Log only this copy: the serialized env carries `env_vars` values.
        redacted_serialized_env = redact_serialized_runtime_env(
            request.serialized_runtime_env
        )
        self._logger.info(
            f"Got request from {request.source_process} to decrease "
            f"reference for runtime env: {redacted_serialized_env}."
        )

        try:
            runtime_env = RuntimeEnv.deserialize(request.serialized_runtime_env)
        except Exception as e:
            self._logger.exception(
                f"[Decrease] Failed to parse runtime env: {redacted_serialized_env}"
            )

            error_message = "".join(
                traceback.format_exception(type(e), e, e.__traceback__)
            )

            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=runtime_env_agent_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=f"{self._node_prefix}{error_message}",
            )

        try:
            self._reference_table.decrease_reference(
                runtime_env, request.serialized_runtime_env, request.source_process
            )
        except Exception as e:
            return runtime_env_agent_pb2.DeleteRuntimeEnvIfPossibleReply(
                status=runtime_env_agent_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=f"{self._node_prefix}Failed to decrement reference for runtime env for {str(e)}",
            )

        return runtime_env_agent_pb2.DeleteRuntimeEnvIfPossibleReply(
            status=runtime_env_agent_pb2.AGENT_RPC_STATUS_OK
        )

    async def GetRuntimeEnvsInfo(self, request):
        """Return the runtime env information of the node."""
        # TODO(sang): Currently, it only includes runtime_env information.
        # We should include the URI information which includes,
        # URIs
        # Caller
        # Ref counts
        # Cache information
        # Metrics (creation time & success)
        # Deleted URIs
        limit = request.limit if request.HasField("limit") else -1
        runtime_env_states = defaultdict(ProtoRuntimeEnvState)
        runtime_env_refs = self._reference_table.runtime_env_refs
        for runtime_env, ref_cnt in runtime_env_refs.items():
            runtime_env_states[runtime_env].runtime_env = runtime_env
            runtime_env_states[runtime_env].ref_cnt = ref_cnt
        for runtime_env, result in self._env_cache.items():
            runtime_env_states[runtime_env].runtime_env = runtime_env
            runtime_env_states[runtime_env].success = result.success
            if not result.success:
                runtime_env_states[runtime_env].error = result.result
            runtime_env_states[runtime_env].creation_time_ms = result.creation_time_ms

        reply = runtime_env_agent_pb2.GetRuntimeEnvsInfoReply()
        count = 0
        for runtime_env_state in runtime_env_states.values():
            if limit != -1 and count >= limit:
                break
            count += 1
            reply.runtime_env_states.append(runtime_env_state)
        reply.total = len(runtime_env_states)
        return reply
