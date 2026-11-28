"""Pre-initialized Python worker fork server daemon and launcher."""

import argparse
import asyncio
import copy
import gc
import importlib
import importlib.metadata
import json
import logging
import os
import random
import select
import shlex
import signal
import socket
import subprocess
import sys
import time
import traceback
from contextlib import suppress
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, Sequence, Tuple

import ray
import ray._common.ray_constants as common_ray_constants
import ray._common.serialization as common_serialization
import ray._private.ray_constants as ray_constants
import ray._private.ray_logging as ray_logging
import ray._private.serialization as private_serialization
import ray._private.services as services
import ray._private.utils
import ray._private.worker
from ray._private.async_compat import try_install_uvloop

if TYPE_CHECKING:
    from ray._private.runtime_env.context import RuntimeEnvContext

logger = logging.getLogger(__name__)

_PRE_FORK_ENV_OVERRIDES = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "PYTORCH_NVML_BASED_CUDA_CHECK": "1",
    "TORCH_DEVICE_BACKEND_AUTOLOAD": "0",
}
_PRE_FORK_ENV_DEFAULTS = {
    "JE_ARROW_MALLOC_CONF": "background_thread:false",
    "_RJEM_MALLOC_CONF": "background_thread:false",
}

_MAX_FORK_REQUEST_BYTES = 4 * 1024 * 1024
_CHILD_SETUP_TIMEOUT_S = 5
# Omit POLLRDHUP so half-closed (SHUT_WR) clients are served; full close sets POLLHUP.
_PEER_DISCONNECTED_MASK = getattr(select, "POLLHUP", 0) | getattr(select, "POLLERR", 0)
_DEDUP_LOG_ENV_KEYS = (
    "RAY_DEDUP_LOGS",
    "RAY_DEDUP_LOGS_AGG_WINDOW_S",
    "RAY_DEDUP_LOGS_ALLOW_REGEX",
    "RAY_DEDUP_LOGS_SKIP_REGEX",
)
_worker_lifetime_pipe_w: Optional[int] = None
_at_fork_registered = False


def _close_worker_lifetime_pipe_in_grandchild() -> None:
    """Closes the worker lifetime pipe write end in forked grandchildren."""
    global _worker_lifetime_pipe_w
    fd = _worker_lifetime_pipe_w
    _worker_lifetime_pipe_w = None
    if fd is not None:
        with suppress(OSError):
            os.close(fd)


def _register_lifetime_pipe_at_fork_once() -> None:
    """Registers the grandchild at-fork lifetime pipe closer once per process."""
    global _at_fork_registered
    if not _at_fork_registered and hasattr(os, "register_at_fork"):
        os.register_at_fork(after_in_child=_close_worker_lifetime_pipe_in_grandchild)
        _at_fork_registered = True


def _is_lifeline_closed(fd: int) -> bool:
    """Returns True if the non-blocking lifeline pipe FD has reached EOF or errored."""
    try:
        return not os.read(fd, 64)
    except BlockingIOError:
        return False
    except OSError:
        return True


def _recv_line(conn: socket.socket, timeout_s: float = 2.0) -> bytes:
    """Reads a newline-terminated JSON request line with size and time caps.

    Returns empty bytes if the peer closes before sending a newline.
    Raises TimeoutError if the deadline expires or ValueError if the line
    exceeds _MAX_FORK_REQUEST_BYTES.
    """
    deadline = time.monotonic() + timeout_s
    buffer = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Timed out reading fork request")
        conn.settimeout(remaining)
        chunk = conn.recv(4096)
        if not chunk:
            return b""
        newline_idx = chunk.find(b"\n")
        if newline_idx != -1:
            if len(buffer) + newline_idx + 1 > _MAX_FORK_REQUEST_BYTES:
                raise ValueError("Fork request exceeds maximum size limit")
            buffer.extend(chunk[: newline_idx + 1])
            return bytes(buffer)
        if len(buffer) + len(chunk) > _MAX_FORK_REQUEST_BYTES:
            raise ValueError("Fork request exceeds maximum size limit")
        buffer.extend(chunk)


def _validate_socket_path(socket_path: str) -> str:
    """Validates and normalizes a Unix domain socket path for a fork server."""
    if os.path.basename(socket_path) in ("", ".", ".."):
        raise OSError(f"Invalid fork server socket path: {socket_path!r}")
    norm_path = os.path.abspath(socket_path)
    ray._private.utils.validate_socket_filepath(norm_path)
    return norm_path


def _normalize_pythonpath(raw_pythonpath: str) -> str:
    if not raw_pythonpath:
        return ""
    return os.pathsep.join(
        os.path.abspath(p) if p and "$" not in p else p
        for p in raw_pythonpath.split(os.pathsep)
    )


def _extract_expected_cwd(context: Optional["RuntimeEnvContext"]) -> Optional[str]:
    """Extracts the working_dir path from a RuntimeEnvContext command_prefix."""
    if context is None:
        return None
    prefix = context.command_prefix
    for i in range(len(prefix) - 2):
        if prefix[i] == "cd" and prefix[i + 2] == "&&":
            try:
                unquoted = shlex.split(prefix[i + 1])
            except ValueError:
                return prefix[i + 1]
            return unquoted[0] if len(unquoted) == 1 else prefix[i + 1]
    return None


def _emit_startup_warning(msg: str) -> None:
    if logger.hasHandlers():
        logger.warning("%s", msg)
    print(msg, file=sys.stderr, flush=True)


def _redirect_post_preload_stdio(
    logs_dir: Optional[str],
    worker_stdout_fd: Optional[int],
    worker_stderr_fd: Optional[int],
) -> None:
    """Switches fork-server fd 1/2 from setup logs to raylet.out/err or container stdio."""
    with suppress(OSError):
        sys.stdout.flush()
    with suppress(OSError):
        sys.stderr.flush()
    if worker_stdout_fd is not None and worker_stderr_fd is not None:
        try:
            os.dup2(worker_stdout_fd, 1)
            os.dup2(worker_stderr_fd, 2)
        finally:
            for fd in {worker_stdout_fd, worker_stderr_fd}:
                if fd > 2:
                    with suppress(OSError):
                        os.close(fd)
        return
    if logs_dir:
        os.makedirs(logs_dir, exist_ok=True)
        for target_fd, filename in ((1, "raylet.out"), (2, "raylet.err")):
            path = os.path.join(logs_dir, filename)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            try:
                os.dup2(fd, target_fd)
            finally:
                if fd != target_fd:
                    os.close(fd)


def _bind_staging_socket(
    sock: socket.socket, socket_dir: str, tmp_name: str, tmp_socket_path: str
) -> None:
    try:
        sock.bind(tmp_socket_path)
    except OSError:
        if not sys.platform.startswith("linux"):
            raise
        dir_fd = os.open(socket_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            sock.bind(f"/proc/self/fd/{dir_fd}/{tmp_name}")
        finally:
            os.close(dir_fd)


class ForkServerProcess:
    """Manages a pre-initialized Python fork server process and its Unix socket."""

    def __init__(
        self,
        socket_path: str,
        preload_modules: Optional[List[str]] = None,
        context: Optional["RuntimeEnvContext"] = None,
        cgroup_path: Optional[str] = None,
        wait_ready: bool = True,
        timeout_s: float = 120.0,
        logs_dir: Optional[str] = None,
        setup_log_path: Optional[str] = None,
        worker_stdout_fd: Optional[int] = None,
        worker_stderr_fd: Optional[int] = None,
    ):
        self.socket_path = _validate_socket_path(socket_path)
        self.started_at = time.monotonic()
        self.ready_at: Optional[float] = None
        self._stopped = False
        self._socket_id: Optional[Tuple[int, int, int]] = None
        self.expected_cwd_path = _extract_expected_cwd(context)
        self.expected_cwd_dev_ino: Optional[Tuple[int, int]] = None
        if self.expected_cwd_path is not None:
            with suppress(OSError):
                st = os.stat(self.expected_cwd_path)
                self.expected_cwd_dev_ino = (st.st_dev, st.st_ino)

        os.makedirs(os.path.dirname(self.socket_path), exist_ok=True)
        Path(self.socket_path).unlink(missing_ok=True)

        workers_dir = os.path.dirname(os.path.abspath(__file__))
        cluster_pythonpath = _normalize_pythonpath(os.environ.get("PYTHONPATH", ""))
        fork_server_args = [
            os.path.abspath(__file__),
            f"--socket-path={self.socket_path}",
            f"--preload-modules={','.join(preload_modules or [])}",
            f"--cluster-pythonpath={cluster_pythonpath}",
            "--monitor-stdin",
        ]
        if logs_dir:
            fork_server_args.append(f"--logs-dir={logs_dir}")

        pass_fds: List[int] = []
        if worker_stdout_fd is not None and worker_stderr_fd is not None:
            fork_server_args.extend(
                [
                    f"--worker-stdout-fd={worker_stdout_fd}",
                    f"--worker-stderr-fd={worker_stderr_fd}",
                ]
            )
            pass_fds.extend([worker_stdout_fd, worker_stderr_fd])

        site_flags = services._site_flags()
        if context is not None:
            launch_ctx = copy.copy(context)
            launch_ctx.env_vars = dict(context.env_vars)
            if self.expected_cwd_path is None and "PYTHONPATH" in launch_ctx.env_vars:
                launch_ctx.env_vars["PYTHONPATH"] = _normalize_pythonpath(
                    launch_ctx.env_vars["PYTHONPATH"]
                )
            if site_flags and context.py_executable == sys.executable:
                launch_ctx.py_executable = " ".join(
                    [shlex.quote(sys.executable), *site_flags]
                )
            cmd = [
                sys.executable,
                *site_flags,
                os.path.join(workers_dir, "setup_worker.py"),
                f"--serialized-runtime-env-context={launch_ctx.serialize()}",
                "--language=PYTHON",
                *fork_server_args,
            ]
        else:
            cmd = [sys.executable, *site_flags, *fork_server_args]

        if cgroup_path and sys.platform.startswith("linux"):
            cmd = [
                sys.executable,
                "-S",
                os.path.join(workers_dir, "_cgroup_bootstrap.py"),
                cgroup_path,
                *cmd,
            ]

        # Scrub agent-inherited internal/allocator vars unless explicitly requested.
        popen_env = os.environ.copy()
        if cluster_pythonpath:
            popen_env["PYTHONPATH"] = cluster_pythonpath
        ctx_env_vars = context.env_vars if context is not None else {}
        for scrubbed_key in ("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", "MALLOC_CONF"):
            if scrubbed_key not in ctx_env_vars:
                popen_env.pop(scrubbed_key, None)

        # Redirect pre-fork bootstrap output so forked workers never inherit
        # RuntimeEnvAgent's PipeLogger pipes (which can deadlock agent teardown).
        self._setup_log_path: Optional[str] = setup_log_path or (
            os.path.join(logs_dir, "fork_server_default.log") if logs_dir else None
        )
        self._setup_log_offset = 0
        if self._setup_log_path is not None:
            os.makedirs(os.path.dirname(self._setup_log_path), exist_ok=True)
            log_fd = os.open(
                self._setup_log_path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o644,
            )
            with suppress(OSError):
                self._setup_log_offset = os.fstat(log_fd).st_size
        else:
            log_fd = os.open(os.devnull, os.O_WRONLY)

        try:
            self.process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=log_fd,
                stderr=log_fd,
                env=popen_env,
                close_fds=True,
                pass_fds=tuple(pass_fds),
                start_new_session=True,
            )
        finally:
            os.close(log_fd)

        if wait_ready:
            self.wait_ready(timeout_s=timeout_s)

    @property
    def stopped(self) -> bool:
        """Returns True if stop() has been called on this fork server."""
        return self._stopped

    def is_alive(self) -> bool:
        """Returns True if the fork server has not been stopped and is still running."""
        return not self._stopped and self.process.poll() is None

    def has_valid_cwd(self) -> bool:
        """Returns False if the fork server's working_dir was unlinked or replaced."""
        if not self.is_alive():
            return False
        if self.expected_cwd_path is not None:
            try:
                cur_st = os.stat(self.expected_cwd_path)
            except OSError:
                return False
            if (
                self.expected_cwd_dev_ino is not None
                and (cur_st.st_dev, cur_st.st_ino) != self.expected_cwd_dev_ino
            ):
                return False
        if sys.platform.startswith("linux"):
            proc_cwd = f"/proc/{self.process.pid}/cwd"
            try:
                if os.readlink(proc_cwd).endswith(" (deleted)"):
                    return False
                if self.expected_cwd_dev_ino is not None:
                    proc_st = os.stat(proc_cwd)
                    if (proc_st.st_dev, proc_st.st_ino) != self.expected_cwd_dev_ino:
                        return False
            except OSError:
                return False
        return True

    def read_startup_warnings(self) -> List[str]:
        """Returns new preload/startup warning lines emitted to the setup log."""
        if self._setup_log_path is None:
            return []
        try:
            with open(
                self._setup_log_path, "r", encoding="utf-8", errors="replace"
            ) as f:
                f.seek(self._setup_log_offset)
                data = f.read()
                self._setup_log_offset = f.tell()
        except OSError:
            return []
        warnings_found = [
            stripped
            for line in data.splitlines()
            if (
                "ForkServer failed to preload module" in (stripped := line.strip())
                or "ForkServer refusing to start" in stripped
            )
        ]
        return list(dict.fromkeys(warnings_found))

    def _record_ready_state(self) -> None:
        st = os.lstat(self.socket_path)
        self._socket_id = (st.st_dev, st.st_ino, st.st_ctime_ns)
        if self.ready_at is None:
            self.ready_at = time.monotonic()
        if (
            self.expected_cwd_dev_ino is None
            and sys.platform.startswith("linux")
            and self.is_alive()
        ):
            with suppress(OSError):
                proc_st = os.stat(f"/proc/{self.process.pid}/cwd")
                self.expected_cwd_dev_ino = (proc_st.st_dev, proc_st.st_ino)

    def wait_ready(self, timeout_s: float = 120.0) -> None:
        try:
            deadline = time.monotonic() + timeout_s
            while not os.path.exists(self.socket_path):
                if self._stopped or self.process.poll() is not None:
                    raise RuntimeError(
                        f"ForkServer exited prematurely with code {self.process.returncode}"
                    )
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Timed out waiting for ForkServer socket at {self.socket_path}"
                    )
                time.sleep(0.01)
            if self._stopped or self.process.poll() is not None:
                raise RuntimeError(
                    f"ForkServer exited prematurely with code {self.process.returncode}"
                )
            self._record_ready_state()
        except BaseException as e:
            if isinstance(e, Exception):
                logger.warning(
                    "ForkServer failed to start at %s: %s", self.socket_path, e
                )
            self.stop()
            raise

    async def wait_ready_async(self, timeout_s: Optional[float] = None) -> None:
        """Asynchronously waits until the fork server socket is published.

        Unlike wait_ready(), does not call stop() on timeout or failure so callers
        can inspect warnings or let a shared default server finish preloading.
        """
        if timeout_s is None:
            timeout_s = float(
                os.environ.get("RAY_fork_server_ready_timeout_s", "120.0")
            )
        deadline = time.monotonic() + timeout_s
        while not os.path.exists(self.socket_path):
            if self._stopped or self.process.poll() is not None:
                raise RuntimeError(
                    f"ForkServer exited prematurely with code {self.process.returncode}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for ForkServer socket at {self.socket_path}"
                )
            await asyncio.sleep(0.01)
        if self._stopped or self.process.poll() is not None:
            raise RuntimeError(
                f"ForkServer exited prematurely with code {self.process.returncode}"
            )
        self._record_ready_state()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self.process.stdin is not None:
            try:
                self.process.stdin.close()
            except OSError as e:
                logger.debug("Failed to close ForkServer stdin: %s", e)
        had_socket_id = self._socket_id is not None
        if self._socket_id is not None:
            try:
                st = os.lstat(self.socket_path)
                if (st.st_dev, st.st_ino, st.st_ctime_ns) == self._socket_id:
                    os.unlink(self.socket_path)
            except FileNotFoundError:
                pass
            except OSError as e:
                logger.warning(
                    "Failed to unlink ForkServer socket %s: %s", self.socket_path, e
                )
            self._socket_id = None
        for sig in (signal.SIGTERM, signal.SIGKILL):
            if self.process.poll() is not None:
                break
            try:
                os.killpg(self.process.pid, sig)
                self.process.wait(timeout=1.0)
            except ProcessLookupError:
                break
            except subprocess.TimeoutExpired:
                log_fn = logger.debug if sig == signal.SIGTERM else logger.warning
                log_fn(
                    "ForkServer pid=%s did not exit after %s",
                    self.process.pid,
                    sig.name,
                )
            except OSError as e:
                logger.warning(
                    "Failed to stop ForkServer pid=%s with %s: %s",
                    self.process.pid,
                    sig.name,
                    e,
                )
        if not had_socket_id:
            # The process may have published socket_path before _record_ready_state()
            # ran (e.g. crashed or required SIGKILL during startup). Safe to unlink
            # once exited because callers complete stop() before starting replacements.
            with suppress(OSError):
                os.unlink(self.socket_path)


def _preload_modules(preload_arg: str) -> None:
    raw_items = (os.environ.get("RAY_preload_python_modules", ""), preload_arg)
    modules = list(
        dict.fromkeys(
            m.strip() for raw in raw_items for m in raw.split(",") if m.strip()
        )
    )
    for module in modules:
        try:
            importlib.import_module(module)
        except Exception as e:
            _emit_startup_warning(
                f"ForkServer failed to preload module '{module}': {e}"
            )


def _env_int(name: str, default: int = 0) -> int:
    with suppress(ValueError):
        if (v := int(os.environ.get(name) or 0)) > 0:
            return v
    return default


def _prepare_child_environment(
    saved_pre_fork_env: Mapping[str, Optional[str]],
    child_env: Dict[str, str],
    child_args: List[str],
    runtime_env_vars: Optional[Dict[str, str]] = None,
    plugin_py_dirs: Sequence[str] = (),
    cluster_py_dirs: Sequence[str] = (),
    entrypoint_file: Optional[str] = None,
) -> None:
    ray._private.worker.t0 = time.time()
    runtime_env_vars = runtime_env_vars or {}
    changed_keys = {*child_env, *runtime_env_vars}
    for key, val in saved_pre_fork_env.items():
        if val is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = val

    parent_pythonpath = os.environ.get("PYTHONPATH", "")
    old_py_dirs = [os.path.abspath(p) for p in parent_pythonpath.split(os.pathsep) if p]
    plugin_py_dirs = tuple(os.path.abspath(p) for p in plugin_py_dirs if p)
    cluster_py_dirs = tuple(os.path.abspath(p) for p in cluster_py_dirs if p)
    os.environ.pop("RAY_JOB_ID", None)
    os.environ.update(child_env)
    ray._private.utils.update_envs(runtime_env_vars)
    if "TZ" in changed_keys and hasattr(time, "tzset"):
        time.tzset()
    if "PYTHONPATH" in changed_keys:
        env_py_dirs = [
            os.path.abspath(p)
            for p in os.environ.get("PYTHONPATH", "").split(os.pathsep)
            if p
        ]
        merged_py_dirs = list(
            dict.fromkeys(
                [
                    *plugin_py_dirs,
                    *env_py_dirs,
                    *(cluster_py_dirs if plugin_py_dirs else ()),
                ]
            )
        )
        if merged_py_dirs:
            os.environ["PYTHONPATH"] = os.pathsep.join(merged_py_dirs)
        else:
            os.environ.pop("PYTHONPATH", None)
        if merged_py_dirs != list(dict.fromkeys(old_py_dirs)):
            ray._private.utils._cached_torch_backend_eps = None
            ray._private.worker.global_worker.serialization_context_map.clear()
            prefixes = (os.path.abspath(sys.prefix), os.path.abspath(sys.base_prefix))
            prefix_seps = tuple(p.rstrip(os.sep) + os.sep for p in prefixes)
            idx = 1 if sys.path else 0
            sys.path[idx:] = [
                p
                for p in sys.path[idx:]
                if (
                    os.path.abspath(p) not in old_py_dirs
                    or os.path.abspath(p) in prefixes
                    or os.path.abspath(p).startswith(prefix_seps)
                )
                and os.path.abspath(p) not in merged_py_dirs
            ]
            sys.path[idx:idx] = merged_py_dirs
            importlib.invalidate_caches()
    ray._private.utils._torch_backends_autoload_pending = (
        "torch" in sys.modules
        and os.environ.get("TORCH_DEVICE_BACKEND_AUTOLOAD") != "0"
    )

    omp_threads = _env_int("OMP_NUM_THREADS")
    if omp_threads > 0:
        ray._private.utils.set_thread_pool_module_threads(omp_threads)
    openblas_threads = _env_int("OPENBLAS_NUM_THREADS")
    mkl_threads = _env_int("MKL_NUM_THREADS")
    if openblas_threads > 0 or mkl_threads > 0:
        ray._private.utils.resize_native_thread_pools(0, openblas_threads, mkl_threads)

    if hasattr(sys.modules.get("torch.cuda"), "_cached_device_count"):
        sys.modules["torch.cuda"]._cached_device_count = None

    # Refresh C++ RayConfig and authentication token cache from updated os.environ.
    ray._raylet.Config.initialize("")
    ray._raylet.AuthenticationTokenLoader.instance().reset_cache()

    # Refresh import-time ray_constants and global_worker attributes that depend on os.environ.
    raylet_internal_keys = (
        "RAY_JOB_ID",
        "RAY_RAYLET_PID",
        "RAY_num_grpc_internal_threads",
        "RAY_start_python_gc_manager_thread",
    )
    if any(
        (k.startswith("RAY_") and k not in raylet_internal_keys)
        or k == "FUNCTION_SIZE_ERROR_THRESHOLD"
        for k in changed_keys
    ):
        importlib.reload(common_ray_constants)
        importlib.reload(ray_constants)
        if any(k in changed_keys for k in _DEDUP_LOG_ENV_KEYS):
            ray_logging.RAY_DEDUP_LOGS = ray_constants.RAY_DEDUP_LOGS
            ray_logging.RAY_DEDUP_LOGS_AGG_WINDOW_S = (
                ray_constants.RAY_DEDUP_LOGS_AGG_WINDOW_S
            )
            ray_logging.RAY_DEDUP_LOGS_ALLOW_REGEX = (
                ray_constants.RAY_DEDUP_LOGS_ALLOW_REGEX
            )
            ray_logging.RAY_DEDUP_LOGS_SKIP_REGEX = (
                ray_constants.RAY_DEDUP_LOGS_SKIP_REGEX
            )
            ray_logging.stdout_deduplicator = ray_logging.LogDeduplicator(
                ray_constants.RAY_DEDUP_LOGS_AGG_WINDOW_S,
                ray_constants.RAY_DEDUP_LOGS_ALLOW_REGEX,
                ray_constants.RAY_DEDUP_LOGS_SKIP_REGEX,
            )
            ray_logging.stderr_deduplicator = ray_logging.LogDeduplicator(
                ray_constants.RAY_DEDUP_LOGS_AGG_WINDOW_S,
                ray_constants.RAY_DEDUP_LOGS_ALLOW_REGEX,
                ray_constants.RAY_DEDUP_LOGS_SKIP_REGEX,
            )
            ray._private.worker.stdout_deduplicator = ray_logging.stdout_deduplicator
            ray._private.worker.stderr_deduplicator = ray_logging.stderr_deduplicator
        if "RAY_USE_UVLOOP" in changed_keys and not ray_constants.env_bool(
            "RAY_USE_UVLOOP", True
        ):
            asyncio.set_event_loop_policy(None)
    if "RAY_allow_out_of_band_object_ref_serialization" in changed_keys:
        oob_allowed = ray_constants.env_bool(
            "RAY_allow_out_of_band_object_ref_serialization", True
        )
        common_serialization.ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION = oob_allowed
        private_serialization.ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION = oob_allowed
    tempfile_mod = sys.modules.get("tempfile")
    if tempfile_mod is not None and changed_keys & {"TMPDIR", "TEMP", "TMP"}:
        tempfile_mod.tempdir = None
    gw = ray._private.worker.global_worker
    gw._original_visible_accelerator_ids = None
    gw._enable_record_actor_task_log = (
        ray_constants.RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING
    )
    ser_keys = (
        "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS",
        "RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION",
        "RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION",
    )
    if any(k in changed_keys for k in ser_keys):
        gw.serialization_context_map.clear()
        dispatch = ray.cloudpickle.CloudPickler.dispatch
        if not ray_constants.RAY_ENABLE_ZERO_COPY_TORCH_TENSORS:
            torch_mod = sys.modules.get("torch")
            if torch_mod is not None and hasattr(torch_mod, "Tensor"):
                dispatch.pop(torch_mod.Tensor, None)
        if os.environ.get("RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION", "0") == "1":
            pa_mod = sys.modules.get("pyarrow")
            if pa_mod is not None:
                for attr in ("Table", "Schema"):
                    cls = getattr(pa_mod, attr, None)
                    if cls is not None:
                        dispatch.pop(cls, None)
        if (
            os.environ.get("RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION", "0")
            == "1"
        ):
            pajson_mod = sys.modules.get("pyarrow.json")
            if pajson_mod is not None:
                for attr in ("ReadOptions", "ParseOptions"):
                    cls = getattr(pajson_mod, attr, None)
                    if cls is not None:
                        dispatch.pop(cls, None)

    random.seed()
    for mod_name in ("numpy.random", "torch"):
        mod = sys.modules.get(mod_name)
        if mod is not None and hasattr(mod, "seed"):
            mod.seed()

    sys.argv = [entrypoint_file or sys.argv[0], *child_args]


def _handle_forked_child(
    conn: socket.socket,
    server_sock: socket.socket,
    fds_to_close: Sequence[Optional[int]],
    saved_pre_fork_env: Mapping[str, Optional[str]],
    plugin_py_dirs: Sequence[str],
    cluster_py_dirs: Sequence[str],
    entrypoint_file: Optional[str] = None,
) -> None:
    """Reads the fork request in the child, prepares its env, and replies with its PID.

    Replies only after setup completes so any pre-entrypoint error closes the socket
    (EOF) and lets the raylet fall back to execvpe immediately. Exits via os._exit on
    error so the child never runs the parent server's cleanup handlers.
    """
    try:
        global _worker_lifetime_pipe_w
        with suppress(OSError):
            os.setpgrp()
        signal.set_wakeup_fd(-1)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT, signal.SIGALRM})
        signal.alarm(_CHILD_SETUP_TIMEOUT_S)

        server_sock.close()
        for fd_to_close in fds_to_close:
            if fd_to_close is not None:
                with suppress(OSError):
                    os.close(fd_to_close)

        line = _recv_line(conn, timeout_s=2.0)
        if not line:
            conn.close()
            os._exit(0)

        request = json.loads(line)
        if not isinstance(request, dict):
            conn.close()
            os._exit(1)
        child_args = request.get("args", [])
        child_env = request.get("env", {})
        runtime_env_vars = request.get("runtime_env_vars", {})
        if (
            not isinstance(child_args, list)
            or not all(isinstance(arg, str) for arg in child_args)
            or not isinstance(child_env, dict)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in child_env.items()
            )
            or not isinstance(runtime_env_vars, dict)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in runtime_env_vars.items()
            )
        ):
            conn.close()
            os._exit(1)

        _prepare_child_environment(
            saved_pre_fork_env,
            child_env,
            child_args,
            runtime_env_vars,
            plugin_py_dirs=plugin_py_dirs,
            cluster_py_dirs=cluster_py_dirs,
            entrypoint_file=entrypoint_file,
        )

        life_r, _worker_lifetime_pipe_w = os.pipe()
        try:
            conn.settimeout(1.0)
            reply = f'{{"pid":{os.getpid()}}}\n'.encode("ascii")
            sent = socket.send_fds(conn, [reply], [life_r])
            if sent < len(reply):
                conn.sendall(reply[sent:])
        finally:
            os.close(life_r)
            conn.close()
        signal.alarm(0)
    except BaseException:
        with suppress(Exception):
            traceback.print_exc()
        os._exit(1)


def _run_fork_server(argv: Optional[List[str]] = None) -> Optional[ModuleType]:
    parser = argparse.ArgumentParser(description="Ray Python worker fork server")
    parser.add_argument("--socket-path", required=True, type=str)
    parser.add_argument("--preload-modules", default="", type=str)
    parser.add_argument("--logs-dir", default=None, type=str)
    parser.add_argument("--cluster-pythonpath", default=None, type=str)
    parser.add_argument("--worker-stdout-fd", default=None, type=int)
    parser.add_argument("--worker-stderr-fd", default=None, type=int)
    parser.add_argument("--monitor-stdin", action="store_true")
    args = parser.parse_args(argv)

    raw_cluster_pythonpath = (
        os.environ.get("PYTHONPATH", "")
        if args.cluster_pythonpath is None
        else args.cluster_pythonpath
    )
    cluster_py_dirs = tuple(
        dict.fromkeys(
            os.path.abspath(p) for p in raw_cluster_pythonpath.split(os.pathsep) if p
        )
    )
    cluster_py_set = set(cluster_py_dirs)
    plugin_py_dirs = tuple(
        dict.fromkeys(
            os.path.abspath(p)
            for p in os.environ.get("PYTHONPATH", "").split(os.pathsep)
            if p and os.path.abspath(p) not in cluster_py_set
        )
    )

    lifeline_fd: Optional[int] = None
    if args.monitor_stdin:
        lifeline_fd = os.dup(0)
        os.set_inheritable(lifeline_fd, False)
        os.set_blocking(lifeline_fd, False)
        devnull_fd = os.open(os.devnull, os.O_RDONLY)
        try:
            os.dup2(devnull_fd, 0)
        finally:
            os.close(devnull_fd)

    for passed_fd in (args.worker_stdout_fd, args.worker_stderr_fd):
        if passed_fd is not None and passed_fd > 2:
            with suppress(OSError):
                os.set_inheritable(passed_fd, False)

    passed_stdio_fds = {
        fd for fd in (args.worker_stdout_fd, args.worker_stderr_fd) if fd is not None
    }

    def _close_unredirected_stdio_fds() -> None:
        for fd in list(passed_stdio_fds):
            if fd > 2:
                with suppress(OSError):
                    os.close(fd)
        passed_stdio_fds.clear()

    def _refuse_startup(msg: str, cause: Optional[BaseException] = None) -> None:
        _emit_startup_warning(msg)
        _close_unredirected_stdio_fds()
        if lifeline_fd is not None:
            with suppress(OSError):
                os.close(lifeline_fd)
        if cause is not None:
            raise SystemExit(1) from cause
        raise SystemExit(1)

    try:
        socket_path = _validate_socket_path(args.socket_path)
    except OSError as e:
        _refuse_startup(f"ForkServer refusing to start: {e}", cause=e)

    _register_lifetime_pipe_at_fork_once()

    # Keep library imports single-threaded before fork(). Preserve any explicit
    # exec-bound jemalloc config via setdefault() instead of overwriting it.
    saved_pre_fork_env = {
        k: os.environ.get(k)
        for k in (*_PRE_FORK_ENV_OVERRIDES, *_PRE_FORK_ENV_DEFAULTS)
    }
    os.environ.update(_PRE_FORK_ENV_OVERRIDES)
    for key, default_val in _PRE_FORK_ENV_DEFAULTS.items():
        os.environ.setdefault(key, default_val)

    import ray._common.usage.usage_lib as _usage_lib  # noqa: F401
    import ray._private.accelerators as _accelerators  # noqa: F401
    import ray._private.workers.default_worker as default_worker

    try_install_uvloop()
    try:
        ray._private.worker.global_worker.get_serialization_context()
    except Exception:
        logger.warning(
            "Failed to pre-initialize serialization context in ForkServer",
            exc_info=True,
        )
    _preload_modules(args.preload_modules)
    ray._private.utils.cache_native_thread_pool_setters()
    if "torch" in sys.modules:
        ray._private.utils._cached_torch_backend_eps = list(
            importlib.metadata.entry_points(group="torch.backends")
        )
    if getattr(sys.modules.get("torch.cuda"), "_initialized", False):
        _refuse_startup(
            "ForkServer refusing to start because a preloaded module initialized "
            "CUDA before fork(); falling back to spawning workers via execvpe."
        )
    if sys.platform.startswith("linux"):
        with suppress(OSError):
            if len(os.listdir("/proc/self/task")) > 1:
                _refuse_startup(
                    "ForkServer refusing to start because a preloaded module started "
                    "background threads before fork(); falling back to execvpe."
                )

    # Ignore SIGCHLD after preloading so preloaded modules calling subprocesses
    # still observe exit codes, while forked workers are auto-reaped by the kernel.
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    gc.collect()
    gc.freeze()

    socket_dir = os.path.dirname(socket_path)
    os.makedirs(socket_dir, exist_ok=True)
    if os.path.isdir(socket_path) and not os.path.islink(socket_path):
        _refuse_startup(
            f"ForkServer refusing to start: socket path is a directory: {socket_path}"
        )
    server_pid = os.getpid()
    tmp_name = f".f{server_pid}"
    tmp_socket_path = os.path.join(socket_dir, tmp_name)
    Path(tmp_socket_path).unlink(missing_ok=True)

    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    peer_poller = select.poll()
    wakeup_r: Optional[int] = None
    wakeup_w: Optional[int] = None
    published_stat: Optional[Tuple[int, int, int]] = None
    stop_requested = False

    def _handle_sigterm(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True

    try:
        try:
            wakeup_r, wakeup_w = os.pipe()
            os.set_blocking(wakeup_r, False)
            os.set_blocking(wakeup_w, False)
            signal.set_wakeup_fd(wakeup_w, warn_on_full_buffer=False)
            signal.signal(signal.SIGTERM, _handle_sigterm)

            _bind_staging_socket(server_sock, socket_dir, tmp_name, tmp_socket_path)
            os.chmod(tmp_socket_path, 0o600)
            server_sock.listen(128)
            server_sock.setblocking(False)
            if stop_requested:
                return None
            if lifeline_fd is not None and _is_lifeline_closed(lifeline_fd):
                return None

            _redirect_post_preload_stdio(
                args.logs_dir, args.worker_stdout_fd, args.worker_stderr_fd
            )
            if args.worker_stdout_fd is not None and args.worker_stderr_fd is not None:
                passed_stdio_fds.clear()
            else:
                _close_unredirected_stdio_fds()

            os.replace(tmp_socket_path, socket_path)
            with suppress(OSError):
                pub_stat = os.lstat(socket_path)
                published_stat = (
                    pub_stat.st_dev,
                    pub_stat.st_ino,
                    pub_stat.st_ctime_ns,
                )
        except Exception as e:
            _emit_startup_warning(
                f"ForkServer refusing to start because socket setup failed for "
                f"{socket_path!r}: {e}"
            )
            traceback.print_exc(file=sys.stderr)
            raise SystemExit(1) from e

        sys.stdout.flush()
        sys.stderr.flush()

        server_fd = server_sock.fileno()
        main_poller = select.poll()
        for fd in (server_fd, wakeup_r, lifeline_fd):
            if fd is not None:
                main_poller.register(
                    fd, select.POLLIN | select.POLLHUP | select.POLLERR
                )

        while not stop_requested:
            try:
                events = main_poller.poll()
            except InterruptedError:
                continue
            except (OSError, ValueError):
                break

            ready_fds = {fd for fd, _mask in events}
            if wakeup_r in ready_fds:
                with suppress(OSError):
                    os.read(wakeup_r, 64)
            if lifeline_fd is not None and lifeline_fd in ready_fds:
                if _is_lifeline_closed(lifeline_fd):
                    break
            if stop_requested or server_fd not in ready_fds:
                continue

            try:
                conn, _ = server_sock.accept()
            except (BlockingIOError, InterruptedError):
                continue
            except OSError as e:
                logger.warning("Fork server failed to accept a connection: %s", e)
                time.sleep(0.1)
                continue

            # Drop clients that already closed while queued.
            conn_fd = conn.fileno()
            peer_poller.register(conn_fd, _PEER_DISCONNECTED_MASK)
            try:
                peer_gone = bool(peer_poller.poll(0))
            finally:
                peer_poller.unregister(conn_fd)
            if peer_gone:
                conn.close()
                continue

            # Fork on accept() so slow clients don't stall the server or dirty CoW pages;
            # the child sends its PID and lifetime pipe after completing setup.
            try:
                pid = os.fork()
            except OSError as e:
                logger.warning("ForkServer os.fork() failed: %s", e)
                conn.close()
                continue

            if pid > 0:
                with suppress(OSError):
                    os.setpgid(pid, pid)
                conn.close()
                continue

            _handle_forked_child(
                conn=conn,
                server_sock=server_sock,
                fds_to_close=(wakeup_r, wakeup_w, lifeline_fd),
                saved_pre_fork_env=saved_pre_fork_env,
                plugin_py_dirs=plugin_py_dirs,
                cluster_py_dirs=cluster_py_dirs,
                entrypoint_file=default_worker.__file__,
            )
            return default_worker
    finally:
        if os.getpid() == server_pid:
            _close_unredirected_stdio_fds()
            with suppress(ValueError, OSError):
                signal.set_wakeup_fd(-1)
            with suppress(OSError):
                server_sock.close()
            for fd_to_close in (wakeup_r, wakeup_w, lifeline_fd):
                if fd_to_close is not None:
                    with suppress(OSError):
                        os.close(fd_to_close)
            with suppress(OSError):
                Path(tmp_socket_path).unlink(missing_ok=True)
            if published_stat is not None:
                with suppress(OSError):
                    cur_stat = os.lstat(socket_path)
                    if (
                        cur_stat.st_dev,
                        cur_stat.st_ino,
                        cur_stat.st_ctime_ns,
                    ) == published_stat:
                        os.unlink(socket_path)

    return None


if __name__ == "__main__":
    default_worker_mod = _run_fork_server()
    if default_worker_mod is not None:
        # Keep worker.main_loop() at top-level module scope (pull/12225).
        mode, worker = default_worker_mod.initialize_default_worker()
        if mode == ray.WORKER_MODE:
            worker.main_loop()
        elif mode in [ray.RESTORE_WORKER_MODE, ray.SPILL_WORKER_MODE]:
            while True:
                time.sleep(100000)
        else:
            raise ValueError(f"Unexpected worker mode: {mode}")
