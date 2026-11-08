import argparse
import asyncio
import copy
import gc
import importlib
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
from typing import IO, TYPE_CHECKING, Dict, List, Optional, Tuple

import ray
import ray._common.ray_constants as common_ray_constants
import ray._common.serialization as common_serialization
import ray._private.ray_constants as ray_constants
import ray._private.ray_logging as ray_logging
import ray._private.serialization as private_serialization
import ray._private.services as services
import ray._private.utils
from ray._private.async_compat import try_install_uvloop

if TYPE_CHECKING:
    from ray._private.runtime_env.context import RuntimeEnvContext

logger = logging.getLogger(__name__)

_MAX_FORK_REQUEST_BYTES = 4 * 1024 * 1024
_PEER_DISCONNECTED_MASK = (
    getattr(select, "POLLRDHUP", 0x2000) | select.POLLHUP | select.POLLERR
)
_WORKER_LIFETIME_PIPE_W: Optional[int] = None
_AT_FORK_REGISTERED = False


def _close_worker_lifetime_pipe_in_grandchild() -> None:
    global _WORKER_LIFETIME_PIPE_W
    fd = _WORKER_LIFETIME_PIPE_W
    _WORKER_LIFETIME_PIPE_W = None
    if fd is not None:
        with suppress(OSError):
            os.close(fd)


def _register_lifetime_pipe_at_fork_once() -> None:
    global _AT_FORK_REGISTERED
    if not _AT_FORK_REGISTERED and hasattr(os, "register_at_fork"):
        os.register_at_fork(after_in_child=_close_worker_lifetime_pipe_in_grandchild)
        _AT_FORK_REGISTERED = True


def _restore_or_unset_env(
    key: str,
    saved_val: Optional[str],
    child_env: Dict[str, str],
    runtime_env_vars: Dict[str, str],
) -> None:
    if key in child_env or key in runtime_env_vars:
        return
    if saved_val is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = saved_val


def _extract_expected_cwd_from_context(
    context: Optional["RuntimeEnvContext"],
) -> Optional[str]:
    if context is None:
        return None
    # WorkingDirPlugin appends ["cd", directory, "&&"] to command_prefix.
    prefix = context.command_prefix
    for i in range(len(prefix) - 2):
        if prefix[i] == "cd" and prefix[i + 2] == "&&":
            unquoted = shlex.split(prefix[i + 1])
            return unquoted[0] if len(unquoted) == 1 else prefix[i + 1]
    return None


def _normalize_pythonpath(raw_pythonpath: str) -> str:
    if not raw_pythonpath:
        return ""
    return os.pathsep.join(
        os.path.abspath(p) if p else p for p in raw_pythonpath.split(os.pathsep)
    )


class ForkServerProcess:
    """Manages a pre-initialized Python fork server process and its Unix socket."""

    def __init__(
        self,
        socket_path: str,
        preload_modules: Optional[List[str]] = None,
        context: Optional["RuntimeEnvContext"] = None,
        cgroup_path: Optional[str] = None,
        logs_dir: Optional[str] = None,
        setup_log_path: Optional[str] = None,
        worker_stdout_fd: Optional[int] = None,
        worker_stderr_fd: Optional[int] = None,
    ):
        self.socket_path = socket_path
        self.started_at = time.monotonic()
        self.ready_at: Optional[float] = None
        self._stopped = False
        self.expected_cwd_path: Optional[str] = _extract_expected_cwd_from_context(
            context
        )
        self.expected_cwd_dev_ino: Optional[Tuple[int, int]] = None
        if self.expected_cwd_path is not None:
            with suppress(OSError):
                st = os.stat(self.expected_cwd_path)
                self.expected_cwd_dev_ino = (st.st_dev, st.st_ino)

        os.makedirs(os.path.dirname(socket_path), exist_ok=True)
        Path(socket_path).unlink(missing_ok=True)

        workers_dir = os.path.dirname(os.path.abspath(__file__))
        cluster_pythonpath = _normalize_pythonpath(os.environ.get("PYTHONPATH", ""))
        fork_server_args = [
            os.path.abspath(__file__),
            f"--socket-path={socket_path}",
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
            if "PYTHONPATH" in launch_ctx.env_vars:
                launch_ctx.env_vars["PYTHONPATH"] = _normalize_pythonpath(
                    launch_ctx.env_vars["PYTHONPATH"]
                )
            if site_flags and context.py_executable == sys.executable:
                launch_ctx.py_executable = " ".join(
                    [shlex.quote(sys.executable), *site_flags]
                )
            setup_worker_path = os.path.join(workers_dir, "setup_worker.py")
            cmd = [
                sys.executable,
                *site_flags,
                setup_worker_path,
                f"--serialized-runtime-env-context={launch_ctx.serialize()}",
                "--language=PYTHON",
                *fork_server_args,
            ]
        else:
            cmd = [sys.executable, *site_flags, *fork_server_args]

        if cgroup_path and sys.platform.startswith("linux"):
            cgroup_bootstrap_path = os.path.join(workers_dir, "_cgroup_bootstrap.py")
            cmd = [
                sys.executable,
                "-S",
                cgroup_bootstrap_path,
                cgroup_path,
                *cmd,
            ]

        # Scrub agent-inherited internal/allocator vars unless explicitly part of
        # the fork server's resolved execution context.
        popen_env = os.environ.copy()
        if cluster_pythonpath:
            popen_env["PYTHONPATH"] = cluster_pythonpath
        ctx_env_vars = context.env_vars if context is not None else {}
        for scrubbed_key in ("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", "MALLOC_CONF"):
            if scrubbed_key not in ctx_env_vars:
                popen_env.pop(scrubbed_key, None)

        # Never inherit RuntimeEnvAgent's PipeLogger stdout/stderr (fd 1/2), which
        # forked workers would otherwise hold open via ScopedDup2Wrapper and deadlock
        # AgentManager teardown. Route pre-fork bootstrap output to setup_log_path.
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

        # Omit preexec_fn so CPython uses C-level _posixsubprocess / os.posix_spawn
        # without running Python code between fork and exec inside multi-threaded
        # RuntimeEnvAgent. Fate-sharing with the parent process uses stdin=PIPE +
        # --monitor-stdin instead of PR_SET_PDEATHSIG, which is bound to the
        # spawning thread on Linux and leaks .sock files on SIGKILL.
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
        self._lifeline: Optional[IO[bytes]] = self.process.stdin

    def is_alive(self) -> bool:
        return not self._stopped and self.process.poll() is None

    def has_valid_cwd(self) -> bool:
        """Return False if the fork server's working_dir was unlinked or replaced."""
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
                link_target = os.readlink(proc_cwd)
                if link_target.endswith(" (deleted)"):
                    return False
                if self.expected_cwd_dev_ino is not None:
                    proc_st = os.stat(proc_cwd)
                    if (proc_st.st_dev, proc_st.st_ino) != self.expected_cwd_dev_ino:
                        return False
            except OSError:
                return False
        return True

    def read_startup_warnings(self) -> List[str]:
        """Return ForkServer preload/startup warning lines emitted by this instance."""
        if self._setup_log_path is None:
            return []
        warnings_found: List[str] = []
        try:
            with open(
                self._setup_log_path, "r", encoding="utf-8", errors="replace"
            ) as f:
                f.seek(self._setup_log_offset)
                data = f.read()
                self._setup_log_offset = f.tell()
        except OSError:
            return []
        for line in data.splitlines():
            stripped = line.strip()
            if (
                "ForkServer failed to preload module" in stripped
                or "ForkServer refusing to start" in stripped
            ):
                warnings_found.append(stripped)
        return warnings_found

    async def wait_ready(self, timeout_s: Optional[float] = None) -> None:
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

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._lifeline is not None:
            with suppress(OSError):
                self._lifeline.close()
            self._lifeline = None
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
                self.process.wait(timeout=1.0)
            except Exception:
                with suppress(Exception):
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=1.0)
        Path(self.socket_path).unlink(missing_ok=True)


def _preload_modules(preload_arg: str) -> None:
    """Import preloaded modules before fork().

    Note: Per-job and per-worker `env_vars` are applied post-fork in each child worker
    so workers with distinct `env_vars` share the same warm fork server. Modules listed
    in `preload_modules` therefore execute their top-level import before per-worker
    `env_vars` are set.
    """
    raw_items = (os.environ.get("RAY_preload_python_modules", ""), preload_arg)
    modules = dict.fromkeys(
        mod.strip() for raw in raw_items for mod in raw.split(",") if mod.strip()
    )
    for mod in modules:
        try:
            importlib.import_module(mod)
        except Exception as e:
            msg = f"ForkServer failed to preload module '{mod}': {e}"
            logger.warning(msg)
            print(msg, file=sys.stderr, flush=True)


_RAYLET_INTERNAL_ENV_KEYS = frozenset(
    {
        "RAY_JOB_ID",
        "RAY_RAYLET_PID",
        "SPT_NOENV",
        "RAY_NODE_RPC_SERVER_THREAD_NUM",
        "GRPC_ENABLE_FORK_SUPPORT",
        "GRPC_POLL_STRATEGY",
        "RAY_start_python_gc_manager_thread",
    }
)

_SERIALIZATION_ENV_OVERRIDE_KEYS = (
    "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS",
    "RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION",
    "RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION",
)

_DEDUP_LOG_ENV_KEYS = (
    "RAY_DEDUP_LOGS",
    "RAY_DEDUP_LOGS_AGG_WINDOW_S",
    "RAY_DEDUP_LOGS_ALLOW_REGEX",
    "RAY_DEDUP_LOGS_SKIP_REGEX",
)


def _recv_line(conn: socket.socket) -> bytes:
    """Read a single newline-terminated JSON request line with a size cap."""
    first = conn.recv(4096)
    if not first:
        return b""
    nl = first.find(b"\n")
    if nl != -1:
        return first[: nl + 1]
    buf = bytearray(first)
    while True:
        if len(buf) > _MAX_FORK_REQUEST_BYTES:
            raise ValueError("Fork request exceeds maximum size limit")
        chunk = conn.recv(4096)
        if not chunk:
            return bytes(buf)
        nl = chunk.find(b"\n")
        if nl != -1:
            buf.extend(chunk[: nl + 1])
            return bytes(buf)
        buf.extend(chunk)


def _redirect_post_preload_stdio(
    logs_dir: Optional[str],
    worker_stdout_fd: Optional[int],
    worker_stderr_fd: Optional[int],
) -> None:
    """Switch fork-server fd 1/2 from setup logs to raylet.out/err or container stdio."""
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


def _clear_conditional_cloudpickle_reducers() -> None:
    dispatch = ray.cloudpickle.CloudPickler.dispatch
    torch_mod = sys.modules.get("torch")
    if torch_mod is not None and hasattr(torch_mod, "Tensor"):
        dispatch.pop(torch_mod.Tensor, None)
    pa_mod = sys.modules.get("pyarrow")
    if pa_mod is not None:
        for attr in ("Table", "Schema"):
            cls = getattr(pa_mod, attr, None)
            if cls is not None:
                dispatch.pop(cls, None)
    pajson_mod = sys.modules.get("pyarrow.json")
    if pajson_mod is not None:
        for attr in ("ReadOptions", "ParseOptions"):
            cls = getattr(pajson_mod, attr, None)
            if cls is not None:
                dispatch.pop(cls, None)


def _is_lifeline_closed(lifeline_fd: int) -> bool:
    """Return True if the parent write-end of the lifeline pipe reached EOF."""
    try:
        return os.read(lifeline_fd, 1) == b""
    except (BlockingIOError, InterruptedError):
        return False
    except OSError:
        return True


def _prepare_child_environment(
    saved_env_pins: Dict[str, Optional[str]],
    plugin_pypaths: List[str],
    cluster_pypaths: List[str],
    child_env: Dict[str, str],
    runtime_env_vars: Dict[str, str],
    child_args: List[str],
    entrypoint_file: str,
) -> None:
    ray._private.worker.t0 = time.time()

    for pinned_key, saved_val in saved_env_pins.items():
        _restore_or_unset_env(pinned_key, saved_val, child_env, runtime_env_vars)

    if "RAY_JOB_ID" not in child_env and "RAY_JOB_ID" not in runtime_env_vars:
        os.environ.pop("RAY_JOB_ID", None)
    # Apply raylet base environment first (literal key-values such as RAY_JOB_ID)
    # before expanding user runtime_env_vars so references like ${RAY_JOB_ID} resolve.
    os.environ.update(child_env)
    if runtime_env_vars:
        ray._private.utils.update_envs(runtime_env_vars)

    if ("TZ" in child_env or "TZ" in runtime_env_vars) and hasattr(time, "tzset"):
        time.tzset()

    if "PYTHONPATH" in child_env or "PYTHONPATH" in runtime_env_vars:
        cluster_set = set(cluster_pypaths)
        env_paths = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
        ordered_pypaths = list(
            dict.fromkeys(
                [
                    *plugin_pypaths,
                    *[p for p in env_paths if p not in cluster_set],
                    *cluster_pypaths,
                ]
            )
        )
        if "PYTHONPATH" in child_env and "PYTHONPATH" not in runtime_env_vars:
            os.environ["PYTHONPATH"] = os.pathsep.join(ordered_pypaths)
        ordered_set = set(ordered_pypaths)
        head = sys.path[:1]
        tail = [p for p in sys.path[1:] if p not in ordered_set]
        sys.path[:] = [*head, *ordered_pypaths, *tail]
        importlib.invalidate_caches()

    omp_threads_str = (
        os.environ.get("OPENBLAS_NUM_THREADS")
        or os.environ.get("MKL_NUM_THREADS")
        or os.environ.get("OMP_NUM_THREADS")
    )
    if omp_threads_str:
        with suppress(ValueError, RuntimeError):
            omp_threads = int(omp_threads_str)
            if omp_threads > 0:
                ray._private.utils.set_blas_and_torch_num_threads(omp_threads)

    # Reset cached CUDA device counts and stage deferred PyTorch device-backend autoload
    # so out-of-tree backends import only after set_visible_accelerator_ids() assigns
    # per-worker visibility env vars (TPU_VISIBLE_CHIPS, HABANA_VISIBLE_MODULES, etc.).
    torch_cuda = sys.modules.get("torch.cuda")
    if torch_cuda is not None and hasattr(torch_cuda, "_cached_device_count"):
        torch_cuda._cached_device_count = None
    if (
        "torch" in sys.modules
        and os.environ.get("TORCH_DEVICE_BACKEND_AUTOLOAD") != "0"
    ):
        ray._private.utils._pending_torch_device_backend_autoload = True

    # Refresh C++ RayConfig (including RAY_JOB_ID / RAY_RAYLET_PID internal flags)
    # and authentication token cache from the child's updated os.environ.
    ray._raylet.Config.initialize("")
    ray._raylet.AuthenticationTokenLoader.instance().reset_cache()

    gw = ray._private.worker.global_worker
    gw._original_visible_accelerator_ids = None

    non_internal_child_keys = child_env.keys() - _RAYLET_INTERNAL_ENV_KEYS
    override_keys = set(runtime_env_vars.keys()) | non_internal_child_keys
    has_ray_const_override = any(
        key.startswith("RAY_") or key == "FUNCTION_SIZE_ERROR_THRESHOLD"
        for key in override_keys
    )
    if has_ray_const_override:
        prev_zero_copy = ray_constants.RAY_ENABLE_ZERO_COPY_TORCH_TENSORS
        importlib.reload(common_ray_constants)
        importlib.reload(ray_constants)
        if "RAY_allow_out_of_band_object_ref_serialization" in override_keys:
            oob_allowed = ray_constants.env_bool(
                "RAY_allow_out_of_band_object_ref_serialization", True
            )
            common_serialization.ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION = (
                oob_allowed
            )
            private_serialization.ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION = (
                oob_allowed
            )
        if any(k in override_keys for k in _DEDUP_LOG_ENV_KEYS):
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
        if ray_constants.RAY_ENABLE_ZERO_COPY_TORCH_TENSORS != prev_zero_copy or any(
            k in override_keys for k in _SERIALIZATION_ENV_OVERRIDE_KEYS
        ):
            _clear_conditional_cloudpickle_reducers()
            gw.serialization_context_map.clear()
        if "RAY_USE_UVLOOP" in override_keys and not ray_constants.env_bool(
            "RAY_USE_UVLOOP", True
        ):
            asyncio.set_event_loop_policy(None)
    gw._enable_record_actor_task_log = (
        ray_constants.RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING
    )

    random.seed()
    for mod_name in ("numpy.random", "torch"):
        mod = sys.modules.get(mod_name)
        if mod is not None and hasattr(mod, "seed"):
            mod.seed()

    sys.argv = [entrypoint_file, *child_args]


def _run_fork_server(argv: Optional[List[str]] = None) -> Optional[ModuleType]:
    parser = argparse.ArgumentParser(description="Ray Python worker fork server")
    parser.add_argument("--socket-path", required=True, type=str)
    parser.add_argument("--preload-modules", default="", type=str)
    parser.add_argument("--logs-dir", default=None, type=str)
    parser.add_argument("--cluster-pythonpath", default="", type=str)
    parser.add_argument("--worker-stdout-fd", default=None, type=int)
    parser.add_argument("--worker-stderr-fd", default=None, type=int)
    parser.add_argument("--monitor-stdin", action="store_true")
    args = parser.parse_args(argv)

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

    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    _register_lifetime_pipe_at_fork_once()

    # Pin thread counts, jemalloc background threads, NVML-only PyTorch CUDA probes,
    # and deferred PyTorch out-of-tree backend autoloading before importing worker or
    # user modules so the fork server remains single-threaded and accelerator-clean.
    saved_env_pins: Dict[str, Optional[str]] = {
        "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS"),
        "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS"),
        "MKL_NUM_THREADS": os.environ.get("MKL_NUM_THREADS"),
        "JE_ARROW_MALLOC_CONF": os.environ.get("JE_ARROW_MALLOC_CONF"),
        "PYTORCH_NVML_BASED_CUDA_CHECK": os.environ.get(
            "PYTORCH_NVML_BASED_CUDA_CHECK"
        ),
        "TORCH_DEVICE_BACKEND_AUTOLOAD": os.environ.get(
            "TORCH_DEVICE_BACKEND_AUTOLOAD"
        ),
    }
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ.setdefault("JE_ARROW_MALLOC_CONF", "background_thread:false")
    os.environ["PYTORCH_NVML_BASED_CUDA_CHECK"] = "1"
    os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] = "0"

    # Import default_worker and Ray helper modules after pinning pre-fork env vars.
    # Importing ray._private.accelerators pre-shares its Python modules and requests
    # stack across CoW pages without invoking vendor hardware SDKs.
    default_worker = importlib.import_module("ray._private.workers.default_worker")
    try_install_uvloop()
    importlib.import_module("ray._common.usage.usage_lib")
    importlib.import_module("ray._private.accelerators")
    ray._private.worker.global_worker.get_serialization_context()
    _preload_modules(args.preload_modules)

    torch_cuda = sys.modules.get("torch.cuda")
    if torch_cuda is not None and getattr(torch_cuda, "_initialized", False):
        msg = (
            "ForkServer refusing to start because a preloaded module initialized "
            "CUDA before fork(); falling back to spawning workers via execvpe."
        )
        logger.warning(msg)
        print(msg, file=sys.stderr, flush=True)
        if lifeline_fd is not None:
            with suppress(OSError):
                os.close(lifeline_fd)
        raise SystemExit(1)

    if "torch" in sys.modules:
        ray._private.utils.cache_torch_device_backend_entrypoints()
    ray._private.utils.set_blas_and_torch_num_threads(1)
    importlib.invalidate_caches()

    cluster_pypaths = [p for p in args.cluster_pythonpath.split(os.pathsep) if p]
    cluster_set = set(cluster_pypaths)
    plugin_pypaths = [
        p
        for p in os.environ.get("PYTHONPATH", "").split(os.pathsep)
        if p and p not in cluster_set
    ]

    gc.collect()
    gc.freeze()

    if lifeline_fd is not None:
        readable, _, _ = select.select([lifeline_fd], [], [], 0)
        if readable and _is_lifeline_closed(lifeline_fd):
            os.close(lifeline_fd)
            return None

    _redirect_post_preload_stdio(
        args.logs_dir, args.worker_stdout_fd, args.worker_stderr_fd
    )

    socket_path = os.path.abspath(args.socket_path)
    socket_dir = os.path.dirname(socket_path)
    os.makedirs(socket_dir, exist_ok=True)
    server_pid = os.getpid()
    tmp_socket_path = os.path.join(socket_dir, f".f{server_pid}")
    Path(tmp_socket_path).unlink(missing_ok=True)

    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    wakeup_r, wakeup_w = os.pipe()
    os.set_blocking(wakeup_r, False)
    os.set_blocking(wakeup_w, False)
    peer_poller = select.poll()
    published_stat: Optional[Tuple[int, int]] = None
    stop_requested = False

    def _handle_sigterm(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True

    signal.set_wakeup_fd(wakeup_w, warn_on_full_buffer=False)
    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        server_sock.bind(tmp_socket_path)
        os.chmod(tmp_socket_path, 0o600)
        tmp_st = os.lstat(tmp_socket_path)
        server_sock.listen(128)
        server_sock.setblocking(False)
        if stop_requested:
            return None
        if lifeline_fd is not None:
            readable, _, _ = select.select([lifeline_fd], [], [], 0)
            if readable and _is_lifeline_closed(lifeline_fd):
                return None
        os.replace(tmp_socket_path, socket_path)
        published_stat = (tmp_st.st_dev, tmp_st.st_ino)

        watch_fds = [server_sock, wakeup_r]
        if lifeline_fd is not None:
            watch_fds.append(lifeline_fd)
        while not stop_requested:
            try:
                readable, _, _ = select.select(watch_fds, [], [])
            except InterruptedError:
                continue
            except (OSError, ValueError):
                break

            if wakeup_r in readable:
                with suppress(BlockingIOError, InterruptedError, OSError):
                    os.read(wakeup_r, 64)
            if lifeline_fd is not None and lifeline_fd in readable:
                if _is_lifeline_closed(lifeline_fd):
                    break
            if stop_requested or server_sock not in readable:
                continue

            try:
                conn, _ = server_sock.accept()
            except (BlockingIOError, InterruptedError):
                continue
            except OSError:
                break

            # Drop connections whose client already timed out and closed while queued.
            conn_fd = conn.fileno()
            peer_poller.register(conn_fd, _PEER_DISCONNECTED_MASK)
            try:
                peer_gone = bool(peer_poller.poll(0))
            finally:
                peer_poller.unregister(conn_fd)
            if peer_gone:
                conn.close()
                continue

            # Create the lifetime pipe and fork immediately on accept() so the parent
            # replies while hot on-CPU without blocking on slow writers or reading
            # per-worker env_vars onto shared CoW parent heap pages.
            try:
                life_r, life_w = os.pipe()
            except OSError as e:
                logger.warning(f"ForkServer os.pipe() failed: {e}")
                with suppress(OSError):
                    conn.sendall(f'{{"error":"os.pipe() failed: {e}"}}\n'.encode())
                conn.close()
                continue

            try:
                pid = os.fork()
            except OSError as e:
                os.close(life_r)
                os.close(life_w)
                logger.warning(f"ForkServer os.fork() failed: {e}")
                with suppress(OSError):
                    conn.sendall(f'{{"error":"os.fork() failed: {e}"}}\n'.encode())
                conn.close()
                continue

            if pid > 0:
                os.close(life_w)
                with suppress(OSError):
                    os.setpgid(pid, pid)
                try:
                    socket.send_fds(
                        conn, [f'{{"pid":{pid}}}\n'.encode("ascii")], [life_r]
                    )
                except OSError:
                    with suppress(OSError):
                        os.kill(pid, signal.SIGKILL)
                finally:
                    os.close(life_r)
                    conn.close()
                continue

            # Child process: read the buffered request in parallel and exit via os._exit
            # on any protocol error so parent finally cleanup never executes.
            try:
                global _WORKER_LIFETIME_PIPE_W
                os.close(life_r)
                _WORKER_LIFETIME_PIPE_W = life_w
                with suppress(OSError):
                    os.setpgrp()
                signal.set_wakeup_fd(-1)
                signal.signal(signal.SIGCHLD, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.default_int_handler)
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

                server_sock.close()
                for fd_to_close in (wakeup_r, wakeup_w, lifeline_fd):
                    if fd_to_close is not None:
                        with suppress(OSError):
                            os.close(fd_to_close)

                conn.settimeout(2.0)
                try:
                    line = _recv_line(conn)
                finally:
                    conn.close()

                if not line:
                    os._exit(0)

                req = json.loads(line)
                if not isinstance(req, dict):
                    os._exit(1)
                child_args = req.get("args", [])
                child_env = req.get("env", {})
                runtime_env_vars = req.get("runtime_env_vars", {})
                if (
                    not isinstance(child_args, list)
                    or not isinstance(child_env, dict)
                    or not isinstance(runtime_env_vars, dict)
                ):
                    os._exit(1)

                _prepare_child_environment(
                    saved_env_pins,
                    plugin_pypaths,
                    cluster_pypaths,
                    child_env,
                    runtime_env_vars,
                    child_args,
                    default_worker.__file__,
                )
                return default_worker
            except BaseException:
                with suppress(Exception):
                    traceback.print_exc()
                os._exit(1)
    finally:
        if os.getpid() == server_pid:
            with suppress(ValueError, OSError):
                signal.set_wakeup_fd(-1)
            with suppress(OSError):
                server_sock.close()
            for fd_to_close in (wakeup_r, wakeup_w, lifeline_fd):
                if fd_to_close is not None:
                    with suppress(OSError):
                        os.close(fd_to_close)
            Path(tmp_socket_path).unlink(missing_ok=True)
            if published_stat is not None:
                with suppress(OSError):
                    cur_st = os.lstat(socket_path)
                    if (cur_st.st_dev, cur_st.st_ino) == published_stat:
                        os.unlink(socket_path)

    return None


if __name__ == "__main__":
    default_worker_mod = _run_fork_server()
    if default_worker_mod is not None:
        # Keep worker.main_loop() at top-level module scope (pull/12225).
        mode, worker = default_worker_mod.initialize_default_worker()
        if mode == ray.WORKER_MODE:
            worker.main_loop()
        else:
            raise ValueError(f"Unexpected worker mode: {mode}")
