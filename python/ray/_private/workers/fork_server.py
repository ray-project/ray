import argparse
import asyncio
import gc
import importlib
import json
import logging
import os
import random
import select
import signal
import socket
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from types import ModuleType
from typing import IO, TYPE_CHECKING, Dict, List, Optional, Tuple

import ray
import ray._private.ray_constants as ray_constants
import ray._private.utils
from ray._private.async_compat import try_install_uvloop

if TYPE_CHECKING:
    from ray._private.runtime_env.context import RuntimeEnvContext

logger = logging.getLogger(__name__)


class ForkServerProcess:
    """Manages a pre-initialized Python fork server process and its Unix socket."""

    def __init__(
        self,
        socket_path: str,
        preload_modules: Optional[List[str]] = None,
        context: Optional["RuntimeEnvContext"] = None,
        cgroup_path: Optional[str] = None,
    ):
        self.socket_path = socket_path
        self.started_at = time.monotonic()
        self.ready_at: Optional[float] = None
        self._stopped = False

        os.makedirs(os.path.dirname(socket_path), exist_ok=True)
        Path(socket_path).unlink(missing_ok=True)

        workers_dir = os.path.dirname(os.path.abspath(__file__))
        fork_server_args = [
            os.path.abspath(__file__),
            f"--socket-path={socket_path}",
            f"--preload-modules={','.join(preload_modules or [])}",
            "--monitor-stdin",
        ]
        if context is not None:
            setup_worker_path = os.path.join(workers_dir, "setup_worker.py")
            cmd = [
                sys.executable,
                setup_worker_path,
                f"--serialized-runtime-env-context={context.serialize()}",
                "--language=PYTHON",
                *fork_server_args,
            ]
        else:
            cmd = [sys.executable, *fork_server_args]

        if cgroup_path and sys.platform.startswith("linux"):
            cgroup_bootstrap_path = os.path.join(workers_dir, "_cgroup_bootstrap.py")
            cmd = [
                sys.executable,
                "-S",
                cgroup_bootstrap_path,
                cgroup_path,
                *cmd,
            ]

        # Omit preexec_fn so CPython uses os.posix_spawn safely inside multi-threaded
        # RuntimeEnvAgent (including worker threads). Fate-sharing with the parent
        # process uses stdin=PIPE + --monitor-stdin instead of PR_SET_PDEATHSIG, which
        # is bound to the spawning thread on Linux and leaks .sock files on SIGKILL.
        self.process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            close_fds=True,
            start_new_session=True,
        )
        self._lifeline: Optional[IO[bytes]] = self.process.stdin

    def is_alive(self) -> bool:
        return not self._stopped and self.process.poll() is None

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
            logger.warning(f"ForkServer failed to preload module '{mod}': {e}")


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


def _recv_line(conn: socket.socket) -> bytes:
    """Read a newline-terminated request line without allocating SocketIO/BufferedReader."""
    first = conn.recv(4096)
    if not first:
        return b""
    nl = first.find(b"\n")
    if nl != -1:
        return first[: nl + 1]
    buf = bytearray(first)
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            return bytes(buf)
        nl = chunk.find(b"\n")
        if nl != -1:
            buf.extend(chunk[: nl + 1])
            return bytes(buf)
        buf.extend(chunk)


def _is_lifeline_closed(lifeline_fd: int) -> bool:
    """Return True if the parent write-end of the lifeline pipe reached EOF."""
    try:
        return os.read(lifeline_fd, 1) == b""
    except (BlockingIOError, InterruptedError):
        return False
    except OSError:
        return True


def _monotonic_time_ms() -> int:
    if hasattr(time, "clock_gettime_ns") and hasattr(time, "CLOCK_MONOTONIC"):
        return time.clock_gettime_ns(time.CLOCK_MONOTONIC) // 1_000_000
    return int(time.monotonic() * 1000)


def _prepare_child_environment(
    saved_omp_num_threads: Optional[str],
    saved_je_arrow_malloc_conf: Optional[str],
    child_env: Dict[str, str],
    runtime_env_vars: Dict[str, str],
    child_args: List[str],
    default_worker_file: str,
) -> None:
    if saved_omp_num_threads is None:
        os.environ.pop("OMP_NUM_THREADS", None)
    else:
        os.environ["OMP_NUM_THREADS"] = saved_omp_num_threads

    if saved_je_arrow_malloc_conf is None:
        os.environ.pop("JE_ARROW_MALLOC_CONF", None)
    else:
        os.environ["JE_ARROW_MALLOC_CONF"] = saved_je_arrow_malloc_conf

    if "RAY_JOB_ID" not in child_env and "RAY_JOB_ID" not in runtime_env_vars:
        os.environ.pop("RAY_JOB_ID", None)
    # Apply raylet base environment first (literal key-values such as RAY_JOB_ID)
    # before expanding user runtime_env_vars so references like ${RAY_JOB_ID} resolve.
    os.environ.update(child_env)
    if runtime_env_vars:
        ray._private.utils.update_envs(runtime_env_vars)
    if "PYTHONPATH" in child_env or "PYTHONPATH" in runtime_env_vars:
        for path_entry in reversed(os.environ.get("PYTHONPATH", "").split(os.pathsep)):
            if path_entry and path_entry not in sys.path:
                sys.path.insert(0, path_entry)
        importlib.invalidate_caches()

    omp_threads_str = os.environ.get("OPENBLAS_NUM_THREADS") or os.environ.get(
        "OMP_NUM_THREADS"
    )
    if omp_threads_str:
        with suppress(ValueError, RuntimeError):
            omp_threads = int(omp_threads_str)
            if omp_threads > 0:
                ray._private.utils.set_blas_and_torch_num_threads(omp_threads)

    # Refresh C++ RayConfig (including RAY_JOB_ID / RAY_RAYLET_PID internal flags)
    # and authentication token cache from the child's updated os.environ.
    ray._raylet.Config.initialize("")
    ray._raylet.AuthenticationTokenLoader.instance().reset_cache()

    # Refresh import-time ray_constants and global_worker attributes when user env
    # overrides Ray flags without touching ray_constants pages on standard workers.
    gw = ray._private.worker.global_worker
    non_internal_child_keys = child_env.keys() - _RAYLET_INTERNAL_ENV_KEYS
    if runtime_env_vars or non_internal_child_keys:
        gw.original_visible_accelerator_ids = (
            ray._private.utils.get_visible_accelerator_ids()
        )
    has_ray_const_override = any(
        key.startswith("RAY_") or key == "FUNCTION_SIZE_ERROR_THRESHOLD"
        for key in (*runtime_env_vars.keys(), *non_internal_child_keys)
    )
    if has_ray_const_override:
        prev_zero_copy = ray_constants.RAY_ENABLE_ZERO_COPY_TORCH_TENSORS
        importlib.reload(ray_constants)
        if (
            ray_constants.RAY_ENABLE_ZERO_COPY_TORCH_TENSORS != prev_zero_copy
            or "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS" in runtime_env_vars
            or "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS" in non_internal_child_keys
        ):
            gw.serialization_context_map.clear()
    gw._enable_record_actor_task_log = (
        ray_constants.RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING
    )

    random.seed()
    for mod_name in ("numpy.random", "torch"):
        mod = sys.modules.get(mod_name)
        if mod is not None and hasattr(mod, "seed"):
            mod.seed()

    sys.argv = [default_worker_file, *child_args]


def _run_fork_server(argv: Optional[List[str]] = None) -> Optional[ModuleType]:
    parser = argparse.ArgumentParser(description="Ray Python worker fork server")
    parser.add_argument("--socket-path", required=True, type=str)
    parser.add_argument("--preload-modules", default="", type=str)
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

    # Pin OMP_NUM_THREADS=1 and disable Arrow's jemalloc background thread before
    # importing default_worker, SerializationContext (numpy/pyarrow), or preload_modules
    # so the fork server remains strictly single-threaded before fork().
    saved_omp_num_threads = os.environ.get("OMP_NUM_THREADS")
    saved_je_arrow_malloc_conf = os.environ.get("JE_ARROW_MALLOC_CONF")
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ.setdefault("JE_ARROW_MALLOC_CONF", "background_thread:false")

    import ray._private.workers.default_worker as default_worker

    try_install_uvloop()
    importlib.import_module("ray._common.usage.usage_lib")
    ray._private.worker.global_worker.get_serialization_context()
    _preload_modules(args.preload_modules)
    ray._private.utils.set_blas_and_torch_num_threads(1)
    importlib.invalidate_caches()

    gc.collect()
    gc.freeze()

    if lifeline_fd is not None:
        readable, _, _ = select.select([lifeline_fd], [], [], 0)
        if readable and _is_lifeline_closed(lifeline_fd):
            os.close(lifeline_fd)
            return None

    socket_path = os.path.abspath(args.socket_path)
    socket_dir = os.path.dirname(socket_path)
    os.makedirs(socket_dir, exist_ok=True)
    server_pid = os.getpid()
    tmp_socket_path = os.path.join(socket_dir, f".fs_{server_pid}.sock")
    Path(tmp_socket_path).unlink(missing_ok=True)

    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    wakeup_r, wakeup_w = os.pipe()
    os.set_blocking(wakeup_r, False)
    os.set_blocking(wakeup_w, False)
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

            try:
                conn.settimeout(2.0)
                line = _recv_line(conn)
                if not line:
                    conn.close()
                    continue
                req = json.loads(line)
                deadline_ms = req.get("deadline_ms")
                if deadline_ms is not None and _monotonic_time_ms() > int(deadline_ms):
                    with suppress(OSError):
                        conn.sendall(
                            json.dumps({"error": "request expired"}).encode("utf-8")
                            + b"\n"
                        )
                    conn.close()
                    continue
                child_args: List[str] = req.get("args", [])
                child_env: Dict[str, str] = req.get("env", {})
                runtime_env_vars: Dict[str, str] = req.get("runtime_env_vars", {})
            except Exception as e:
                logger.warning(f"Invalid fork request: {e}")
                conn.close()
                continue

            pid = os.fork()
            if pid > 0:
                with suppress(OSError):
                    os.setpgid(pid, pid)
                try:
                    conn.sendall(json.dumps({"pid": pid}).encode("utf-8") + b"\n")
                except OSError:
                    with suppress(OSError):
                        os.kill(pid, signal.SIGKILL)
                finally:
                    conn.close()
            else:
                with suppress(OSError):
                    os.setpgrp()
                signal.set_wakeup_fd(-1)
                signal.signal(signal.SIGCHLD, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.default_int_handler)
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

                conn.close()
                server_sock.close()
                for fd_to_close in (wakeup_r, wakeup_w, lifeline_fd):
                    if fd_to_close is not None:
                        with suppress(OSError):
                            os.close(fd_to_close)

                _prepare_child_environment(
                    saved_omp_num_threads,
                    saved_je_arrow_malloc_conf,
                    child_env,
                    runtime_env_vars,
                    child_args,
                    default_worker.__file__,
                )
                return default_worker
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
        elif mode in [ray.RESTORE_WORKER_MODE, ray.SPILL_WORKER_MODE]:
            while True:
                time.sleep(100000)
        else:
            raise ValueError(f"Unexpected worker mode: {mode}")
