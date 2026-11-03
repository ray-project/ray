import argparse
import gc
import importlib
import json
import logging
import os
import random
import signal
import socket
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Dict, List, Optional

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
    ):
        self.socket_path = socket_path
        os.makedirs(os.path.dirname(socket_path), exist_ok=True)
        Path(socket_path).unlink(missing_ok=True)

        fork_server_args = [
            os.path.abspath(__file__),
            f"--socket-path={socket_path}",
            f"--preload-modules={','.join(preload_modules or [])}",
        ]
        if context is not None:
            setup_worker_path = os.path.join(
                os.path.dirname(__file__), "setup_worker.py"
            )
            cmd = [
                sys.executable,
                setup_worker_path,
                f"--serialized-runtime-env-context={context.serialize()}",
                "--language=PYTHON",
                *fork_server_args,
            ]
        else:
            cmd = [sys.executable, *fork_server_args]

        self.process = subprocess.Popen(
            cmd,
            close_fds=True,
            start_new_session=True,
            preexec_fn=(
                ray._private.utils.set_kill_on_parent_death_linux
                if sys.platform.startswith("linux")
                else None
            ),
        )

    def wait_ready(self, timeout_s: Optional[float] = None) -> None:
        if timeout_s is None:
            timeout_s = float(
                os.environ.get("RAY_fork_server_ready_timeout_s", "120.0")
            )
        deadline = time.monotonic() + timeout_s
        while not os.path.exists(self.socket_path):
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"ForkServer exited prematurely with code {self.process.returncode}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for ForkServer socket at {self.socket_path}"
                )
            time.sleep(0.01)

    def stop(self) -> None:
        Path(self.socket_path).unlink(missing_ok=True)
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
                self.process.wait(timeout=1.0)
            except Exception:
                with suppress(Exception):
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=1.0)


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

    # Refresh C++ authentication token cache from updated os.environ.
    ray._raylet.AuthenticationTokenLoader.instance().reset_cache()

    # Refresh import-time global_worker attributes that depend on os.environ.
    gw = ray._private.worker.global_worker
    if runtime_env_vars or not (child_env.keys() <= _RAYLET_INTERNAL_ENV_KEYS):
        gw.original_visible_accelerator_ids = (
            ray._private.utils.get_visible_accelerator_ids()
        )
    gw._enable_record_actor_task_log = ray_constants.env_bool(
        "RAY_ENABLE_RECORD_ACTOR_TASK_LOG", False
    )
    if (
        "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS" in child_env
        or "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS" in runtime_env_vars
    ):
        gw.serialization_context_map.clear()

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
    args = parser.parse_args(argv)

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

    socket_path = os.path.abspath(args.socket_path)
    socket_dir = os.path.dirname(socket_path)
    os.makedirs(socket_dir, exist_ok=True)
    server_pid = os.getpid()
    tmp_socket_path = os.path.join(socket_dir, f".fs_{server_pid}.sock")
    Path(tmp_socket_path).unlink(missing_ok=True)

    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_sock.bind(tmp_socket_path)
    os.chmod(tmp_socket_path, 0o600)
    server_sock.listen(128)
    os.replace(tmp_socket_path, socket_path)

    def _cleanup_and_exit(_signum, _frame):
        server_sock.close()
        Path(tmp_socket_path).unlink(missing_ok=True)
        Path(socket_path).unlink(missing_ok=True)
        os._exit(0)

    signal.signal(signal.SIGTERM, _cleanup_and_exit)

    try:
        while True:
            try:
                conn, _ = server_sock.accept()
            except InterruptedError:
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
                if deadline_ms is not None and time.time() * 1000.0 > float(
                    deadline_ms
                ):
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
                conn.close()
                server_sock.close()

                signal.signal(signal.SIGCHLD, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.default_int_handler)
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

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
            Path(tmp_socket_path).unlink(missing_ok=True)
            Path(socket_path).unlink(missing_ok=True)

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
