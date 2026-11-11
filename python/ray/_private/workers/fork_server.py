import argparse
import ctypes
import gc
import importlib
import importlib.metadata
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
import traceback
from contextlib import suppress
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import ray
import ray._private.ray_constants as ray_constants
import ray._private.utils
import ray._private.worker

if TYPE_CHECKING:
    from ray._private.runtime_env.context import RuntimeEnvContext

logger = logging.getLogger(__name__)

_PRE_FORK_ENV_OVERRIDES = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "JE_ARROW_MALLOC_CONF": "background_thread:false",
    "_RJEM_MALLOC_CONF": "background_thread:false",
    "PYTORCH_NVML_BASED_CUDA_CHECK": "1",
    "TORCH_DEVICE_BACKEND_AUTOLOAD": "0",
}

_MAX_FORK_REQUEST_BYTES = 4 * 1024 * 1024
# Omit POLLRDHUP so half-closed clients (shutdown(SHUT_WR) after sending the request)
# are still served; full client close raises POLLHUP on AF_UNIX SOCK_STREAM.
_PEER_DISCONNECTED_MASK = getattr(select, "POLLHUP", 0) | getattr(select, "POLLERR", 0)
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


def _is_lifeline_closed(fd: int) -> bool:
    try:
        return not os.read(fd, 64)
    except BlockingIOError:
        return False
    except OSError:
        return True


def _recv_line(conn: socket.socket, timeout_s: float = 2.0) -> bytes:
    """Read a single newline-terminated JSON request line with size and time caps."""
    deadline = time.monotonic() + timeout_s
    buf = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Timed out reading fork request")
        conn.settimeout(remaining)
        chunk = conn.recv(4096)
        if not chunk:
            return b""
        nl = chunk.find(b"\n")
        if nl != -1:
            if len(buf) + nl + 1 > _MAX_FORK_REQUEST_BYTES:
                raise ValueError("Fork request exceeds maximum size limit")
            buf.extend(chunk[: nl + 1])
            return bytes(buf)
        if len(buf) + len(chunk) > _MAX_FORK_REQUEST_BYTES:
            raise ValueError("Fork request exceeds maximum size limit")
        buf.extend(chunk)


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
    ):
        self.socket_path = os.path.abspath(socket_path)
        ray._private.utils.validate_socket_filepath(self.socket_path)
        Path(self.socket_path).unlink(missing_ok=True)

        workers_dir = os.path.dirname(os.path.abspath(__file__))
        cmd = [sys.executable]
        if context is not None:
            cmd += [
                os.path.join(workers_dir, "setup_worker.py"),
                f"--serialized-runtime-env-context={context.serialize()}",
                "--language=PYTHON",
            ]
        cmd += [
            os.path.abspath(__file__),
            f"--socket-path={self.socket_path}",
            f"--preload-modules={','.join(preload_modules or [])}",
            "--monitor-stdin",
        ]
        if cgroup_path and sys.platform.startswith("linux"):
            cmd = [
                sys.executable,
                "-S",
                os.path.join(workers_dir, "_cgroup_bootstrap.py"),
                cgroup_path,
                *cmd,
            ]

        self.process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            start_new_session=True,
        )
        self._socket_id = None
        if wait_ready:
            self.wait_ready(timeout_s=timeout_s)

    def wait_ready(self, timeout_s: float = 120.0) -> None:
        try:
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
            st = os.lstat(self.socket_path)
            self._socket_id = (st.st_dev, st.st_ino)
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        if self.process.stdin is not None:
            with suppress(OSError):
                self.process.stdin.close()
        if self._socket_id is not None:
            with suppress(OSError):
                st = os.lstat(self.socket_path)
                if (st.st_dev, st.st_ino) == self._socket_id:
                    os.unlink(self.socket_path)
            self._socket_id = None
        for sig in (signal.SIGTERM, signal.SIGKILL):
            if self.process.poll() is not None:
                break
            with suppress(Exception):
                os.killpg(self.process.pid, sig)
                self.process.wait(timeout=1.0)


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
        except Exception:
            logger.exception(f"Failed to preload module '{module}'")


def _env_int(name: str, default: int = 0) -> int:
    with suppress(ValueError):
        if (v := int(os.environ.get(name) or 0)) > 0:
            return v
    return default


def _resize_native_thread_pools(omp: int, openblas: int, mkl: int) -> None:
    if (
        (omp <= 0 and openblas <= 0 and mkl <= 0)
        or not sys.platform.startswith("linux")
        or not hasattr(os, "RTLD_NOLOAD")
    ):
        return
    targets = (
        (
            openblas,
            (
                "scipy_openblas_set_num_threads64_",
                "scipy_openblas_set_num_threads",
                "openblas_set_num_threads",
            ),
        ),
        (mkl, ("MKL_Set_Num_Threads",)),
        (omp, ("omp_set_num_threads",)),
    )
    so_paths = ()
    with suppress(OSError), open("/proc/self/maps") as f:
        so_paths = {line.split(None, 5)[-1].strip() for line in f if ".so" in line}
    for path in so_paths:
        if path.startswith("/") and any(
            k in path.lower() for k in ("openblas", "mkl", "omp")
        ):
            with suppress(OSError):
                lib = ctypes.CDLL(path, mode=os.RTLD_NOLOAD | os.RTLD_LOCAL)
                for count, syms in targets:
                    for sym in syms if count > 0 else ():
                        if (fn := getattr(lib, sym, None)) is not None:
                            fn(ctypes.c_int(count))


def _prepare_child_environment(
    saved_pre_fork_env: Dict[str, Optional[str]],
    child_env: Dict[str, str],
    child_args: List[str],
    runtime_env_vars: Optional[Dict[str, str]] = None,
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
    os.environ.pop("RAY_JOB_ID", None)
    os.environ.update(child_env)
    ray._private.utils.update_envs(runtime_env_vars)
    if "TZ" in changed_keys and hasattr(time, "tzset"):
        time.tzset()
    if "PYTHONPATH" in changed_keys:
        ray._private.utils._cached_torch_backend_eps = None
        cluster_py = {
            os.path.abspath(p)
            for p in child_env.get("PYTHONPATH", "").split(os.pathsep)
            if p
        }
        rt_env_py = [p for p in old_py_dirs if p not in cluster_py]
        merged_py_dirs = list(
            dict.fromkeys(
                [
                    *rt_env_py,
                    *(
                        os.path.abspath(p)
                        for layer in (
                            os.environ.get("PYTHONPATH", ""),
                            child_env.get("PYTHONPATH", ""),
                            parent_pythonpath,
                        )
                        for p in layer.split(os.pathsep)
                        if p
                    ),
                ]
            )
        )
        if merged_py_dirs:
            os.environ["PYTHONPATH"] = os.pathsep.join(merged_py_dirs)
        else:
            os.environ.pop("PYTHONPATH", None)
        idx = 1 if sys.path else 0
        sys.path[idx:] = [
            p
            for p in sys.path[idx:]
            if os.path.abspath(p) not in old_py_dirs
            and os.path.abspath(p) not in merged_py_dirs
        ]
        sys.path[idx:idx] = merged_py_dirs
        importlib.invalidate_caches()
    ray._private.utils._torch_backends_autoload_pending = "torch" in sys.modules

    omp_threads = _env_int("OMP_NUM_THREADS")
    if omp_threads > 0:
        ray._private.utils.set_thread_pool_module_threads(omp_threads)
    _resize_native_thread_pools(
        omp_threads,
        _env_int("OPENBLAS_NUM_THREADS", omp_threads),
        _env_int("MKL_NUM_THREADS", omp_threads),
    )

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
        importlib.reload(ray_constants)
        default_worker_mod = sys.modules.get("ray._private.workers.default_worker")
        if default_worker_mod is not None:
            default_worker_mod.parser.set_defaults(
                logging_level=ray_constants.LOGGER_LEVEL
            )
    if "RAY_allow_out_of_band_object_ref_serialization" in changed_keys:
        serialization_mod = sys.modules.get("ray._private.serialization")
        if serialization_mod is not None:
            serialization_mod.ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION = (
                ray_constants.env_bool(
                    "RAY_allow_out_of_band_object_ref_serialization", True
                )
            )
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

    sys.argv = [sys.argv[0], *child_args]


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

    _register_lifetime_pipe_at_fork_once()

    # Keep library imports single-threaded before fork().
    saved_pre_fork_env = {k: os.environ.get(k) for k in _PRE_FORK_ENV_OVERRIDES}
    os.environ.update(_PRE_FORK_ENV_OVERRIDES)

    import ray._common.usage.usage_lib as _usage_lib  # noqa: F401
    import ray._private.workers.default_worker as default_worker

    with suppress(Exception):
        ray._private.worker.global_worker.get_serialization_context()
    _preload_modules(args.preload_modules)
    ray._private.utils._cached_torch_backend_eps = list(
        importlib.metadata.entry_points(group="torch.backends")
    )
    if getattr(sys.modules.get("torch.cuda"), "_initialized", False):
        if lifeline_fd is not None:
            with suppress(OSError):
                os.close(lifeline_fd)
        raise RuntimeError(
            "Preloaded module initialized CUDA before fork(); refusing to start ForkServer"
        )

    # Ignore SIGCHLD after preloading so preloaded modules calling subprocesses
    # still observe exit codes, while forked workers are auto-reaped by the kernel.
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    gc.collect()
    gc.freeze()

    socket_path = os.path.abspath(args.socket_path)
    ray._private.utils.validate_socket_filepath(socket_path)
    socket_dir = os.path.dirname(socket_path)
    os.makedirs(socket_dir, exist_ok=True)
    server_pid = os.getpid()
    tmp_name = f".f{server_pid}"
    tmp_socket_path = os.path.join(socket_dir, tmp_name)
    Path(tmp_socket_path).unlink(missing_ok=True)

    server_sock: Optional[socket.socket] = None
    wakeup_r: Optional[int] = None
    wakeup_w: Optional[int] = None
    peer_poller = select.poll()
    published_stat: Optional[Tuple[int, int]] = None
    stop_requested = False

    def _handle_sigterm(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True

    try:
        server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        wakeup_r, wakeup_w = os.pipe()
        os.set_blocking(wakeup_r, False)
        os.set_blocking(wakeup_w, False)
        signal.set_wakeup_fd(wakeup_w, warn_on_full_buffer=False)
        signal.signal(signal.SIGTERM, _handle_sigterm)

        _bind_staging_socket(server_sock, socket_dir, tmp_name, tmp_socket_path)
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

        sys.stdout.flush()
        sys.stderr.flush()

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
            except OSError as e:
                logger.warning(f"Fork server failed to accept a connection: {e}")
                time.sleep(0.1)
                continue

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
            # stays off slow clients and never pulls per-worker env_vars into its CoW heap.
            try:
                life_r, life_w = os.pipe()
            except OSError as e:
                logger.warning(f"ForkServer os.pipe() failed: {e}")
                conn.close()
                continue

            try:
                pid = os.fork()
            except OSError as e:
                os.close(life_r)
                os.close(life_w)
                logger.warning(f"ForkServer os.fork() failed: {e}")
                conn.close()
                continue

            if pid > 0:
                os.close(life_w)
                with suppress(OSError):
                    os.setpgid(pid, pid)
                try:
                    conn.settimeout(1.0)
                    reply = f'{{"pid":{pid}}}\n'.encode("ascii")
                    sent = socket.send_fds(conn, [reply], [life_r])
                    if sent < len(reply):
                        conn.sendall(reply[sent:])
                except OSError:
                    with suppress(OSError):
                        os.kill(pid, signal.SIGKILL)
                finally:
                    os.close(life_r)
                    conn.close()
                continue

            # Child process: read the request after fork and exit via os._exit on any
            # protocol/setup error so parent finally cleanup never executes.
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

                try:
                    line = _recv_line(conn, timeout_s=2.0)
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
                    or not all(isinstance(a, str) for a in child_args)
                    or not isinstance(child_env, dict)
                    or not all(
                        isinstance(k, str) and isinstance(v, str)
                        for k, v in child_env.items()
                    )
                    or not isinstance(runtime_env_vars, dict)
                    or not all(
                        isinstance(k, str) and isinstance(v, str)
                        for k, v in runtime_env_vars.items()
                    )
                ):
                    os._exit(1)

                _prepare_child_environment(
                    saved_pre_fork_env, child_env, child_args, runtime_env_vars
                )
                return default_worker
            except BaseException:
                with suppress(Exception):
                    traceback.print_exc()
                os._exit(1)
    finally:
        if os.getpid() == server_pid:
            with suppress(ValueError, OSError):
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
            with suppress(ValueError, OSError):
                signal.set_wakeup_fd(-1)
            if server_sock is not None:
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
