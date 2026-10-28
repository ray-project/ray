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

_PRE_FORK_ENV_OVERRIDES = {
    "OMP_NUM_THREADS": "1",
    "JE_ARROW_MALLOC_CONF": "background_thread:false",
}


class ForkServerProcess:
    """Manages a pre-initialized Python fork server process and its Unix socket."""

    def __init__(
        self,
        socket_path: str,
        preload_modules: Optional[List[str]] = None,
        context: Optional["RuntimeEnvContext"] = None,
        cgroup_path: Optional[str] = None,
    ):
        self.socket_path = os.path.abspath(socket_path)
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
            start_new_session=True,
            preexec_fn=(
                ray._private.utils.set_kill_on_parent_death_linux
                if sys.platform.startswith("linux")
                else None
            ),
        )
        try:
            deadline = time.monotonic() + 30.0
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
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        Path(self.socket_path).unlink(missing_ok=True)
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
    ray._private.utils.try_import_each_module(modules)


def _prepare_child_environment(
    saved_pre_fork_env: Dict[str, Optional[str]],
    child_env: Dict[str, str],
    child_args: List[str],
    runtime_env_vars: Optional[Dict[str, str]] = None,
) -> None:
    for key, val in saved_pre_fork_env.items():
        if val is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = val

    os.environ.pop("RAY_JOB_ID", None)
    os.environ.update(child_env)
    if runtime_env_vars:
        ray._private.utils.update_envs(runtime_env_vars)
    if "PYTHONPATH" in child_env or (
        runtime_env_vars and "PYTHONPATH" in runtime_env_vars
    ):
        for p in reversed(os.environ.get("PYTHONPATH", "").split(os.pathsep)):
            if p and p not in sys.path:
                sys.path.insert(0, p)
        importlib.invalidate_caches()

    with suppress(ValueError, RuntimeError):
        omp_threads = int(os.environ.get("OMP_NUM_THREADS") or 0)
        if omp_threads > 0:
            for mod_name in ray_constants.THREAD_POOL_MODULES:
                if mod_name in sys.modules:
                    sys.modules[mod_name].set_num_threads(omp_threads)

    # Refresh C++ RayConfig and authentication token cache from updated os.environ.
    ray._raylet.Config.initialize("")
    ray._raylet.AuthenticationTokenLoader.instance().reset_cache()

    # Refresh import-time ray_constants and global_worker attributes that depend on os.environ.
    if any(
        k.startswith("RAY_") or k == "FUNCTION_SIZE_ERROR_THRESHOLD"
        for k in (*child_env, *(runtime_env_vars or ()))
    ):
        importlib.reload(ray_constants)
    gw = ray._private.worker.global_worker
    gw.original_visible_accelerator_ids = (
        ray._private.utils.get_visible_accelerator_ids()
    )
    gw._enable_record_actor_task_log = (
        ray_constants.RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING
    )
    if "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS" in child_env or (
        runtime_env_vars and "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS" in runtime_env_vars
    ):
        gw.serialization_context_map.clear()

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
    args = parser.parse_args(argv)

    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    # Keep library imports single-threaded before fork().
    saved_pre_fork_env = {k: os.environ.get(k) for k in _PRE_FORK_ENV_OVERRIDES}
    os.environ.update(_PRE_FORK_ENV_OVERRIDES)

    import ray._common.usage.usage_lib as _usage_lib  # noqa: F401
    import ray._private.workers.default_worker as default_worker

    try_install_uvloop()
    with suppress(Exception):
        ray._private.worker.global_worker.get_serialization_context()
    _preload_modules(args.preload_modules)

    gc.collect()
    gc.freeze()

    socket_path = os.path.abspath(args.socket_path)
    socket_dir = os.path.dirname(socket_path)
    os.makedirs(socket_dir, exist_ok=True)
    server_pid = os.getpid()
    tmp_socket_path = os.path.join(socket_dir, f".fs_{server_pid}.sock")
    Path(tmp_socket_path).unlink(missing_ok=True)

    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    published_ino: Optional[int] = None
    try:
        server_sock.bind(tmp_socket_path)
        os.chmod(tmp_socket_path, 0o600)
        tmp_ino = os.lstat(tmp_socket_path).st_ino
        server_sock.listen(128)
        os.replace(tmp_socket_path, socket_path)
        published_ino = tmp_ino

        signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
        sys.stdout.flush()
        sys.stderr.flush()

        while True:
            try:
                conn, _ = server_sock.accept()
            except InterruptedError:
                continue
            except OSError:
                break

            with conn:
                try:
                    conn.settimeout(2.0)
                    with conn.makefile("rb") as f:
                        line = f.readline()
                    if not line:
                        continue
                    req = json.loads(line)
                    if not isinstance(req, dict):
                        raise ValueError("Fork request must be a JSON object")
                    child_args = req.get("args", [])
                    child_env = req.get("env", {})
                    runtime_env_vars = req.get("runtime_env_vars", {})
                    if (
                        not isinstance(child_args, list)
                        or not isinstance(child_env, dict)
                        or not isinstance(runtime_env_vars, dict)
                    ):
                        raise ValueError(
                            "'args' must be a list and 'env'/'runtime_env_vars' must be dicts"
                        )
                    deadline_ms = req.get("deadline_ms")
                    if (
                        deadline_ms is not None
                        and time.time() * 1000.0 > float(deadline_ms)
                    ):
                        with suppress(OSError):
                            conn.sendall(b'{"error": "request expired"}\n')
                        continue
                    pid = os.fork()
                except Exception as e:
                    logger.warning(f"Failed to handle fork request: {e}")
                    continue

                if pid > 0:
                    with suppress(OSError):
                        os.setpgid(pid, pid)
                    try:
                        conn.sendall(json.dumps({"pid": pid}).encode("utf-8") + b"\n")
                    except OSError:
                        with suppress(OSError):
                            os.kill(pid, signal.SIGKILL)
                    continue

            with suppress(OSError):
                os.setpgrp()
            server_sock.close()
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGINT, signal.default_int_handler)
            signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
            _prepare_child_environment(
                saved_pre_fork_env, child_env, child_args, runtime_env_vars
            )
            return default_worker
    finally:
        if os.getpid() == server_pid:
            server_sock.close()
            Path(tmp_socket_path).unlink(missing_ok=True)
            if published_ino is not None:
                with suppress(OSError):
                    if os.lstat(socket_path).st_ino == published_ino:
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
