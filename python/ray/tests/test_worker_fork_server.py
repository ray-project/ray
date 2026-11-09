import asyncio
import ctypes
import gc
import importlib
import importlib.util
import io
import json
import logging
import os
import random
import select
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pytest

import ray
from ray._common.network_utils import build_address, find_free_port
from ray._common.test_utils import fetch_prometheus, wait_for_condition
from ray._private import ray_constants
from ray._private.internal_api import memory_summary
from ray._private.runtime_env.agent.runtime_env_agent import (
    _FORK_SERVER_ALLOWED_RUNTIME_ENV_KEYS,
    RuntimeEnvAgent,
    is_exec_bound_env_var,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.uv import UvPlugin
from ray._private.runtime_env.working_dir import WorkingDirPlugin
from ray._private.workers.fork_server import ForkServerProcess
from ray.cloudpickle import CloudPickler
from ray.core.generated import runtime_env_agent_pb2
from ray.runtime_env import RuntimeEnv

try:
    import scipy.linalg as _scipy_linalg  # noqa: F401

    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

HAS_POLARS = importlib.util.find_spec("polars") is not None

try:
    import torch
except ImportError:
    torch = None


def _pid_exists(pid: int) -> bool:
    """Return True if a live (non-zombie) process with `pid` exists."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    stat_path = Path(f"/proc/{pid}/stat")
    if stat_path.exists():
        try:
            content = stat_path.read_text()
            state = content.rsplit(")", 1)[1].split()[0]
            if state == "Z":
                return False
        except Exception:
            pass
    return True


def _find_fork_server_pids(session_dir: str) -> Dict[str, int]:
    """Return mapping of socket path -> PID for running fork_server processes."""
    result: Dict[str, int] = {}
    proc_root = Path("/proc")
    if not proc_root.exists():
        return result
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (
                (entry / "cmdline")
                .read_bytes()
                .decode("utf-8", errors="ignore")
                .split("\x00")
            )
        except Exception:
            continue
        if not cmdline or cmdline[0].startswith("ray::"):
            continue
        basenames = [os.path.basename(part) for part in cmdline]
        if "_cgroup_bootstrap.py" in basenames or "setup_worker.py" in basenames:
            continue
        if not any(
            "ray._private.workers.fork_server" in part
            or "fork_server.py" == os.path.basename(part)
            for part in cmdline
        ):
            continue
        for i, arg in enumerate(cmdline):
            if arg == "--socket-path" and i + 1 < len(cmdline):
                sock = cmdline[i + 1]
                if session_dir in sock:
                    result[sock] = int(entry.name)
            elif arg.startswith("--socket-path="):
                sock = arg.split("=", 1)[1]
                if session_dir in sock:
                    result[sock] = int(entry.name)
    return result


def _find_child_pids(parent_pid: int) -> List[int]:
    """Return live non-zombie child PIDs whose parent PID equals `parent_pid`."""
    children: List[int] = []
    proc_root = Path("/proc")
    if not proc_root.exists():
        return children
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            content = (entry / "stat").read_text()
            fields = content.rsplit(")", 1)[1].split()
            state = fields[0]
            ppid = int(fields[1])
            if state != "Z" and ppid == parent_pid:
                children.append(pid)
        except Exception:
            continue
    return sorted(children)


def _find_io_worker_ppids(session_dir: str) -> Dict[str, List[int]]:
    """Return parent PIDs of live SPILL_WORKER and RESTORE_WORKER processes in `session_dir`."""
    logs_dir = Path(session_dir) / "logs"
    result: Dict[str, List[int]] = {"spill": [], "restore": []}
    for kind, pattern in (
        ("spill", "python-core-spill_worker-*_*.log"),
        ("restore", "python-core-restore_worker-*_*.log"),
    ):
        for log_path in logs_dir.glob(pattern):
            try:
                pid = int(log_path.stem.rsplit("_", 1)[1])
                content = Path(f"/proc/{pid}/stat").read_text()
                fields = content.rsplit(")", 1)[1].split()
                if fields[0] != "Z":
                    result[kind].append(int(fields[1]))
            except Exception:
                continue
    return result


def _make_openblas_threads_by_lib_getter():
    """Return a local closure mapping loaded OpenBLAS .so basename -> active thread count."""

    def _get_openblas_threads_by_lib() -> Dict[str, int]:
        out: Dict[str, int] = {}
        try:
            with open("/proc/self/maps", "r", encoding="utf-8", errors="ignore") as f:
                maps_lines = f.readlines()
        except OSError:
            return out
        seen = set()
        for line in maps_lines:
            if "openblas" not in line.lower():
                continue
            parts = line.split(None, 5)
            if len(parts) < 6:
                continue
            so_path = parts[5].strip()
            if not so_path.startswith("/") or so_path in seen:
                continue
            seen.add(so_path)
            try:
                lib = ctypes.CDLL(so_path, mode=os.RTLD_NOLOAD)
            except Exception:
                continue
            for sym in (
                "scipy_openblas_get_num_threads64_",
                "scipy_openblas_get_num_threads",
                "openblas_get_num_threads64_",
                "openblas_get_num_threads",
            ):
                fn = getattr(lib, sym, None)
                if fn is not None:
                    try:
                        fn.argtypes = []
                        fn.restype = ctypes.c_int
                        out[os.path.basename(so_path)] = int(fn())
                        break
                    except Exception:
                        pass
        return out

    return _get_openblas_threads_by_lib


def _make_openblas_threads_getter():
    """Return a local closure (serialized by value by cloudpickle) querying OpenBLAS thread count."""
    get_by_lib = _make_openblas_threads_by_lib_getter()

    def _get_openblas_num_threads() -> Optional[int]:
        vals = list(get_by_lib().values())
        return vals[0] if vals else None

    return _get_openblas_num_threads


@pytest.mark.parametrize(
    "preload_modules,expected_error",
    [
        (["os", "sys", "urllib.parse"], None),
        ([], None),
        ("os", TypeError),
        ([123], TypeError),
        ([""], ValueError),
        (["123invalid"], ValueError),
        (["bad..module"], ValueError),
    ],
)
def test_runtime_env_preload_modules_validation(preload_modules, expected_error):
    """Verify RuntimeEnv validates preload_modules as a list of non-empty strings."""
    if expected_error is None:
        env = RuntimeEnv(preload_modules=preload_modules)
        assert env.preload_modules() == preload_modules
    else:
        with pytest.raises(expected_error):
            RuntimeEnv(preload_modules=preload_modules)


def _find_idle_python_worker_pids(session_dir: str) -> List[int]:
    """Return live ray::IDLE worker PIDs connected to `session_dir`."""
    pids: List[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (
                (entry / "cmdline")
                .read_bytes()
                .decode("utf-8", errors="ignore")
                .split("\x00")
            )
            if not cmdline or cmdline[0] != "ray::IDLE":
                continue
            environ = (entry / "environ").read_bytes().decode("utf-8", errors="ignore")
            if session_dir in environ or any(session_dir in a for a in cmdline):
                pids.append(int(entry.name))
        except Exception:
            continue
    return sorted(pids)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_default_workers_signals_and_process_groups(
    shutdown_only, monkeypatch
):
    """Verify default ForkServer disables boot prestarting, remains single-threaded, forks workers cleanly with refreshed t0 and filtered stack frames, supports SIGINT cancellation, and isolates actor process groups."""
    monkeypatch.setenv("RAY_record_ref_creation_sites", "1")
    ctx = ray.init(
        num_cpus=4,
        include_dashboard=False,
        _system_config={
            "enable_worker_fork_server": True,
            "record_ref_creation_sites": True,
        },
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(lambda: os.path.exists(default_sock), timeout=10)
    assert stat.S_IMODE(os.stat(default_sock).st_mode) == 0o600
    # Boot-time Python worker prestarting is disabled on Linux under fork server.
    assert _find_idle_python_worker_pids(session_dir) == []

    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    default_fs_pgid = os.getpgid(default_fs_pid)
    assert len(os.listdir(f"/proc/{default_fs_pid}/task")) == 1
    post_boot_marker = time.time()
    time.sleep(0.05)

    @ray.remote
    def get_worker_info(idx: int):
        time.sleep(0.15)
        pid = os.getpid()
        return {
            "idx": idx,
            "pid": pid,
            "ppid": os.getppid(),
            "pgid": os.getpgid(0),
            "freeze_count": gc.get_freeze_count(),
            "rand": random.random(),
            "np_rand": float(np.random.rand()),
            "worker_t0": ray._private.worker.t0,
        }

    results = ray.get([get_worker_info.remote(i) for i in range(4)])
    assert len({r["pid"] for r in results}) == 4
    for r in results:
        assert r["ppid"] == default_fs_pid
        assert r["pgid"] == r["pid"]
        assert r["pgid"] != default_fs_pgid
        assert r["freeze_count"] > 0
        assert r["worker_t0"] >= post_boot_marker

    assert len({r["rand"] for r in results}) == 4
    assert len({r["np_rand"] for r in results}) == 4

    # Verify graceful task cancellation via SIGINT works on forked workers, and
    # Cython get_py_stack omits fork_server.py / default_worker.py frames.
    @ray.remote
    class SignalTracker:
        def __init__(self):
            self.started = False
            self.caught_keyboard_interrupt = False
            self._pinned_ref = ray.put(b"probe" * 256)

        def mark_started(self):
            self.started = True

        def mark_interrupted(self):
            self.caught_keyboard_interrupt = True

        def get_state(self):
            return self.started, self.caught_keyboard_interrupt

    tracker = SignalTracker.remote()
    assert ray.get(tracker.get_state.remote()) == (False, False)
    mem_info = memory_summary(ctx.address_info["address"], line_wrap=False)
    assert "(put object)" in mem_info
    assert "test_worker_fork_server.py" in mem_info
    assert "_private/workers/fork_server.py" not in mem_info
    assert "_private/workers/default_worker.py" not in mem_info

    @ray.remote
    def cancellable_task(tracker_handle):
        try:
            ray.get(tracker_handle.mark_started.remote())
            while True:
                time.sleep(0.05)
        except KeyboardInterrupt:
            ray.get(tracker_handle.mark_interrupted.remote())
            raise

    task_ref = cancellable_task.remote(tracker)
    wait_for_condition(lambda: ray.get(tracker.get_state.remote())[0], timeout=10)
    ray.cancel(task_ref, force=False)
    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(task_ref)
    assert ray.get(tracker.get_state.remote())[1] is True

    # Verify killing an actor kills its process group (including child subprocesses)
    # without harming the ForkServer.
    @ray.remote
    class SubprocessActor:
        def __init__(self):
            self.child = subprocess.Popen(["sleep", "300"])

        def get_pids(self):
            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "pgid": os.getpgid(0),
                "child_pid": self.child.pid,
                "child_pgid": os.getpgid(self.child.pid),
            }

    actor = SubprocessActor.remote()
    info = ray.get(actor.get_pids.remote())
    assert info["ppid"] == default_fs_pid
    assert info["pgid"] == info["pid"]
    assert info["child_pgid"] == info["pid"]
    assert _pid_exists(info["child_pid"])
    default_fs_fds = _proc_fd_map(default_fs_pid)
    actor_fds = _proc_fd_map(info["pid"])
    assert os.path.realpath(actor_fds["0"]) == os.path.realpath(os.devnull)
    fs_ipc_targets = {
        target
        for fd_num, target in default_fs_fds.items()
        if fd_num not in ("0", "1", "2")
        and (target.startswith("pipe:[") or target.startswith("socket:["))
    }
    assert fs_ipc_targets and fs_ipc_targets.isdisjoint(actor_fds.values())

    ray.kill(actor)
    wait_for_condition(lambda: not _pid_exists(info["pid"]), timeout=10)
    wait_for_condition(lambda: not _pid_exists(info["child_pid"]), timeout=10)
    assert _pid_exists(default_fs_pid)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_timeout_and_dead_server_fallback(shutdown_only, monkeypatch):
    """Verify 3-strike 100ms timeout hysteresis with zero late duplicate workers, default
    auto-respawn on fs_0, and cached runtime_env lazy respawn at deterministic fs_<9b64>."""
    monkeypatch.setenv("RAY_fork_server_monitor_interval_s", "0.05")
    monkeypatch.setenv("RAY_fork_server_respawn_backoff_base_s", "0.6")
    ctx = ray.init(
        num_cpus=1,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

    @ray.remote(num_cpus=1)
    class PingActor:
        def get_ids(self):
            return (
                os.getpid(),
                os.getppid(),
                ray.get_runtime_context().get_worker_id(),
            )

    a1 = PingActor.remote()
    pid1, ppid1, wid1 = ray.get(a1.get_ids.remote())
    assert ppid1 == default_fs_pid

    # Pause ForkServer with SIGSTOP so kernel accepts AF_UNIX connections in backlog
    # without replying; 3 consecutive ~100ms timeouts fall back cleanly with fresh
    # WorkerIDs and activate the 5s cooldown so a4 bypasses the stalled socket immediately.
    os.kill(default_fs_pid, signal.SIGSTOP)
    fallback_wids = {wid1}
    try:
        for _ in range(3):
            t0 = time.monotonic()
            fb = PingActor.options(num_cpus=0).remote()
            _, fb_ppid, fb_wid = ray.get(fb.get_ids.remote())
            assert fb_ppid != default_fs_pid
            assert fb_wid not in fallback_wids
            fallback_wids.add(fb_wid)
            assert time.monotonic() - t0 < 4.0

        a3 = PingActor.options(num_cpus=0).remote()
        _, ppid3, wid3 = ray.get(a3.get_ids.remote())
        assert ppid3 != default_fs_pid
        assert wid3 not in fallback_wids
    finally:
        os.kill(default_fs_pid, signal.SIGCONT)

    # After SIGCONT, the ForkServer accepts the 3 stale backlog connections, sees
    # POLLRDHUP / EPIPE from the closed client sockets before running worker startup,
    # and drops them cleanly so only a1 remains under default_fs_pid.
    wait_for_condition(lambda: _find_child_pids(default_fs_pid) == [pid1], timeout=5)

    # Cached runtime_env lazy respawn: keep a4 alive so kill_env stays in _env_cache
    # with ref_cnt >= 1, SIGKILL env_fs_pid, then verify a5 lazily respawns a new
    # fork server at the exact same deterministic 12-char fs_<9b64> socket path!
    kill_env = {"preload_modules": ["json"], "env_vars": {"FORK_SERVER_KILL_TEST": "1"}}
    a4 = PingActor.options(num_cpus=0, runtime_env=kill_env).remote()
    pid4, env_fs_pid, _ = ray.get(a4.get_ids.remote())
    assert env_fs_pid != default_fs_pid
    env_socks = [
        s for s, p in _find_fork_server_pids(session_dir).items() if p == env_fs_pid
    ]
    assert len(env_socks) == 1
    expected_env_sock = env_socks[0]
    sock_basename = os.path.basename(expected_env_sock)
    assert sock_basename.startswith("fs_") and len(sock_basename) == 12

    os.kill(env_fs_pid, signal.SIGKILL)
    wait_for_condition(lambda: not _pid_exists(env_fs_pid), timeout=10)
    # Pre-crash actor a4 continues running independently after its fork server dies.
    assert ray.get(a4.get_ids.remote())[0] == pid4

    a5 = PingActor.options(num_cpus=0, runtime_env=kill_env).remote()
    _, ppid5, _ = ray.get(a5.get_ids.remote())
    assert ppid5 != env_fs_pid
    assert ppid5 != default_fs_pid
    assert _find_fork_server_pids(session_dir).get(expected_env_sock) == ppid5

    # Default {} supervised auto-respawn: SIGKILL default_fs_pid, verify a1 survives,
    # an immediate actor falls back seamlessly to execvpe during the 0.6s backoff
    # window, background supervisor atomically replaces fs_0 (new st_ino clears
    # cooldown), and a6 forks from the respawned server.
    old_default_ino = os.stat(default_sock).st_ino
    os.kill(default_fs_pid, signal.SIGKILL)
    wait_for_condition(lambda: not _pid_exists(default_fs_pid), timeout=5)
    assert ray.get(a1.get_ids.remote())[0] == pid1

    a_dead_fb = PingActor.options(num_cpus=0).remote()
    _, ppid_dead_fb, _ = ray.get(a_dead_fb.get_ids.remote())
    assert ppid_dead_fb != default_fs_pid

    def _default_socket_respawned() -> bool:
        try:
            cur_ino = os.stat(default_sock).st_ino
        except OSError:
            return False
        fs_pid = _find_fork_server_pids(session_dir).get(default_sock)
        return cur_ino != old_default_ino and fs_pid not in (None, default_fs_pid)

    wait_for_condition(_default_socket_respawned, timeout=10)
    new_default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    a6 = PingActor.options(num_cpus=0).remote()
    _, ppid6, _ = ray.get(a6.get_ids.remote())
    assert ppid6 == new_default_fs_pid


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
@pytest.mark.skipif(torch is None, reason="PyTorch is not installed.")
def test_fork_server_per_runtime_env_preload_env_vars_and_cleanup(
    shutdown_only, monkeypatch, tmp_path
):
    """Verify per-runtime_env ForkServer preloads modules (including py_modules), interpolates ${VAR}/$VAR, runs worker_process_setup_hook, configures OMP_NUM_THREADS for torch, reuses idle ForkServer across scale-to-zero, and cleans up after idle TTL."""
    monkeypatch.setenv("EXISTING_HOST_VAR", "host_val")
    pkg_dir = tmp_path / "custom_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text(
        "import os\n"
        "import ray\n"
        "PRELOADED_PID = os.getpid()\n"
        "PRELOADED_OBJECT_ID = ray._raylet.ObjectID.from_random().hex()\n"
        "@ray.remote\n"
        "def _preloaded_remote_fn():\n"
        "    return 1\n"
        "def setup_hook():\n"
        "    os.environ['SETUP_HOOK_RAN_PID'] = str(os.getpid())\n"
    )

    ctx = ray.init(
        num_cpus=4,
        include_dashboard=False,
        _system_config={
            "enable_worker_fork_server": True,
            "runtime_env_max_idle_fork_servers": 4,
            "runtime_env_fork_server_idle_ttl_s": 3,
            "idle_worker_killing_time_threshold_ms": 200,
        },
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    _get_openblas_num_threads = _make_openblas_threads_getter()

    runtime_env = {
        "py_modules": [f"local://{tmp_path}"],
        "preload_modules": ["json", "torch", "custom_pkg"],
        "env_vars": {
            "__RAY_WORKER_PROCESS_SETUP_HOOK_ENV_VAR": "custom_pkg.setup_hook",
            "OMP_NUM_THREADS": "3",
            "A_JOB_REF": "job_${RAY_JOB_ID}",
            "A_BASE_DIR": "/tmp/ray_fork_test",
            "B_SUB_DIR": "${A_BASE_DIR}/sub",
            "C_NESTED_DIR": "${B_SUB_DIR}/nested",
            "D_DOLLAR_DIR": "$C_NESTED_DIR/leaf",
            "E_WITH_EXISTING": "prefix_${EXISTING_HOST_VAR}_suffix",
            "F_UNKNOWN_REF": "${DEFINITELY_UNSET_VAR_XYZ}/tail",
        },
    }

    @ray.remote(runtime_env=runtime_env, max_calls=1)
    def check_env_task(idx: int = 0):
        time.sleep(0.1)
        torch_preloaded = "torch" in sys.modules
        custom_pkg_preloaded = "custom_pkg" in sys.modules
        import custom_pkg
        import torch as _torch

        return {
            "idx": idx,
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "torch_preloaded": torch_preloaded,
            "custom_pkg_preloaded": custom_pkg_preloaded,
            "custom_pkg_pid": custom_pkg.PRELOADED_PID,
            "preloaded_obj_id": custom_pkg.PRELOADED_OBJECT_ID,
            "worker_random_obj_id": ray._raylet.ObjectID.from_random().hex(),
            "setup_hook_pid": int(os.environ.get("SETUP_HOOK_RAN_PID", "0")),
            "tensor_sum": float(_torch.ones((2, 2)).sum().item()),
            "omp_num_threads": os.environ.get("OMP_NUM_THREADS"),
            "torch_threads": _torch.get_num_threads(),
            "openblas_threads": _get_openblas_num_threads(),
            "A_JOB_REF": os.environ.get("A_JOB_REF"),
            "A_BASE_DIR": os.environ.get("A_BASE_DIR"),
            "B_SUB_DIR": os.environ.get("B_SUB_DIR"),
            "C_NESTED_DIR": os.environ.get("C_NESTED_DIR"),
            "D_DOLLAR_DIR": os.environ.get("D_DOLLAR_DIR"),
            "E_WITH_EXISTING": os.environ.get("E_WITH_EXISTING"),
            "F_UNKNOWN_REF": os.environ.get("F_UNKNOWN_REF"),
        }

    @ray.remote(runtime_env=runtime_env)
    class EnvTorchActor:
        def __init__(self):
            self.torch_preloaded = "torch" in sys.modules
            self.custom_pkg_preloaded = "custom_pkg" in sys.modules

        def info(self):
            import custom_pkg
            import torch as _torch

            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "torch_preloaded": self.torch_preloaded,
                "custom_pkg_preloaded": self.custom_pkg_preloaded,
                "custom_pkg_pid": custom_pkg.PRELOADED_PID,
                "setup_hook_pid": int(os.environ.get("SETUP_HOOK_RAN_PID", "0")),
                "tensor_sum": float(_torch.ones((3, 3)).sum().item()),
                "omp_num_threads": os.environ.get("OMP_NUM_THREADS"),
                "torch_threads": _torch.get_num_threads(),
                "openblas_threads": _get_openblas_num_threads(),
                "C_NESTED_DIR": os.environ.get("C_NESTED_DIR"),
                "PER_ACTOR_RANK": os.environ.get("PER_ACTOR_RANK"),
            }

    # Verify env_vars-only runtime_env reuses the default fork server (fork_server_default.sock).
    @ray.remote(runtime_env={"env_vars": {"PER_ACTOR_RANK": "0"}})
    class EnvVarsOnlyActor:
        def info(self):
            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "PER_ACTOR_RANK": os.environ.get("PER_ACTOR_RANK"),
            }

    env_vars_only_actor = EnvVarsOnlyActor.remote()
    env_vars_only_res = ray.get(env_vars_only_actor.info.remote())
    assert env_vars_only_res["ppid"] == default_fs_pid
    assert env_vars_only_res["PER_ACTOR_RANK"] == "0"
    ray.kill(env_vars_only_actor)

    # Create `actor` (num_cpus=0 so 4 CPUs remain free for the 4 max_calls=1 tasks)
    # first to warm the runtime_env fork server cleanly without leaving extra idle
    # speculative workers cached in Raylet's soft-cap pool.
    actor = EnvTorchActor.options(num_cpus=0).remote()
    actor_res = ray.get(actor.info.remote())
    task_results = ray.get([check_env_task.remote(i) for i in range(4)])
    task_res = task_results[0]

    # Verify another actor with the same py_modules + preload_modules but different
    # per-actor env_vars shares the exact same ForkServer PID.
    other_env = {
        "py_modules": [f"local://{tmp_path}"],
        "preload_modules": ["json", "torch", "custom_pkg"],
        "env_vars": {"PER_ACTOR_RANK": "7", "OMP_NUM_THREADS": "2"},
    }
    actor_other_env = EnvTorchActor.options(runtime_env=other_env).remote()
    other_res = ray.get(actor_other_env.info.remote())

    env_fs_pid = actor_res["ppid"]
    assert env_fs_pid != default_fs_pid
    assert len(os.listdir(f"/proc/{env_fs_pid}/task")) == 1
    assert len({r["pid"] for r in task_results}) == 4
    assert len({r["worker_random_obj_id"] for r in task_results}) == 4
    for r in task_results:
        assert r["ppid"] == env_fs_pid
        assert r["preloaded_obj_id"] == task_res["preloaded_obj_id"]
        assert r["worker_random_obj_id"] != r["preloaded_obj_id"]
    assert task_res["torch_preloaded"] is True
    assert task_res["custom_pkg_preloaded"] is True
    # custom_pkg was imported inside the ForkServer before os.fork(), so its recorded PID
    # matches the ForkServer PID, while _setup_hook ran post-fork in the child worker PID.
    assert task_res["custom_pkg_pid"] == env_fs_pid
    assert task_res["setup_hook_pid"] == task_res["pid"]
    assert task_res["tensor_sum"] == 4.0
    assert task_res["omp_num_threads"] == "3"
    assert task_res["torch_threads"] == 3
    assert task_res["openblas_threads"] == 3
    assert task_res["A_JOB_REF"] == f"job_{ray.get_runtime_context().get_job_id()}"
    assert task_res["A_BASE_DIR"] == "/tmp/ray_fork_test"
    assert task_res["B_SUB_DIR"] == "/tmp/ray_fork_test/sub"
    assert task_res["C_NESTED_DIR"] == "/tmp/ray_fork_test/sub/nested"
    assert task_res["D_DOLLAR_DIR"] == "/tmp/ray_fork_test/sub/nested/leaf"
    assert task_res["E_WITH_EXISTING"] == "prefix_host_val_suffix"
    assert task_res["F_UNKNOWN_REF"] == "/tail"

    assert actor_res["torch_preloaded"] is True
    assert actor_res["custom_pkg_preloaded"] is True
    assert actor_res["custom_pkg_pid"] == env_fs_pid
    assert actor_res["setup_hook_pid"] == actor_res["pid"]
    assert actor_res["tensor_sum"] == 9.0
    assert actor_res["omp_num_threads"] == "3"
    assert actor_res["torch_threads"] == 3
    assert actor_res["openblas_threads"] == 3
    assert actor_res["C_NESTED_DIR"] == "/tmp/ray_fork_test/sub/nested"

    assert other_res["ppid"] == env_fs_pid
    assert other_res["custom_pkg_pid"] == env_fs_pid
    assert other_res["PER_ACTOR_RANK"] == "7"
    assert other_res["omp_num_threads"] == "2"
    assert other_res["torch_threads"] == 2
    assert other_res["openblas_threads"] == 2
    assert other_res["C_NESTED_DIR"] is None
    ray.kill(actor_other_env)
    wait_for_condition(lambda: not _pid_exists(other_res["pid"]), timeout=15)

    fs_pids = _find_fork_server_pids(session_dir)
    env_socks = [s for s, p in fs_pids.items() if p == env_fs_pid]
    assert len(env_socks) == 1
    env_sock = env_socks[0]
    assert os.path.exists(env_sock)
    assert stat.S_IMODE(os.stat(env_sock).st_mode) == 0o600

    # Killing the actor and draining any FIFO speculative runtime_env idle workers from
    # Raylet's soft-cap pool drops the runtime_env refcount to 0; the ForkServer remains
    # warm in the idle cache so a subsequent actor reuses it without re-importing torch.
    @ray.remote
    def _fill_default_idle():
        time.sleep(0.1)
        return os.getpid()

    ray.kill(actor)
    wait_for_condition(lambda: not _pid_exists(actor_res["pid"]), timeout=15)
    assert len(set(ray.get([_fill_default_idle.remote() for _ in range(4)]))) == 4
    assert _pid_exists(env_fs_pid)

    actor2 = EnvTorchActor.remote()
    actor2_res = ray.get(actor2.info.remote())
    assert actor2_res["ppid"] == env_fs_pid
    assert actor2_res["custom_pkg_pid"] == env_fs_pid

    # Once actor2 is killed and the idle TTL expires, the ForkServer is stopped.
    ray.kill(actor2)
    wait_for_condition(lambda: not _pid_exists(actor2_res["pid"]), timeout=15)
    wait_for_condition(lambda: not _pid_exists(env_fs_pid), timeout=15)
    wait_for_condition(lambda: not os.path.exists(env_sock), timeout=15)
    assert _pid_exists(default_fs_pid)

    ray.shutdown()
    wait_for_condition(lambda: not _pid_exists(default_fs_pid), timeout=15)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
@pytest.mark.skipif(torch is None, reason="PyTorch is not installed.")
def test_fork_server_preloaded_torch_cow_memory_and_thread_reconfig(shutdown_only):
    """Verify CoW page sharing (Shared_Dirty vs Private_Dirty) and post-fork torch thread reconfiguration when torch is preloaded via _system_config."""
    ctx = ray.init(
        num_cpus=4,
        include_dashboard=False,
        _system_config={
            "enable_worker_fork_server": True,
            "preload_python_modules": ["torch"],
        },
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    assert len(os.listdir(f"/proc/{default_fs_pid}/task")) == 1
    _get_openblas_num_threads = _make_openblas_threads_getter()

    @ray.remote
    class CoWTorchActor:
        def __init__(self):
            self.torch_preloaded = "torch" in sys.modules

        def measure(self):
            import torch as _torch

            x = _torch.ones((64, 64))
            _ = float((x @ x).sum().item())
            smaps: Dict[str, int] = {}
            rollup = Path("/proc/self/smaps_rollup")
            if rollup.exists():
                for line in rollup.read_text().splitlines():
                    parts = line.split()
                    if len(parts) >= 2 and parts[0].endswith(":"):
                        try:
                            smaps[parts[0][:-1]] = int(parts[1])
                        except ValueError:
                            pass
            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "torch_preloaded": self.torch_preloaded,
                "smaps": smaps,
            }

    actors = [CoWTorchActor.remote() for _ in range(4)]
    measurements = ray.get([a.measure.remote() for a in actors])
    for m in measurements:
        assert m["ppid"] == default_fs_pid
        assert m["torch_preloaded"] is True
        shared_dirty_kb = m["smaps"].get("Shared_Dirty", 0)
        private_dirty_kb = m["smaps"].get("Private_Dirty", 0)
        assert (
            shared_dirty_kb > 50 * 1024
        ), f"Expected >50 MiB Shared_Dirty, got {m['smaps']}"
        assert (
            private_dirty_kb < 50 * 1024
        ), f"Expected <50 MiB Private_Dirty, got {m['smaps']}"

    for a in actors:
        ray.kill(a)

    # Verify num_cpus=2 reconfigures torch and OpenBLAS threads to 2, and num_cpus=1 resets to 1.
    @ray.remote(num_cpus=2)
    def check_threads_two():
        import torch as _torch

        return {
            "ppid": os.getppid(),
            "omp_env": os.environ.get("OMP_NUM_THREADS"),
            "torch_threads": _torch.get_num_threads(),
            "openblas_threads": _get_openblas_num_threads(),
        }

    @ray.remote(num_cpus=1)
    def check_threads_one():
        import torch as _torch

        return {
            "ppid": os.getppid(),
            "omp_env": os.environ.get("OMP_NUM_THREADS"),
            "torch_threads": _torch.get_num_threads(),
            "openblas_threads": _get_openblas_num_threads(),
        }

    res2 = ray.get(check_threads_two.remote())
    assert res2["ppid"] == default_fs_pid
    assert res2["omp_env"] == "2"
    assert res2["torch_threads"] == 2
    assert res2["openblas_threads"] == 2

    res1 = ray.get(check_threads_one.remote())
    assert res1["ppid"] == default_fs_pid
    assert res1["omp_env"] == "1"
    assert res1["torch_threads"] == 1
    assert res1["openblas_threads"] == 1


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_object_spilling_and_restoration_io_workers(
    shutdown_only, tmp_path
):
    """Verify SPILL_WORKER and RESTORE_WORKER I/O workers bypass the fork server
    via StartProcess instead of inheriting preloaded ML libraries."""
    spill_dir = tmp_path / "spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "enable_worker_fork_server": True,
            "preload_python_modules": ["json"],
            "max_io_workers": 2,
            "min_spilling_size": 1,
            "object_spilling_config": json.dumps(
                {"type": "filesystem", "params": {"directory_path": str(spill_dir)}}
            ),
        },
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

    refs = [ray.put(np.full((10 * 1024 * 1024,), i, dtype=np.uint8)) for i in range(10)]
    for i, ref in enumerate(refs):
        arr = ray.get(ref)
        assert int(arr[0]) == i
        assert int(arr[-1]) == i
    io_ppids = _find_io_worker_ppids(session_dir)
    assert len(io_ppids["spill"]) > 0
    assert len(io_ppids["restore"]) > 0
    assert all(ppid != default_fs_pid for ppid in io_ppids["spill"])
    assert all(ppid != default_fs_pid for ppid in io_ppids["restore"])
    assert _pid_exists(default_fs_pid)


def _send_raw_fork_request_with_fd(sock_path: str) -> Tuple[int, int]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(5.0)
        s.connect(sock_path)
        req = {
            "args": [],
            "env": dict(os.environ),
            "runtime_env_vars": {},
        }
        s.sendall((json.dumps(req) + "\n").encode("utf-8"))
        msg, fds, _, _ = socket.recv_fds(s, 4096, 2)
    resp = json.loads(msg.decode("utf-8"))
    assert "error" not in resp, resp
    assert len(fds) == 1, fds
    return int(resp["pid"]), fds[0]


def _send_raw_fork_request(sock_path: str) -> int:
    pid, life_r = _send_raw_fork_request_with_fd(sock_path)
    os.close(life_r)
    return pid


def _proc_fd_map(pid: int) -> Dict[str, str]:
    fd_dir = Path(f"/proc/{pid}/fd")
    result: Dict[str, str] = {}
    for entry in fd_dir.iterdir():
        try:
            result[entry.name] = os.readlink(entry)
        except OSError:
            continue
    return result


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_stdin_lifeline_and_threadpool_spawn(tmp_path, monkeypatch):
    """Verify ForkServerProcess survives spawning thread exit (posix_spawn), closes
    the lifeline FD in forked children, and unlinks its socket on parent SIGKILL."""
    # Preloaded test helper replaces default_worker.initialize_default_worker so
    # standalone forked children stay alive after _prepare_child_environment() without
    # needing a live Raylet connection.
    (tmp_path / "standalone_child_hold.py").write_text(
        "import json, os, pathlib, time\n"
        "import ray._private.workers.default_worker as _dw\n"
        f"_out_dir = pathlib.Path({str(tmp_path)!r})\n"
        "def _hold():\n"
        "    (_out_dir / f'child_env_{os.getpid()}.json').write_text(json.dumps({\n"
        "        'OPENBLAS_NUM_THREADS': os.environ.get('OPENBLAS_NUM_THREADS'),\n"
        "        'MKL_NUM_THREADS': os.environ.get('MKL_NUM_THREADS'),\n"
        "    }))\n"
        "    time.sleep(300)\n"
        "_dw.initialize_default_worker = _hold\n"
    )
    ray_python = str(Path(ray.__file__).resolve().parents[1])
    test_pypath = f"{tmp_path}{os.pathsep}{ray_python}"
    monkeypatch.setenv("PYTHONPATH", test_pypath)

    # (a) Spawning inside a short-lived ThreadPoolExecutor worker thread must NOT
    # kill the fork server when that thread exits (unlike PR_SET_PDEATHSIG).
    sock_a = str(tmp_path / "fs_thread.sock")

    def _spawn_in_worker_thread() -> ForkServerProcess:
        return ForkServerProcess(
            socket_path=sock_a, preload_modules=["standalone_child_hold"]
        )

    with ThreadPoolExecutor(max_workers=1) as pool:
        fs_a = pool.submit(_spawn_in_worker_thread).result()
    child_a_pid = None
    life_r_a = -1
    try:
        asyncio.run(fs_a.wait_ready(timeout_s=15.0))
        time.sleep(0.1)
        assert fs_a.is_alive()
        assert os.path.exists(sock_a)
        child_a_pid, life_r_a = _send_raw_fork_request_with_fd(sock_a)
        wait_for_condition(lambda: _pid_exists(child_a_pid), timeout=5)
        # Lifetime pipe stays silent while child is alive, then hits EOF immediately on kill.
        readable, _, _ = select.select([life_r_a], [], [], 0.0)
        assert readable == []
        os.kill(child_a_pid, signal.SIGKILL)
        readable, _, _ = select.select([life_r_a], [], [], 5.0)
        assert readable == [life_r_a]
        assert os.read(life_r_a, 16) == b""
    finally:
        if life_r_a >= 0:
            os.close(life_r_a)
        if child_a_pid is not None and _pid_exists(child_a_pid):
            os.kill(child_a_pid, signal.SIGKILL)
        fs_a.stop()
    assert not os.path.exists(sock_a)

    # (b) Hold ForkServerProcess in a helper parent process, fork a child worker,
    # inspect /proc/<child_pid>/fd against /proc/<fs_pid>/fd to prove zero leaked
    # lifeline pipe or listening socket FDs and /dev/null stdin, then SIGKILL (-9)
    # the helper parent: fork_server.py must exit cleanly, unlink its socket, AND
    # leave the already-forked child worker alive.
    sock_b = str(tmp_path / "fs_sigkill.sock")
    parent_script = tmp_path / "parent_holder.py"
    parent_script.write_text(
        "import asyncio, time\n"
        "from ray._private.workers.fork_server import ForkServerProcess\n"
        f"fs = ForkServerProcess(socket_path={sock_b!r}, preload_modules=['standalone_child_hold'])\n"
        "asyncio.run(fs.wait_ready(timeout_s=15.0))\n"
        "print(fs.process.pid, flush=True)\n"
        "time.sleep(300)\n"
    )
    holder_env = dict(os.environ)
    holder_env["PYTHONPATH"] = test_pypath
    holder_env["OPENBLAS_NUM_THREADS"] = "4"
    holder_env["MKL_NUM_THREADS"] = "4"
    holder = subprocess.Popen(
        [sys.executable, str(parent_script)],
        stdout=subprocess.PIPE,
        env=holder_env,
        text=True,
    )
    child_b_pid = None
    fs_b_pid = None
    try:
        fs_b_pid = int(holder.stdout.readline().strip())
        assert _pid_exists(fs_b_pid)
        assert os.path.exists(sock_b)
        fs_status = Path(f"/proc/{fs_b_pid}/status").read_text()
        assert "Threads:\t1\n" in fs_status, fs_status

        fs_fds = _proc_fd_map(fs_b_pid)
        assert os.path.realpath(fs_fds["0"]) == os.path.realpath(os.devnull)
        fs_pipes_and_socks = {
            target
            for fd_num, target in fs_fds.items()
            if fd_num not in ("0", "1", "2")
            and (target.startswith("pipe:[") or target.startswith("socket:["))
        }
        # Expect both the lifeline stdin pipe and the AF_UNIX listening socket in fs_b_pid.
        assert any(t.startswith("pipe:[") for t in fs_pipes_and_socks), fs_fds
        assert any(t.startswith("socket:[") for t in fs_pipes_and_socks), fs_fds

        child_b_pid = _send_raw_fork_request(sock_b)
        wait_for_condition(lambda: _pid_exists(child_b_pid), timeout=5)

        def _child_fds_clean() -> bool:
            child_fds = _proc_fd_map(child_b_pid)
            return (
                "0" in child_fds
                and os.path.realpath(child_fds["0"]) == os.path.realpath(os.devnull)
                and fs_pipes_and_socks.isdisjoint(
                    target
                    for fd_num, target in child_fds.items()
                    if fd_num not in ("1", "2")
                )
            )

        wait_for_condition(_child_fds_clean, timeout=5)
        child_env_file = tmp_path / f"child_env_{child_b_pid}.json"
        wait_for_condition(lambda: child_env_file.exists(), timeout=5)
        assert json.loads(child_env_file.read_text()) == {
            "OPENBLAS_NUM_THREADS": "4",
            "MKL_NUM_THREADS": "4",
        }

        # SIGKILL (-9) the parent holder process!
        os.kill(holder.pid, signal.SIGKILL)
        holder.wait(timeout=5)

        wait_for_condition(lambda: not _pid_exists(fs_b_pid), timeout=5)
        wait_for_condition(lambda: not os.path.exists(sock_b), timeout=5)
        # Already-forked child worker stays alive independently!
        time.sleep(0.15)
        assert _pid_exists(child_b_pid)
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=5)
        if holder.stdout is not None:
            holder.stdout.close()
        if fs_b_pid is not None and _pid_exists(fs_b_pid):
            os.kill(fs_b_pid, signal.SIGKILL)
        if child_b_pid is not None and _pid_exists(child_b_pid):
            os.kill(child_b_pid, signal.SIGKILL)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_v1_immediate_teardown_sibling_refs_and_sprawl_cap(
    shutdown_only, tmp_path
):
    """Verify v1 default max_idle=0 immediate teardown, multi-env_vars sibling ref safety,
    PYTHONPATH isolation across siblings, and max_active_fork_servers cap + recovery."""
    wd_dir = tmp_path / "wd"
    wd_dir.mkdir()
    (wd_dir / "shared_wd_mod.py").write_text(
        "import os\nTAG = 'from_wd'\nIMPORT_PID = os.getpid()\n"
    )

    secret_dir = tmp_path / "secret_pypath"
    secret_dir.mkdir()
    (secret_dir / "only_for_s1.py").write_text("SECRET = 42\n")

    # Cap = 2 active fork servers total (1 default {} + at most 1 non-default env).
    ctx = ray.init(
        num_cpus=4,
        include_dashboard=False,
        _system_config={
            "enable_worker_fork_server": True,
            "max_active_fork_servers": 2,
        },
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

    @ray.remote(num_cpus=0)
    class ProbeActor:
        def inspect(self):
            import shared_wd_mod

            try:
                import only_for_s1  # noqa: F401

                has_secret = True
            except ImportError:
                has_secret = False
            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "wd_tag": shared_wd_mod.TAG,
                "wd_import_pid": shared_wd_mod.IMPORT_PID,
                "has_secret": has_secret,
                "rank": os.environ.get("RANK"),
            }

    @ray.remote(num_cpus=0)
    class SimpleActor:
        def ids(self):
            return os.getpid(), os.getppid()

    env_s1 = {
        "py_modules": [f"local://{wd_dir}"],
        "preload_modules": ["json", "shared_wd_mod"],
        "env_vars": {"RANK": "0", "PYTHONPATH": str(secret_dir)},
    }
    env_s2 = {
        "py_modules": [f"local://{wd_dir}"],
        "preload_modules": ["json", "shared_wd_mod"],
        "env_vars": {"RANK": "1"},
    }
    s1 = ProbeActor.options(runtime_env=env_s1).remote()
    r1 = ray.get(s1.inspect.remote())
    s2 = ProbeActor.options(runtime_env=env_s2).remote()
    r2 = ray.get(s2.inspect.remote())

    env1_fs_pid = r1["ppid"]
    assert env1_fs_pid != default_fs_pid
    assert r2["ppid"] == env1_fs_pid
    assert r1["wd_tag"] == "from_wd" and r2["wd_tag"] == "from_wd"
    assert r1["wd_import_pid"] == env1_fs_pid and r2["wd_import_pid"] == env1_fs_pid
    assert r1["rank"] == "0" and r1["has_secret"] is True
    # s1's user PYTHONPATH MUST NOT leak into the shared fork server or sibling s2!
    assert r2["rank"] == "1" and r2["has_secret"] is False

    env1_socks = [
        s for s, p in _find_fork_server_pids(session_dir).items() if p == env1_fs_pid
    ]
    assert len(env1_socks) == 1
    env1_sock = env1_socks[0]

    # Killing s1 drops env_s1's ref_cnt to 0 while sibling env_s2 still holds ref_cnt=1;
    # the shared fork server MUST stay alive!
    ray.kill(s1)
    wait_for_condition(lambda: not _pid_exists(r1["pid"]), timeout=10)
    time.sleep(0.3)
    assert _pid_exists(env1_fs_pid)
    assert os.path.exists(env1_sock)

    # Sprawl cap reached (default + env1 == 2 == max_active_fork_servers):
    # a 2nd distinct runtime_env bypasses fork server and runs cleanly via execvpe.
    env_over = {"preload_modules": ["math"]}
    over_actor = SimpleActor.options(runtime_env=env_over).remote()
    over_pid, over_ppid = ray.get(over_actor.ids.remote())
    assert over_ppid not in (default_fs_pid, env1_fs_pid)
    assert len(_find_fork_server_pids(session_dir)) == 2
    ray.kill(over_actor)
    wait_for_condition(lambda: not _pid_exists(over_pid), timeout=10)

    # Killing s2 drops the LAST ref on env1's fs_key to 0 -> v1 default (max_idle=0)
    # stops env1_fs_pid and unlinks env1_sock immediately without waiting 60s!
    ray.kill(s2)
    wait_for_condition(lambda: not _pid_exists(r2["pid"]), timeout=10)
    wait_for_condition(lambda: not _pid_exists(env1_fs_pid), timeout=5)
    wait_for_condition(lambda: not os.path.exists(env1_sock), timeout=5)

    # Now below cap (1 < 2): both a fresh env AND the previously capped env_over
    # acquire dedicated fork servers cleanly.
    rec_actor = SimpleActor.options(runtime_env=env_over).remote()
    rec_pid, rec_fs_pid = ray.get(rec_actor.ids.remote())
    assert rec_fs_pid != default_fs_pid
    assert rec_fs_pid in _find_fork_server_pids(session_dir).values()
    ray.kill(rec_actor)
    wait_for_condition(lambda: not _pid_exists(rec_pid), timeout=10)
    wait_for_condition(lambda: not _pid_exists(rec_fs_pid), timeout=5)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
@pytest.mark.skipif(torch is None, reason="PyTorch is not installed.")
def test_fork_server_post_fork_cpp_job_id_and_ray_constants_env_vars(ray_start_cluster):
    """Verify post-fork C++ RayConfig::JOB_ID() updates across N>=2 sequential jobs on
    the same fork server, and runtime_env RAY_* flags (zero-copy Torch tensors, actor
    task logging) activate independently across sibling actors sharing a fork server."""
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=4,
        _system_config={
            "enable_worker_fork_server": True,
        },
    )

    # Job 1
    ctx1 = ray.init(address=cluster.address)
    session_dir = ctx1.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    job1_id = ray.get_runtime_context().get_job_id()

    @ray.remote
    def probe_job_ids():
        return {
            "ppid": os.getppid(),
            "ctx_job_id": ray.get_runtime_context().get_job_id(),
            "cpp_config_job_id": ray._raylet.Config.JOB_ID(),
            "env_job_id": os.environ.get("RAY_JOB_ID"),
        }

    j1_res = ray.get(probe_job_ids.remote())
    assert j1_res == {
        "ppid": default_fs_pid,
        "ctx_job_id": job1_id,
        "cpp_config_job_id": job1_id,
        "env_job_id": job1_id,
    }
    ray.shutdown()

    # Job 2 reuses the EXACT SAME default fork server and must observe Job 2's ID in C++!
    ray.init(address=cluster.address)
    try:
        job2_id = ray.get_runtime_context().get_job_id()
        assert job2_id != job1_id

        @ray.remote
        def probe_job_ids_2():
            return {
                "ppid": os.getppid(),
                "ctx_job_id": ray.get_runtime_context().get_job_id(),
                "cpp_config_job_id": ray._raylet.Config.JOB_ID(),
                "env_job_id": os.environ.get("RAY_JOB_ID"),
            }

        @ray.remote(num_cpus=0)
        class Job2Actor:
            def check(self):
                return {
                    "ppid": os.getppid(),
                    "ctx_job_id": ray.get_runtime_context().get_job_id(),
                    "cpp_config_job_id": ray._raylet.Config.JOB_ID(),
                }

        j2_task = ray.get(probe_job_ids_2.remote())
        j2_actor_handle = Job2Actor.remote()
        j2_actor = ray.get(j2_actor_handle.check.remote())
        assert j2_task == {
            "ppid": default_fs_pid,
            "ctx_job_id": job2_id,
            "cpp_config_job_id": job2_id,
            "env_job_id": job2_id,
        }
        assert j2_actor == {
            "ppid": default_fs_pid,
            "ctx_job_id": job2_id,
            "cpp_config_job_id": job2_id,
        }
        ray.kill(j2_actor_handle)

        # Sibling actors Z1 and Z0 forked from the SAME preload_modules=["torch"] fork
        # server verify RAY_* overrides, CloudPickler.dispatch clearing, Arrow/OOB flags,
        # uvloop reset, and default_worker --logging-level=None dynamic resolution.
        @ray.remote(num_cpus=0)
        class RayConstProbeActor:
            def verify(self):
                import torch as _torch

                import ray._common.serialization as c_ser
                import ray._private.serialization as p_ser
                from ray._private import ray_constants as rc

                gw = ray._private.worker.global_worker
                ser_ctx = gw.get_serialization_context()
                ref = ray.put(_torch.arange(65536, dtype=_torch.float32))
                t_a = ray.get(ref)
                t_b = ray.get(ref)
                policy_mod = type(asyncio.get_event_loop_policy()).__module__
                return {
                    "ppid": os.getppid(),
                    "cpp_job_id": ray._raylet.Config.JOB_ID(),
                    "rc_zero_copy": rc.RAY_ENABLE_ZERO_COPY_TORCH_TENSORS,
                    "ctx_zero_copy": ser_ctx._zero_copy_tensors_enabled,
                    "tensor_in_dispatch": _torch.Tensor in CloudPickler.dispatch,
                    "arrow_table_in_dispatch": pa.Table in CloudPickler.dispatch,
                    "oob_allowed": (
                        c_ser.ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION
                        and p_ser.ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION
                    ),
                    "loop_policy_mod": policy_mod,
                    "ray_logger_level": logging.getLogger("ray").getEffectiveLevel(),
                    "shares_plasma_ptr": t_a.data_ptr() == t_b.data_ptr(),
                    "sum_ok": float(t_a.sum().item())
                    == float(_torch.arange(65536, dtype=_torch.float32).sum().item()),
                    "rc_actor_log": rc.RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING,
                    "gw_actor_log": gw._enable_record_actor_task_log,
                }

        z1 = RayConstProbeActor.options(
            runtime_env={
                "preload_modules": ["torch"],
                "env_vars": {
                    "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS": "1",
                    "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1",
                },
            }
        ).remote()
        z0 = RayConstProbeActor.options(
            runtime_env={
                "preload_modules": ["torch"],
                "env_vars": {
                    "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS": "0",
                    "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "0",
                    "RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION": "1",
                    "RAY_allow_out_of_band_object_ref_serialization": "0",
                    "RAY_USE_UVLOOP": "0",
                    "RAY_LOGGER_LEVEL": "debug",
                },
            }
        ).remote()
        res_z1, res_z0 = ray.get([z1.verify.remote(), z0.verify.remote()])
        assert res_z1["ppid"] == res_z0["ppid"]
        assert res_z1["ppid"] != default_fs_pid
        assert res_z1["cpp_job_id"] == job2_id and res_z0["cpp_job_id"] == job2_id
        assert res_z1["sum_ok"] is True and res_z0["sum_ok"] is True

        assert res_z1["rc_zero_copy"] is True
        assert res_z1["ctx_zero_copy"] is True
        assert res_z1["tensor_in_dispatch"] is True
        assert res_z1["arrow_table_in_dispatch"] is True
        assert res_z1["oob_allowed"] is True
        assert res_z1["ray_logger_level"] == logging.INFO
        assert res_z1["shares_plasma_ptr"] is True
        assert res_z1["rc_actor_log"] is True
        assert res_z1["gw_actor_log"] is True

        assert res_z0["rc_zero_copy"] is False
        assert res_z0["ctx_zero_copy"] is False
        assert res_z0["tensor_in_dispatch"] is False
        assert res_z0["arrow_table_in_dispatch"] is False
        assert res_z0["oob_allowed"] is False
        assert "uvloop" not in res_z0["loop_policy_mod"]
        assert res_z0["ray_logger_level"] == logging.DEBUG
        assert res_z0["shares_plasma_ptr"] is False
        assert res_z0["rc_actor_log"] is False
        assert res_z0["gw_actor_log"] is False
    finally:
        ray.shutdown()


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_prometheus_metrics_observability(shutdown_only, monkeypatch):
    """Verify live Prometheus scrape reports ray_node_fork_server_count (1 -> 2 -> 1),
    Component='ray::fork_server' smaps_rollup USS/RSS stats, and Raylet worker counters."""
    monkeypatch.setenv("REPORTER_UPDATE_INTERVAL_MS", "200")
    metrics_port = find_free_port()
    ctx = ray.init(
        num_cpus=1,
        include_dashboard=True,
        _metrics_export_port=metrics_port,
        _system_config={
            "enable_worker_fork_server": True,
            "metrics_report_interval_ms": 200,
        },
    )
    node_ip = ctx.address_info["node_ip_address"]
    session_dir = ctx.address_info["session_dir"]
    prom_addr = build_address(node_ip, metrics_port)
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )

    @ray.remote(num_cpus=0)
    class HoldActor:
        def ids(self):
            return os.getpid(), os.getppid()

    # Keep a forked default actor alive and confirm it is NOT miscounted as a fork server.
    default_actor = HoldActor.remote()
    _, default_fs_pid = ray.get(default_actor.ids.remote())
    assert default_fs_pid == _find_fork_server_pids(session_dir)[default_sock]

    def _scrape():
        _, _, samples = fetch_prometheus([prom_addr])
        return samples

    def _check_count(expected: float) -> bool:
        samples = _scrape()
        count_samples = [s for s in samples if s.name == "ray_node_fork_server_count"]
        if not count_samples or count_samples[-1].value != expected:
            return False
        labels = count_samples[-1].labels
        assert "ip" in labels and "SessionName" in labels
        fs_rss = [
            s
            for s in samples
            if s.name == "ray_component_rss_mb"
            and s.labels.get("Component") == "ray::fork_server"
        ]
        fs_uss = [
            s
            for s in samples
            if s.name == "ray_component_uss_mb"
            and s.labels.get("Component") == "ray::fork_server"
        ]
        actor_uss = [
            s
            for s in samples
            if s.name == "ray_component_uss_mb"
            and s.labels.get("Component") == "ray::HoldActor"
        ]
        fs_fds = [
            s
            for s in samples
            if s.name == "ray_component_num_fds"
            and s.labels.get("Component") == "ray::fork_server"
        ]
        forked_cnt = [
            s
            for s in samples
            if s.name.startswith("ray_internal_num_processes_forked_from_server")
            and not s.name.endswith("_created")
        ]
        return (
            bool(fs_rss)
            and fs_rss[-1].value > 0
            and bool(fs_uss)
            and 0 < fs_uss[-1].value <= fs_rss[-1].value
            and bool(actor_uss)
            and 0 < actor_uss[-1].value < 40.0
            and bool(fs_fds)
            and fs_fds[-1].value > 0
            and bool(forked_cnt)
            and forked_cnt[-1].value >= 1.0
        )

    wait_for_condition(lambda: _check_count(1.0), timeout=20, retry_interval_ms=200)

    # Spawn a custom runtime_env actor -> active fork server count goes 1 -> 2.
    custom_actor = HoldActor.options(runtime_env={"preload_modules": ["json"]}).remote()
    custom_pid, _ = ray.get(custom_actor.ids.remote())
    wait_for_condition(lambda: _check_count(2.0), timeout=15, retry_interval_ms=200)

    # Kill custom actor -> v1 immediate teardown drops fork server count 2 -> 1.
    ray.kill(custom_actor)
    wait_for_condition(lambda: not _pid_exists(custom_pid), timeout=10)
    wait_for_condition(lambda: _check_count(1.0), timeout=15, retry_interval_ms=200)

    # Trigger a single timeout fallback and verify Raylet fallback counter increments.
    cur_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    os.kill(cur_fs_pid, signal.SIGSTOP)
    try:
        fb_actor = HoldActor.remote()
        _, fb_ppid = ray.get(fb_actor.ids.remote())
        assert fb_ppid != cur_fs_pid
    finally:
        os.kill(cur_fs_pid, signal.SIGCONT)

    def _has_fallback_counter() -> bool:
        samples = _scrape()
        fb = [
            s
            for s in samples
            if s.name.startswith("ray_internal_num_fork_server_fallbacks")
            and not s.name.endswith("_created")
        ]
        return bool(fb) and fb[-1].value >= 1.0

    wait_for_condition(_has_fallback_counter, timeout=15, retry_interval_ms=200)


def _proc_contains_secret_bytes(pid: int, needle: bytes) -> bool:
    """Scan all readable anonymous/heap mappings of `pid` for `needle`."""
    maps_path = Path(f"/proc/{pid}/maps")
    mem_path = Path(f"/proc/{pid}/mem")
    overlap = max(len(needle) - 1, 1)
    with maps_path.open("r", encoding="utf-8", errors="ignore") as mf, mem_path.open(
        "rb", buffering=0
    ) as mem_f:
        for line in mf:
            parts = line.split(None, 5)
            if len(parts) < 5 or "r" not in parts[1]:
                continue
            label = parts[5].strip() if len(parts) == 6 else ""
            if label.startswith("[vvar") or label.startswith("/dev/"):
                continue
            start_hex, end_hex = parts[0].split("-")
            start, end = int(start_hex, 16), int(end_hex, 16)
            if end <= start or end - start > 512 * 1024 * 1024:
                continue
            pos = start
            tail = b""
            while pos < end:
                to_read = min(1024 * 1024, end - pos)
                try:
                    mem_f.seek(pos)
                    chunk = mem_f.read(to_read)
                except OSError:
                    break
                if not chunk:
                    break
                if needle in (tail + chunk):
                    return True
                tail = chunk[-overlap:]
                pos += len(chunk)
    return False


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_exec_bound_ld_library_path_tz_pythonpath_and_secret_isolation(
    shutdown_only, tmp_path
):
    """Verify >=35-char --temp-dir socket budget, real ctypes.CDLL load under
    LD_LIBRARY_PATH via a dedicated execve fork server, parent heap secret isolation
    from fork()-on-accept(), post-fork TZ time.tzset(), and working_dir > PYTHONPATH."""
    assert is_exec_bound_env_var("LD_LIBRARY_PATH")
    assert is_exec_bound_env_var("LD_PRELOAD")
    assert is_exec_bound_env_var("PYTHONHASHSEED")
    assert is_exec_bound_env_var("GLIBC_TUNABLES")
    assert is_exec_bound_env_var("JE_MALLOC_CONF")
    assert is_exec_bound_env_var("JE_ARROW_MALLOC_CONF")
    assert is_exec_bound_env_var("_RJEM_MALLOC_CONF")
    assert is_exec_bound_env_var("GOMP_CPU_AFFINITY")
    assert is_exec_bound_env_var("OMP_WAIT_POLICY")
    assert is_exec_bound_env_var("KMP_AFFINITY")
    assert is_exec_bound_env_var("MKL_THREADING_LAYER")
    assert is_exec_bound_env_var("OPENBLAS_CORETYPE")
    assert not is_exec_bound_env_var("OMP_NUM_THREADS")
    assert not is_exec_bound_env_var("OPENBLAS_NUM_THREADS")
    assert not is_exec_bound_env_var("MKL_NUM_THREADS")
    assert not is_exec_bound_env_var("OPENAI_API_KEY")
    assert not is_exec_bound_env_var("TZ")
    assert not is_exec_bound_env_var("PYTHONPATH")

    # Compile a real C shared library into an isolated directory not on host rpath.
    cc = shutil.which("cc") or shutil.which("gcc") or shutil.which("clang")
    assert cc is not None, "C compiler required to build real shared library probe"
    lib_dir = tmp_path / "native_lib"
    lib_dir.mkdir()
    c_src = lib_dir / "fsprobe.c"
    c_src.write_text("int fs_probe_magic(void) { return 20261107; }\n")
    so_file = lib_dir / "libfsprobe.so"
    subprocess.run(
        [cc, "-shared", "-fPIC", str(c_src), "-o", str(so_file)],
        check=True,
    )

    # Module-precedence fixture: py_modules > working_dir > env_vars["PYTHONPATH"].
    pymod_dir = tmp_path / "pymod_pkg"
    wd_dir = tmp_path / "wd_pkg"
    pypath_dir = tmp_path / "user_pypath"
    for d in (pymod_dir, wd_dir, pypath_dir):
        d.mkdir()
    (pymod_dir / "all_three_mod.py").write_text("SOURCE = 'py_modules'\n")
    (wd_dir / "all_three_mod.py").write_text("SOURCE = 'working_dir'\n")
    (pypath_dir / "all_three_mod.py").write_text("SOURCE = 'pythonpath'\n")
    (wd_dir / "wd_vs_pypath_mod.py").write_text("SOURCE = 'working_dir'\n")
    (pypath_dir / "wd_vs_pypath_mod.py").write_text("SOURCE = 'pythonpath'\n")
    (pypath_dir / "pypath_only_mod.py").write_text("SOURCE = 'pythonpath'\n")

    # >= 36-char temp dir verifying fs_0 and 12-char fs_<9b64> stay well below 104 bytes.
    long_temp_dir = tempfile.mkdtemp(prefix="/tmp/ray_long_tmpdir_fs_budget_")
    assert len(long_temp_dir) >= 35
    try:
        ctx = ray.init(
            num_cpus=4,
            include_dashboard=False,
            _temp_dir=long_temp_dir,
            _system_config={"enable_worker_fork_server": True},
        )
        session_dir = ctx.address_info["session_dir"]
        default_sock = os.path.join(session_dir, "sockets", "fs_0")
        assert len(default_sock.encode("utf-8")) < 104
        wait_for_condition(
            lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
        )
        default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

        # 1. Real ctypes.CDLL load via LD_LIBRARY_PATH shares one dedicated fork server
        # across actors with different non-exec env_vars.
        @ray.remote(num_cpus=0)
        class NativeLibActor:
            def call_lib(self):
                lib = ctypes.CDLL("libfsprobe.so")
                lib.fs_probe_magic.restype = ctypes.c_int
                return {
                    "pid": os.getpid(),
                    "ppid": os.getppid(),
                    "magic": int(lib.fs_probe_magic()),
                    "rank": os.environ.get("ACTOR_RANK"),
                }

        na0 = NativeLibActor.options(
            runtime_env={
                "env_vars": {"LD_LIBRARY_PATH": str(lib_dir), "ACTOR_RANK": "0"}
            }
        ).remote()
        na1 = NativeLibActor.options(
            runtime_env={
                "env_vars": {"LD_LIBRARY_PATH": str(lib_dir), "ACTOR_RANK": "1"}
            }
        ).remote()
        r_na0, r_na1 = ray.get([na0.call_lib.remote(), na1.call_lib.remote()])
        assert r_na0["magic"] == 20261107 and r_na1["magic"] == 20261107
        assert r_na0["rank"] == "0" and r_na1["rank"] == "1"
        assert r_na0["ppid"] == r_na1["ppid"]
        assert r_na0["ppid"] != default_fs_pid
        ld_socks = [
            s
            for s, p in _find_fork_server_pids(session_dir).items()
            if p == r_na0["ppid"]
        ]
        assert len(ld_socks) == 1 and len(ld_socks[0].encode("utf-8")) < 104
        ray.kill(na0)
        ray.kill(na1)

        # 2. fork()-on-accept() secret isolation: construct token dynamically so literal
        # is never in source, verify child receives it AND checks its own /proc/mem has
        # it, while parent fs_0 /proc/<pid>/mem contains ZERO occurrences!
        secret_val = "RAY_FS_SECRET_" + "_".join(
            f"{random.getrandbits(32):08x}" for _ in range(4)
        )

        @ray.remote(num_cpus=0)
        class SecretAndTzActor:
            def probe(self):
                return {
                    "pid": os.getpid(),
                    "ppid": os.getppid(),
                    "secret": os.environ.get("WORKER_API_SECRET"),
                    "tz_hour_epoch": time.localtime(0).tm_hour,
                    "tzname": time.tzname,
                }

        sa_utc = SecretAndTzActor.options(
            runtime_env={"env_vars": {"WORKER_API_SECRET": secret_val, "TZ": "UTC"}}
        ).remote()
        sa_tokyo = SecretAndTzActor.options(
            runtime_env={"env_vars": {"TZ": "Asia/Tokyo"}}
        ).remote()
        res_utc, res_tokyo = ray.get([sa_utc.probe.remote(), sa_tokyo.probe.remote()])
        assert res_utc["ppid"] == default_fs_pid
        assert res_tokyo["ppid"] == default_fs_pid
        assert res_utc["secret"] == secret_val
        assert res_utc["tz_hour_epoch"] == 0
        assert "UTC" in res_utc["tzname"][0]
        assert res_tokyo["tz_hour_epoch"] == 9
        assert "JST" in res_tokyo["tzname"][0]
        # Positive control: child heap holds secret; parent fork server heap MUST NOT!
        needle = secret_val.encode("utf-8")
        assert _proc_contains_secret_bytes(res_utc["pid"], needle) is True
        assert _proc_contains_secret_bytes(default_fs_pid, needle) is False
        ray.kill(sa_utc)
        ray.kill(sa_tokyo)

        # 3. Exact PYTHONPATH priority: py_modules > working_dir > env_vars["PYTHONPATH"].
        @ray.remote(
            num_cpus=0,
            runtime_env={
                "working_dir": f"local://{wd_dir}",
                "py_modules": [f"local://{pymod_dir}"],
                "env_vars": {"PYTHONPATH": str(pypath_dir)},
            },
        )
        def check_import_precedence():
            import all_three_mod
            import pypath_only_mod
            import wd_vs_pypath_mod

            return (
                os.getppid(),
                all_three_mod.SOURCE,
                wd_vs_pypath_mod.SOURCE,
                pypath_only_mod.SOURCE,
            )

        imp_ppid, s_all, s_wd, s_pyp = ray.get(check_import_precedence.remote())
        assert imp_ppid != os.getpid()
        assert (s_all, s_wd, s_pyp) == ("py_modules", "working_dir", "pythonpath")
    finally:
        ray.shutdown()
        shutil.rmtree(long_temp_dir, ignore_errors=True)


class _FakeUriWorkingDirPlugin(WorkingDirPlugin):
    """WorkingDirPlugin subclass charging a synthetic byte size on local:// URIs so URICache
    eviction can be exercised deterministically without network downloads."""

    def __init__(self, resources_dir: str, size_bytes: int = 100):
        super().__init__(resources_dir, SimpleNamespace())
        self._size_bytes = size_bytes
        self.deleted_uris: List[str] = []
        self.pre_delete_pid_alive: Dict[str, bool] = {}
        self.tracked_pids: Dict[str, int] = {}

    async def create(
        self,
        uri: Optional[str],
        runtime_env: dict,
        context: RuntimeEnvContext,
        logger: logging.Logger = logging.getLogger(__name__),
    ) -> int:
        await super().create(uri, runtime_env, context, logger)
        return self._size_bytes

    def delete_uri(self, uri: str, logger: Optional[logging.Logger] = None) -> int:
        if uri in self.tracked_pids:
            self.pre_delete_pid_alive[uri] = _pid_exists(self.tracked_pids[uri])
        self.deleted_uris.append(uri)
        assert uri.startswith("local://")
        shutil.rmtree(uri[len("local://") :], ignore_errors=True)
        return self._size_bytes


class _FakeUvEnvVarPlugin(UvPlugin):
    """UvPlugin subclass reusing real UvPlugin.get_uris $VAR interpolation while
    installing lightweight symlinked virtualenvs instead of invoking network uv."""

    def __init__(self, root_dir: Path):
        self._root_dir = root_dir

    async def create(
        self,
        uri: Optional[str],
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        assert uri is not None
        venv_dir = self._root_dir / uri.replace("://", "_")
        bin_dir = venv_dir / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)
        py_link = bin_dir / f"python_{uri[-8:]}"
        if not py_link.exists():
            os.symlink(sys.executable, py_link)
        context.py_executable = str(py_link)
        return 10.0

    def modify_context(
        self,
        uris: List[str],
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        if uris:
            uri = uris[0]
            venv_dir = self._root_dir / uri.replace("://", "_")
            context.py_executable = str(venv_dir / "bin" / f"python_{uri[-8:]}")

    def delete_uri(self, uri: str, logger: logging.Logger) -> float:
        shutil.rmtree(self._root_dir / uri.replace("://", "_"), ignore_errors=True)
        return 10.0


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
@pytest.mark.asyncio
async def test_fork_server_uv_cache_key_uricache_eviction_cwd_and_source_process_guards(
    tmp_path, monkeypatch
):
    """Verify G2 UvPlugin $VER cache-key isolation, G10 source_process == 'raylet' guard
    (including subsequent cache hit), URICache disk eviction stopping idle fork servers
    before rmtree, cwd inode replacement restart, and plugin/profiler allowlist bypass."""
    runtime_env_dir = tmp_path / "runtime_resources"
    logs_dir = tmp_path / "logs"
    sockets_dir = tmp_path / "sockets"
    for d in (runtime_env_dir, logs_dir, sockets_dir):
        d.mkdir()

    monkeypatch.setenv("RAY_enable_worker_fork_server", "0")
    agent = RuntimeEnvAgent(
        runtime_env_dir=str(runtime_env_dir),
        logging_params={
            "filename": "runtime_env_agent.log",
            "log_dir": str(logs_dir),
            "logging_level": "INFO",
            "logging_format": ray_constants.LOGGER_FORMAT,
            "max_bytes": 1024 * 1024,
            "backup_count": 1,
        },
        gcs_client=SimpleNamespace(),
        temp_dir=str(tmp_path),
        address="127.0.0.1",
        runtime_env_agent_port=0,
    )
    # Enable per-env fork servers explicitly on this isolated agent instance (skipping
    # background fs_0 supervisor so fork-server counts reflect exact test actions).
    agent._enable_worker_fork_server = True
    agent._sockets_dir = str(sockets_dir)
    agent._fork_server_max_idle_envs = 2
    agent._fork_server_idle_ttl_s = 60.0
    agent._max_active_fork_servers = 8

    wd_plugin = _FakeUriWorkingDirPlugin(str(runtime_env_dir), size_bytes=100)
    uv_plugin = _FakeUvEnvVarPlugin(tmp_path / "uv_venvs")
    agent._working_dir_plugin = wd_plugin
    agent._uv_plugin = uv_plugin
    for p in (wd_plugin, uv_plugin):
        p_ctx = agent._plugin_manager.plugins[p.name]
        p_ctx.class_instance = p
        p_ctx.uri_cache._delete_fn = p.delete_uri
        agent._wrap_uri_cache_delete_fn(p_ctx.uri_cache)
    # Keep working_dir URI cache ceiling at 150 bytes so second 100-byte env evicts first.
    agent._plugin_manager.plugins[wd_plugin.name].uri_cache.max_total_size_bytes = 150

    async def _create(
        env_dict: dict, source_process: str = "raylet"
    ) -> RuntimeEnvContext:
        req = runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest(
            job_id=b"01000000",
            serialized_runtime_env=RuntimeEnv(**env_dict).serialize(),
            source_process=source_process,
        )
        reply = await agent.GetOrCreateRuntimeEnv(req)
        assert (
            reply.status == runtime_env_agent_pb2.AGENT_RPC_STATUS_OK
        ), reply.error_message
        return RuntimeEnvContext.deserialize(reply.serialized_runtime_env_context)

    async def _delete(env_dict: dict, source_process: str = "raylet") -> None:
        req = runtime_env_agent_pb2.DeleteRuntimeEnvIfPossibleRequest(
            serialized_runtime_env=RuntimeEnv(**env_dict).serialize(),
            source_process=source_process,
        )
        reply = await agent.DeleteRuntimeEnvIfPossible(req)
        assert reply.status == runtime_env_agent_pb2.AGENT_RPC_STATUS_OK

    try:
        # 1. UvPlugin $VER interpolation MUST produce distinct fork servers & py_executables.
        uv_env_1 = {"uv": {"packages": ["demo==$VER"]}, "env_vars": {"VER": "1.0"}}
        uv_env_2 = {"uv": {"packages": ["demo==$VER"]}, "env_vars": {"VER": "2.0"}}
        ctx_uv1 = await _create(uv_env_1)
        ctx_uv2 = await _create(uv_env_2)
        assert ctx_uv1.fork_server_socket_path and ctx_uv2.fork_server_socket_path
        assert ctx_uv1.fork_server_socket_path != ctx_uv2.fork_server_socket_path
        assert ctx_uv1.py_executable != ctx_uv2.py_executable
        assert len(agent._fork_servers) == 2
        await _delete(uv_env_1)
        await _delete(uv_env_2)

        # 2. source_process != "raylet" MUST NOT spawn or leak fork servers, while a
        # subsequent "raylet" cache hit spawns the fork server without polluting _env_cache.
        sp_env = {"preload_modules": ["math"]}
        for non_raylet in ("client_server", "raylet_eager_install", ""):
            ctx_nr = await _create(sp_env, source_process=non_raylet)
            assert ctx_nr.fork_server_socket_path is None
        assert RuntimeEnv(**sp_env).serialize() not in agent._raylet_env_refs
        assert len(agent._fork_servers) == 2  # still only the 2 idle uv servers

        ctx_raylet_hit = await _create(sp_env, source_process="raylet")
        assert ctx_raylet_hit.fork_server_socket_path is not None
        assert os.path.exists(ctx_raylet_hit.fork_server_socket_path)
        ctx_cs_after = await _create(sp_env, source_process="client_server")
        assert ctx_cs_after.fork_server_socket_path is None
        await _delete(sp_env, source_process="raylet")
        for non_raylet in ("client_server", "raylet_eager_install", ""):
            await _delete(sp_env, source_process=non_raylet)

        # 3. URICache disk eviction stops retained idle fork server BEFORE delete_uri runs,
        # and local:// symlink replacement invalidates & restarts stale cwd fork server.
        agent._cleanup_fork_servers()
        agent._shutting_down = False
        dir_a = tmp_path / "wd_evict_a"
        dir_b = tmp_path / "wd_evict_b"
        dir_a.mkdir()
        dir_b.mkdir()
        env_a = {"working_dir": f"local://{dir_a}"}
        env_b = {"working_dir": f"local://{dir_b}"}
        ctx_a = await _create(env_a)
        assert ctx_a.fork_server_socket_path is not None
        fs_a_key = agent._serialized_env_to_fs_key[RuntimeEnv(**env_a).serialize()]
        fs_a_pid = agent._fork_servers[fs_a_key].process.pid
        wd_plugin.tracked_pids[f"local://{dir_a}"] = fs_a_pid
        assert _pid_exists(fs_a_pid)
        await _delete(env_a)
        assert fs_a_key in agent._idle_fork_servers and _pid_exists(fs_a_pid)

        # Creating 100-byte env_b overflows 150-byte URI cache -> stops fs_a BEFORE rmtree!
        ctx_b = await _create(env_b)
        assert ctx_b.fork_server_socket_path is not None
        assert f"local://{dir_a}" in wd_plugin.deleted_uris
        assert wd_plugin.pre_delete_pid_alive[f"local://{dir_a}"] is False
        assert fs_a_key not in agent._fork_servers
        assert not _pid_exists(fs_a_pid)
        await _delete(env_b)

        # Symlink target swap changes cwd (st_dev, st_ino) -> next raylet call restarts server.
        v1_dir = tmp_path / "sym_v1"
        v2_dir = tmp_path / "sym_v2"
        v1_dir.mkdir()
        v2_dir.mkdir()
        sym_dir = tmp_path / "sym_link"
        sym_dir.symlink_to(v1_dir)
        env_sym = {"working_dir": f"local://{sym_dir}"}
        ctx_sym1 = await _create(env_sym)
        sym_fs_key = agent._serialized_env_to_fs_key[RuntimeEnv(**env_sym).serialize()]
        pid_v1 = agent._fork_servers[sym_fs_key].process.pid
        assert agent._fork_servers[sym_fs_key].has_valid_cwd() is True

        sym_dir.unlink()
        sym_dir.symlink_to(v2_dir)
        shutil.rmtree(v1_dir)
        assert agent._fork_servers[sym_fs_key].has_valid_cwd() is False
        ctx_sym2 = await _create(env_sym)
        assert ctx_sym2.fork_server_socket_path == ctx_sym1.fork_server_socket_path
        pid_v2 = agent._fork_servers[sym_fs_key].process.pid
        assert pid_v2 != pid_v1 and _pid_exists(pid_v2)
        await _delete(env_sym)
        await _delete(env_sym)

        # 4. Exhaustive RuntimeEnv.known_fields partition, conda/_ray_commit/excludes
        # allowlist acceptance, custom/excluded plugin rejection, and wrapper logging.
        excluded_builtin_keys = {
            "container",
            "image_uri",
            "_nsight",
            "_rocprof_sys",
            "java_jars",
        }
        assert _FORK_SERVER_ALLOWED_RUNTIME_ENV_KEYS.isdisjoint(
            RuntimeEnv.extensions_fields
        )
        assert _FORK_SERVER_ALLOWED_RUNTIME_ENV_KEYS.isdisjoint(excluded_builtin_keys)
        assert (
            _FORK_SERVER_ALLOWED_RUNTIME_ENV_KEYS
            | RuntimeEnv.extensions_fields
            | excluded_builtin_keys
        ) == RuntimeEnv.known_fields

        conda_rt = RuntimeEnv(conda="my_test_conda_env", _validate=True)
        assert "_ray_commit" in conda_rt
        assert agent._should_start_fork_server(conda_rt, RuntimeEnvContext())
        assert agent._should_start_fork_server(
            RuntimeEnv(
                _validate=False,
                conda="my_test_conda_env",
                _ray_commit="deadbeef",
                _ray_release="2.0.0",
                _inject_current_ray=True,
            ),
            RuntimeEnvContext(),
        )
        assert agent._should_start_fork_server(
            RuntimeEnv(working_dir="local:///tmp", excludes=["*.tmp"]),
            RuntimeEnvContext(),
        )

        for unsupported_rt in (
            RuntimeEnv(_validate=False, java_jars=["s3://bucket/app.jar"]),
            RuntimeEnv(container={"image": "anyscale/ray:latest"}),
            RuntimeEnv(image_uri="rayproject/ray:latest"),
            RuntimeEnv(nsight="default"),
            RuntimeEnv(rocprof_sys="default"),
            RuntimeEnv(_validate=False, custom_third_party_plugin={"enabled": True}),
        ):
            assert not agent._should_start_fork_server(
                unsupported_rt, RuntimeEnvContext()
            ), unsupported_rt

        wrapper_exec = f"{sys.executable} -O"
        ctx_prof = await _create({"py_executable": wrapper_exec})
        assert ctx_prof.fork_server_socket_path is None
        ctx_prof_cached = await _create({"py_executable": wrapper_exec})
        assert ctx_prof_cached.fork_server_socket_path is None
        setup_log_text = (logs_dir / "runtime_env_setup-01000000.log").read_text()
        assert (
            setup_log_text.count(
                f"Bypassing worker fork server because runtime_env py_executable "
                f"{wrapper_exec!r} is a multi-argument wrapper command."
            )
            == 1
        )
        await _delete({"py_executable": wrapper_exec})
        await _delete({"py_executable": wrapper_exec})

        buf = io.StringIO()
        probe_logger = logging.Logger("bypass_probe")
        probe_logger.addHandler(logging.StreamHandler(buf))
        assert not agent._should_start_fork_server(
            RuntimeEnv(py_executable=f"{sys.executable} 'unclosed"),
            RuntimeEnvContext(),
            logger=probe_logger,
        )
        assert "is a multi-argument wrapper command." in buf.getvalue()
    finally:
        agent._cleanup_fork_servers()


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_log_isolation_pipelogger_teardown_and_preload_warning(
    shutdown_only, tmp_path
):
    """Verify G5 missing preload_modules warns in runtime_env_setup-*.log without failing,
    neither ForkServerProcess nor forked workers inherit RuntimeEnvAgent PipeLogger FDs
    (so teardown exits cleanly even with surviving worker stdio holders), and
    RAY_LOG_TO_STDERR=1 streams worker stderr."""
    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    logs_dir = Path(session_dir) / "logs"

    @ray.remote(
        num_cpus=0,
        runtime_env={
            "preload_modules": ["definitely_missing_mod_g5_xyz_99", "math"],
        },
    )
    class WarnButRunActor:
        def check(self):
            return os.getpid(), os.getppid(), "math" in sys.modules

    actor = WarnButRunActor.remote()
    actor_pid, actor_fs_pid, math_loaded = ray.get(actor.check.remote())
    held_stdio_fds: List[int] = []
    assert math_loaded is True
    assert actor_fs_pid in _find_fork_server_pids(session_dir).values()

    try:

        def _warning_logged() -> bool:
            for p in list(logs_dir.glob("runtime_env_setup-*.log")) + list(
                logs_dir.glob("fork_server_*.log")
            ):
                if (
                    "ForkServer failed to preload module 'definitely_missing_mod_g5_xyz_99'"
                    in p.read_text(errors="ignore")
                ):
                    return True
            return False

        wait_for_condition(_warning_logged, timeout=5)
        # Specifically confirm runtime_env_setup-<job_id>.log (tailed by log_monitor -> driver) got it.
        assert any(
            "ForkServer failed to preload module 'definitely_missing_mod_g5_xyz_99'"
            in p.read_text(errors="ignore")
            for p in logs_dir.glob("runtime_env_setup-*.log")
        )

        # Neither ForkServerProcess nor forked workers inherit RuntimeEnvAgent's
        # PipeLogger pipes, so cluster teardown finishes cleanly even when another
        # process keeps the forked worker's stdout/stderr open across ray.shutdown().
        agent_pid = int(
            Path(f"/proc/{actor_fs_pid}/stat").read_text().rsplit(")", 1)[1].split()[1]
        )
        assert _pid_exists(agent_pid)
        fs_fds = _proc_fd_map(actor_fs_pid)
        actor_fds = _proc_fd_map(actor_pid)
        agent_fds = _proc_fd_map(agent_pid)
        agent_stdio_pipes = {
            agent_fds[fd]
            for fd in ("1", "2")
            if fd in agent_fds and agent_fds[fd].startswith("pipe:[")
        }
        agent_pipes = {v for v in agent_fds.values() if v.startswith("pipe:[")}
        # ForkServerProcess holds only its intentional lifeline_r pipe from RuntimeEnvAgent
        # (never RuntimeEnvAgent's stdout/stderr PipeLogger pipes), while forked workers
        # close lifeline_r too and share zero pipes of any kind with RuntimeEnvAgent.
        assert agent_stdio_pipes.isdisjoint(fs_fds.values()), (
            agent_stdio_pipes,
            fs_fds,
        )
        assert agent_pipes.isdisjoint(actor_fds.values()), (agent_pipes, actor_fds)
        assert any(v.endswith("raylet.out") for v in actor_fds.values()), actor_fds
        assert any(v.endswith("raylet.err") for v in actor_fds.values()), actor_fds

        for fd_num in (1, 2):
            held_stdio_fds.append(
                os.open(f"/proc/{actor_pid}/fd/{fd_num}", os.O_WRONLY)
            )

        ray.shutdown()
        wait_for_condition(
            lambda: not _pid_exists(agent_pid) and not _pid_exists(actor_fs_pid),
            timeout=10,
        )
    finally:
        for fd in held_stdio_fds:
            os.close(fd)

    # Verify worker_stdout_fd / worker_stderr_fd passthrough for RAY_LOG_TO_STDERR=1.
    capture_err = tmp_path / "container_stderr.log"
    (tmp_path / "stderr_emit_hold.py").write_text(
        "import sys\n"
        "import ray._private.workers.default_worker as _dw\n"
        "def _emit():\n"
        "    print('RAY_LOG_TO_STDERR_FORK_OK', file=sys.stderr, flush=True)\n"
        "_dw.initialize_default_worker = _emit\n"
    )
    ray_python = str(Path(ray.__file__).resolve().parents[1])
    sock_std = str(tmp_path / "fs_stderr")
    out_r = os.open(os.devnull, os.O_WRONLY)
    err_w = os.open(str(capture_err), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    fs_std = ForkServerProcess(
        socket_path=sock_std,
        preload_modules=["stderr_emit_hold"],
        context=RuntimeEnvContext(
            env_vars={
                "PYTHONPATH": f"{tmp_path}{os.pathsep}{ray_python}",
                "RAY_LOG_TO_STDERR": "1",
            }
        ),
        setup_log_path=str(tmp_path / "setup.log"),
        worker_stdout_fd=out_r,
        worker_stderr_fd=err_w,
    )
    try:
        asyncio.run(fs_std.wait_ready(timeout_s=15.0))
        _ = _send_raw_fork_request(sock_std)
        wait_for_condition(
            lambda: capture_err.exists()
            and "RAY_LOG_TO_STDERR_FORK_OK" in capture_err.read_text(),
            timeout=5,
        )
    finally:
        fs_std.stop()
        os.close(out_r)
        os.close(err_w)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_accelerator_safety_and_scipy_32bit_openblas(
    shutdown_only, tmp_path, monkeypatch
):
    """Verify G1 lazy Worker.original_visible_accelerator_ids, pre-fork
    PYTORCH_NVML_BASED_CUDA_CHECK=1 & TORCH_DEVICE_BACKEND_AUTOLOAD=0 pinning + post-fork
    restoration + deferred backend autoload hook + torch.cuda._initialized fail-closed
    fallback, and G9 dynamic SciPy 32-bit + NumPy 64-bit OpenBLAS thread reconfiguration."""
    (tmp_path / "preload_env_probe.py").write_text(
        "import os, ray\n"
        "PRELOAD_SNAPSHOT = {\n"
        "    'nvml_check': os.environ.get('PYTORCH_NVML_BASED_CUDA_CHECK'),\n"
        "    'backend_autoload': os.environ.get('TORCH_DEVICE_BACKEND_AUTOLOAD'),\n"
        "    'raw_visible_cache': ray._private.worker.global_worker._original_visible_accelerator_ids,\n"
        "}\n"
    )
    (tmp_path / "bad_cuda_init_mod.py").write_text(
        "import sys, types\n"
        "fake_cuda = sys.modules.get('torch.cuda')\n"
        "if fake_cuda is None:\n"
        "    fake_cuda = types.ModuleType('torch.cuda')\n"
        "    sys.modules['torch.cuda'] = fake_cuda\n"
        "fake_cuda._initialized = True\n"
    )

    # Host has PYTORCH_NVML_BASED_CUDA_CHECK="0" and TORCH_DEVICE_BACKEND_AUTOLOAD unset;
    # preload must observe ("1", "0", None), while post-fork child restores ("0", None)
    # and computes original_visible_accelerator_ids lazily from child env!
    monkeypatch.setenv("PYTORCH_NVML_BASED_CUDA_CHECK", "0")
    monkeypatch.delenv("TORCH_DEVICE_BACKEND_AUTOLOAD", raising=False)
    get_by_lib = _make_openblas_threads_by_lib_getter()

    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    logs_dir = Path(session_dir) / "logs"
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

    preload_list = ["preload_env_probe"] + (["torch"] if torch is not None else [])

    @ray.remote(
        num_cpus=0,
        runtime_env={
            "py_modules": [f"local://{tmp_path}"],
            "preload_modules": preload_list,
            "env_vars": {"CUDA_VISIBLE_DEVICES": "2,3"},
        },
    )
    class AccelSafetyActor:
        def check(self):
            import preload_env_probe

            import ray._private.utils as rutils

            gw = ray._private.worker.global_worker
            autoload_pending_turned_off = False
            if torch is not None:
                # set_visible_accelerator_ids already ran during default_worker init and
                # consumed _pending_torch_device_backend_autoload once!
                autoload_pending_turned_off = (
                    rutils._pending_torch_device_backend_autoload is False
                )
            return {
                "ppid": os.getppid(),
                "preload_snapshot": preload_env_probe.PRELOAD_SNAPSHOT,
                "child_nvml": os.environ.get("PYTORCH_NVML_BASED_CUDA_CHECK"),
                "child_autoload": os.environ.get("TORCH_DEVICE_BACKEND_AUTOLOAD"),
                "child_orig_vis": gw.original_visible_accelerator_ids,
                "autoload_consumed": autoload_pending_turned_off,
            }

    acc_actor = AccelSafetyActor.remote()
    acc_res = ray.get(acc_actor.check.remote())
    assert acc_res["ppid"] in _find_fork_server_pids(session_dir).values()
    assert acc_res["preload_snapshot"] == {
        "nvml_check": "1",
        "backend_autoload": "0",
        "raw_visible_cache": None,
    }
    assert acc_res["child_nvml"] == "0"
    assert acc_res["child_autoload"] is None
    assert acc_res["child_orig_vis"]["GPU"] == ["2", "3"]
    if torch is not None:
        assert acc_res["autoload_consumed"] is True
    ray.kill(acc_actor)

    # Preloaded module setting torch.cuda._initialized = True MUST fail closed pre-fork
    # (never publish a poisoned socket) and fall back cleanly to execvpe!
    @ray.remote(
        num_cpus=0,
        runtime_env={
            "py_modules": [f"local://{tmp_path}"],
            "preload_modules": ["bad_cuda_init_mod"],
        },
    )
    def run_with_poisoned_preload():
        return os.getppid()

    fb_ppid = ray.get(run_with_poisoned_preload.remote())
    assert fb_ppid not in _find_fork_server_pids(session_dir).values()
    assert any(
        "initialized CUDA before fork()" in p.read_text(errors="ignore")
        for p in list(logs_dir.glob("runtime_env_setup-*.log"))
        + list(logs_dir.glob("fork_server_*.log"))
    )

    # G9: SciPy 32-bit OpenBLAS imported post-fork in a 1-CPU forked worker MUST stay
    # pinned to 1 thread (never host N cores) AND dynamically update to 2 -> 1 alongside
    # NumPy's pre-fork 64-bit OpenBLAS when the worker runs num_cpus=2 then num_cpus=1!
    if HAS_SCIPY:

        @ray.remote(num_cpus=1)
        def import_scipy_one_cpu():
            import scipy.linalg  # noqa: F401

            return os.getpid(), os.getppid(), get_by_lib()

        @ray.remote(num_cpus=2)
        def run_scipy_two_cpus():
            import scipy.linalg  # noqa: F401

            return os.getpid(), os.getppid(), get_by_lib()

        pid_a, ppid_a, libs_1a = ray.get(import_scipy_one_cpu.remote())
        assert ppid_a == default_fs_pid
        assert len(libs_1a) >= 2, libs_1a  # both 64-bit NumPy and 32-bit SciPy OpenBLAS
        assert all(v == 1 for v in libs_1a.values()), libs_1a

        pid_b, ppid_b, libs_2 = ray.get(run_scipy_two_cpus.remote())
        assert pid_b == pid_a and ppid_b == default_fs_pid
        assert len(libs_2) >= 2, libs_2
        assert all(v == 2 for v in libs_2.values()), libs_2

        pid_c, ppid_c, libs_1c = ray.get(import_scipy_one_cpu.remote())
        assert pid_c == pid_a and ppid_c == default_fs_pid
        assert all(v == 1 for v in libs_1c.values()), libs_1c


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_worker_process_setup_hook(shutdown_only, tmp_path):
    """Verify callable and module-string worker_process_setup_hook run post-fork via
    the default (fs_0) and per-runtime_env fork servers without falling back to exec."""
    pkg_dir = tmp_path / "hook_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text(
        "import os\n"
        "def run_string_hook():\n"
        "    os.environ['__RAY_TEST_STRING_HOOK_PID'] = str(os.getpid())\n"
    )

    def callable_setup_hook():
        _os = __import__("os")
        _os.environ["__RAY_TEST_CALLABLE_HOOK_PID"] = str(_os.getpid())

    metrics_port = find_free_port()
    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={"worker_process_setup_hook": callable_setup_hook},
        _metrics_export_port=metrics_port,
        _system_config={
            "enable_worker_fork_server": True,
            "metrics_report_interval_ms": 200,
        },
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    prom_addr = build_address(ctx.address_info["node_ip_address"], metrics_port)

    # 1. Job-level callable hook with no exec-bound fields reuses warm default fs_0.
    @ray.remote
    def check_callable():
        return (
            os.environ.get("__RAY_TEST_CALLABLE_HOOK_PID"),
            os.getppid(),
            os.getpid(),
        )

    hook_pid_1, ppid_1, pid_1 = ray.get(check_callable.remote())
    assert ppid_1 == default_fs_pid
    assert hook_pid_1 == str(pid_1)

    def _has_fork_without_fallback(addr: str) -> bool:
        try:
            _, _, samples = fetch_prometheus([addr])
        except Exception:
            return False
        forked = sum(
            s.value
            for s in samples
            if s.name.startswith("ray_internal_num_processes_forked_from_server")
            and not s.name.endswith("_created")
        )
        fallbacks = sum(
            s.value
            for s in samples
            if s.name.startswith("ray_internal_num_fork_server_fallbacks")
            and not s.name.endswith("_created")
        )
        return forked >= 1 and fallbacks == 0

    wait_for_condition(lambda: _has_fork_without_fallback(prom_addr), timeout=10)
    ray.shutdown()

    metrics_port_2 = find_free_port()
    ctx2 = ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={
            "worker_process_setup_hook": "hook_pkg.run_string_hook",
            "py_modules": [f"local://{tmp_path}"],
        },
        _metrics_export_port=metrics_port_2,
        _system_config={
            "enable_worker_fork_server": True,
            "metrics_report_interval_ms": 200,
        },
    )
    session_dir_2 = ctx2.address_info["session_dir"]
    default_sock_2 = os.path.join(session_dir_2, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock_2)
        and default_sock_2 in _find_fork_server_pids(session_dir_2),
        timeout=15,
    )
    default_fs_pid_2 = _find_fork_server_pids(session_dir_2)[default_sock_2]
    prom_addr_2 = build_address(ctx2.address_info["node_ip_address"], metrics_port_2)

    @ray.remote
    def check_string():
        return (
            os.environ.get("__RAY_TEST_STRING_HOOK_PID"),
            os.getppid(),
            os.getpid(),
        )

    hook_pid_2, ppid_2, pid_2 = ray.get(check_string.remote())
    fs_pids_2 = _find_fork_server_pids(session_dir_2)
    per_env_pids_2 = {pid for sock, pid in fs_pids_2.items() if sock != default_sock_2}
    assert ppid_2 != default_fs_pid_2
    assert ppid_2 in per_env_pids_2
    assert hook_pid_2 == str(pid_2)
    wait_for_condition(lambda: _has_fork_without_fallback(prom_addr_2), timeout=10)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_setup_timeout_clamping_and_fallback(shutdown_only, tmp_path):
    """Verify finite setup_timeout_seconds clamps cold ForkServer wait_ready (including
    SIGTERM-ignoring preloads) so tasks fall back cleanly to execvpe without failing
    GetOrCreateRuntimeEnv, while warm fs_0 continues serving tight timeouts."""
    slow_root = tmp_path / "slow_root"
    slow_pkg = slow_root / "slow_mod"
    slow_pkg.mkdir(parents=True)
    (slow_pkg / "__init__.py").write_text(
        "import signal\n"
        "import time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "time.sleep(10.0)\n"
    )
    slow_1s_root = tmp_path / "slow_1s_root"
    slow_1s_pkg = slow_1s_root / "slow_1s_mod"
    slow_1s_pkg.mkdir(parents=True)
    (slow_1s_pkg / "__init__.py").write_text("import time\ntime.sleep(5.0)\n")

    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

    # 1. Tight setup_timeout_seconds=1 still uses already-warm fs_0 immediately.
    @ray.remote(runtime_env={"config": {"setup_timeout_seconds": 1}})
    def check_warm_default():
        return os.getppid()

    assert ray.get(check_warm_default.remote()) == default_fs_pid

    # 2. Cold preload ignoring SIGTERM clamps wait_ready + stop() inside 3s budget
    # and falls back to execvpe (where preload_modules is not auto-imported).
    @ray.remote(
        runtime_env={
            "py_modules": [f"local://{slow_root}"],
            "preload_modules": ["slow_mod"],
            "config": {"setup_timeout_seconds": 3},
        }
    )
    def check_clamped_fallback():
        return "slow_mod" in sys.modules, os.getppid()

    t0 = time.monotonic()
    preloaded_3s, ppid_3s = ray.get(check_clamped_fallback.remote())
    elapsed_3s = time.monotonic() - t0
    assert preloaded_3s is False
    assert ppid_3s != default_fs_pid
    assert ppid_3s not in _find_fork_server_pids(session_dir).values()
    assert 1.0 <= elapsed_3s < 8.0

    # 3. Cold preload with setup_timeout_seconds=1 skips cold fork server immediately.
    @ray.remote(
        runtime_env={
            "py_modules": [f"local://{slow_1s_root}"],
            "preload_modules": ["slow_1s_mod"],
            "config": {"setup_timeout_seconds": 1},
        }
    )
    def check_tight_1s_skip():
        return "slow_1s_mod" in sys.modules, os.getppid()

    preloaded_1s, ppid_1s = ray.get(check_tight_1s_skip.remote())
    assert preloaded_1s is False
    assert ppid_1s not in _find_fork_server_pids(session_dir).values()

    # Ensure no per-env fork server leaked from the timed-out startup, and setup logs
    # record both the clamped wait_ready fallback and the tight-budget skip.
    wait_for_condition(
        lambda: set(_find_fork_server_pids(session_dir).keys()) == {default_sock},
        timeout=5,
    )
    logs_dir = Path(session_dir) / "logs"

    def _check_setup_logs():
        combined = "\n".join(
            p.read_text(errors="replace")
            for p in logs_dir.glob("runtime_env*.log")
            if p.is_file()
        )
        return (
            "Failed to wait for fork server, falling back to exec" in combined
            and "Skipping cold fork server start" in combined
        )

    wait_for_condition(_check_setup_logs, timeout=10)


def test_clamped_ready_timeout_preserves_default_120s_cap(monkeypatch):
    """Verify default 600s setup_timeout_seconds does not raise wait_ready above
    RAY_fork_server_ready_timeout_s (default 120s)."""
    now = time.monotonic()
    deadline_600 = RuntimeEnvAgent._fork_server_setup_deadline(now, 600)
    clamped_default = RuntimeEnvAgent._clamped_ready_timeout_s(deadline_600)
    assert 119.0 <= clamped_default <= 120.0

    monkeypatch.setenv("RAY_fork_server_ready_timeout_s", "2.5")
    assert RuntimeEnvAgent._clamped_ready_timeout_s(deadline_600) == pytest.approx(
        2.5, abs=0.2
    )

    deadline_3 = RuntimeEnvAgent._fork_server_setup_deadline(time.monotonic(), 3)
    clamped_3 = RuntimeEnvAgent._clamped_ready_timeout_s(deadline_3)
    assert 1.0 < clamped_3 <= 1.5

    assert RuntimeEnvAgent._fork_server_setup_deadline(now, None) is None
    assert RuntimeEnvAgent._clamped_ready_timeout_s(None) == pytest.approx(2.5)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_pip_excludes_local_dev_wrapper_bypass_and_rjem_pin(
    ray_start_cluster, monkeypatch, tmp_path
):
    """Verify pip (_ray_commit), working_dir + excludes, and RAY_RUNTIME_ENV_LOCAL_DEV_MODE
    (_inject_current_ray) spawn dedicated fork servers; multi-arg py_executable logs and
    falls back cleanly via execvpe; and _RJEM_MALLOC_CONF keeps Rust jemalloc preloads
    (e.g. polars) single-threaded pre-fork while restoring/unsetting cleanly post-fork."""
    monkeypatch.delenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE", raising=False)
    monkeypatch.delenv("_RJEM_MALLOC_CONF", raising=False)
    monkeypatch.delenv("JE_ARROW_MALLOC_CONF", raising=False)

    tests_dir = Path(__file__).resolve().parent
    local_wheel = tests_dir / "pip_install_test-0.5-py3-none-any.whl"
    assert local_wheel.is_file(), f"Missing hermetic test wheel: {local_wheel}"

    wdir = tmp_path / "wd_with_excludes"
    wdir.mkdir()
    (wdir / "kept.txt").write_text("kept_payload")
    (wdir / "ignored.tmp").write_text("excluded_payload")
    (wdir / "wd_excl_mod.py").write_text("import os\nPRELOAD_PID = os.getpid()\n")

    dev_dir = tmp_path / "local_dev_pymod"
    dev_dir.mkdir()
    (dev_dir / "dev_inject_mod.py").write_text("import os\nPRELOAD_PID = os.getpid()\n")

    rjem_dir = tmp_path / "rjem_pymod"
    rjem_dir.mkdir()
    (rjem_dir / "rjem_probe_mod.py").write_text(
        "import os\n"
        "PRELOAD_RJEM = os.environ.get('_RJEM_MALLOC_CONF')\n"
        "PRELOAD_JE_ARROW = os.environ.get('JE_ARROW_MALLOC_CONF')\n"
    )

    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=2,
        _system_config={"enable_worker_fork_server": True},
    )
    ctx = ray.init(address=cluster.address)
    session_dir = ctx.address_info["session_dir"]
    logs_dir = Path(session_dir) / "logs"
    default_sock = os.path.join(session_dir, "sockets", "fs_0")
    wait_for_condition(
        lambda: os.path.exists(default_sock)
        and default_sock in _find_fork_server_pids(session_dir),
        timeout=15,
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

    # (a.1) Hermetic offline `pip` runtime_env carries auto-injected `_ray_commit` and
    # spawns a dedicated `sockets/fs_<hash>` fork server running the virtualenv Python.
    pip_spec = {
        "packages": ["pip-install-test==0.5"],
        "pip_install_options": [
            "--no-index",
            f"--find-links={tests_dir}",
            "--no-deps",
            "--no-cache-dir",
            "--disable-pip-version-check",
        ],
    }
    pip_rt = RuntimeEnv(pip=pip_spec)
    assert "_ray_commit" in pip_rt

    @ray.remote(num_cpus=0, runtime_env=pip_rt)
    class PipForkActor:
        def inspect(self):
            mod = importlib.import_module("pip_install_test")
            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "exe": sys.executable,
                "mod_file": os.path.realpath(mod.__file__),
            }

    pip_actor = PipForkActor.remote()
    pip_info = ray.get(pip_actor.inspect.remote())
    fs_by_sock = _find_fork_server_pids(session_dir)
    pip_socks = [s for s, p in fs_by_sock.items() if p == pip_info["ppid"]]
    assert len(pip_socks) == 1
    assert pip_socks[0] != default_sock
    assert os.path.basename(pip_socks[0]).startswith("fs_")
    assert pip_info["ppid"] != default_fs_pid
    assert pip_info["exe"] != sys.executable
    assert "virtualenv" in pip_info["mod_file"]
    ray.kill(pip_actor)
    wait_for_condition(lambda: not _pid_exists(pip_info["pid"]), timeout=10)

    # (a.2) `RAY_RUNTIME_ENV_LOCAL_DEV_MODE=1` injects `_inject_current_ray=True` and still forks.
    monkeypatch.setenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE", "1")
    dev_rt = RuntimeEnv(
        py_modules=[f"local://{dev_dir}"],
        preload_modules=["dev_inject_mod"],
    )
    assert dev_rt.get("_inject_current_ray") is True

    @ray.remote(num_cpus=0, runtime_env=dev_rt)
    class LocalDevForkActor:
        def inspect(self):
            mod = importlib.import_module("dev_inject_mod")
            return os.getpid(), os.getppid(), mod.PRELOAD_PID

    dev_actor = LocalDevForkActor.remote()
    dev_pid, dev_ppid, dev_preload_pid = ray.get(dev_actor.inspect.remote())
    dev_socks = [
        s for s, p in _find_fork_server_pids(session_dir).items() if p == dev_ppid
    ]
    assert len(dev_socks) == 1 and dev_socks[0] != default_sock
    assert dev_preload_pid == dev_ppid
    ray.kill(dev_actor)
    wait_for_condition(lambda: not _pid_exists(dev_pid), timeout=10)
    monkeypatch.delenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE", raising=False)

    # (b) Multi-argument `py_executable` falls back cleanly via execvpe and logs once in setup log.
    wrapper_cmd = f"{sys.executable} -u"

    @ray.remote(num_cpus=0, runtime_env={"py_executable": wrapper_cmd})
    class WrapperExecActor:
        def inspect(self):
            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "stdout_line_buffering": getattr(sys.stdout, "line_buffering", None),
                "raw_stdout_type": type(
                    getattr(sys.__stdout__, "buffer", sys.__stdout__)
                ).__name__,
            }

    wrap_a1 = WrapperExecActor.remote()
    wrap_a2 = WrapperExecActor.remote()
    w1, w2 = ray.get([wrap_a1.inspect.remote(), wrap_a2.inspect.remote()])
    live_fs_pids = set(_find_fork_server_pids(session_dir).values())
    assert w1["ppid"] not in live_fs_pids
    assert w2["ppid"] not in live_fs_pids
    # Under `python -u`, sys.__stdout__.buffer wraps raw FileIO directly without BufferedWriter.
    assert w1["raw_stdout_type"] == "FileIO"
    ray.kill(wrap_a1)
    ray.kill(wrap_a2)

    expected_msg = (
        f"Bypassing worker fork server because runtime_env py_executable "
        f"{wrapper_cmd!r} is a multi-argument wrapper command."
    )

    def _wrapper_logged_once() -> bool:
        combined = "\n".join(
            p.read_text(errors="replace")
            for p in logs_dir.glob("runtime_env_setup-*.log")
        )
        return combined.count(expected_msg) == 1

    wait_for_condition(_wrapper_logged_once, timeout=5)

    # (c) `_RJEM_MALLOC_CONF` is pinned pre-fork (keeping `polars` single-threaded in the
    # fork server when installed), unset post-fork by default, and preserved when overridden.
    rjem_preloads = ["rjem_probe_mod"] + (["polars"] if HAS_POLARS else [])

    @ray.remote(
        num_cpus=0,
        runtime_env={
            "py_modules": [f"local://{rjem_dir}"],
            "preload_modules": rjem_preloads,
        },
    )
    class RjemPinActor:
        def inspect(self):
            probe = importlib.import_module("rjem_probe_mod")
            child_rjem = os.environ.get("_RJEM_MALLOC_CONF")
            child_je_arrow = os.environ.get("JE_ARROW_MALLOC_CONF")
            polars_preloaded = "polars" in sys.modules
            polars_sum = None
            if HAS_POLARS:
                pl = importlib.import_module("polars")
                polars_sum = int(
                    pl.DataFrame({"x": [1, 2, 3, 4]}).select(pl.col("x").sum()).item()
                )
            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "preload_rjem": probe.PRELOAD_RJEM,
                "preload_je_arrow": probe.PRELOAD_JE_ARROW,
                "child_rjem": child_rjem,
                "child_je_arrow": child_je_arrow,
                "polars_preloaded": polars_preloaded,
                "polars_sum": polars_sum,
            }

    rjem_actor = RjemPinActor.remote()
    rjem_info = ray.get(rjem_actor.inspect.remote())
    rjem_fs_pid = rjem_info["ppid"]
    assert rjem_fs_pid != default_fs_pid
    assert rjem_fs_pid in _find_fork_server_pids(session_dir).values()
    assert len(os.listdir(f"/proc/{rjem_fs_pid}/task")) == 1
    assert "Threads:\t1\n" in Path(f"/proc/{rjem_fs_pid}/status").read_text()
    assert rjem_info["preload_rjem"] == "background_thread:false"
    assert rjem_info["preload_je_arrow"] == "background_thread:false"
    assert rjem_info["child_rjem"] is None
    assert rjem_info["child_je_arrow"] is None
    if HAS_POLARS:
        assert rjem_info["polars_preloaded"] is True
        assert rjem_info["polars_sum"] == 10

    rjem_override_actor = RjemPinActor.options(
        runtime_env={
            "py_modules": [f"local://{rjem_dir}"],
            "preload_modules": ["rjem_probe_mod"],
            "env_vars": {"_RJEM_MALLOC_CONF": "dirty_decay_ms:5000"},
        }
    ).remote()
    rjem_override = ray.get(rjem_override_actor.inspect.remote())
    assert rjem_override["ppid"] not in (default_fs_pid, rjem_fs_pid)
    assert rjem_override["ppid"] in _find_fork_server_pids(session_dir).values()
    assert rjem_override["preload_rjem"] == "dirty_decay_ms:5000"
    assert rjem_override["child_rjem"] == "dirty_decay_ms:5000"
    ray.kill(rjem_actor)
    ray.kill(rjem_override_actor)
    ray.shutdown()

    # (a.3) Job-level `working_dir` + `excludes` uploads filtered package, hits
    # raylet_eager_install -> raylet cache path, and forks from dedicated `sockets/fs_<hash>`.
    wd_rt = {
        "working_dir": str(wdir),
        "excludes": ["*.tmp"],
        "preload_modules": ["wd_excl_mod"],
    }
    assert "excludes" in RuntimeEnv(**wd_rt)
    ray.init(address=cluster.address, runtime_env=wd_rt)
    try:

        @ray.remote(num_cpus=0)
        class ExcludesForkActor:
            def inspect(self):
                mod = importlib.import_module("wd_excl_mod")
                return {
                    "pid": os.getpid(),
                    "ppid": os.getppid(),
                    "preload_pid": mod.PRELOAD_PID,
                    "has_kept": Path("kept.txt").is_file(),
                    "has_ignored": Path("ignored.tmp").exists(),
                }

        wd_actor = ExcludesForkActor.remote()
        wd_info = ray.get(wd_actor.inspect.remote())
        wd_socks = [
            s
            for s, p in _find_fork_server_pids(session_dir).items()
            if p == wd_info["ppid"]
        ]
        assert len(wd_socks) == 1 and wd_socks[0] != default_sock
        assert os.path.basename(wd_socks[0]).startswith("fs_")
        assert wd_info["preload_pid"] == wd_info["ppid"]
        assert wd_info["has_kept"] is True
        assert wd_info["has_ignored"] is False
        ray.kill(wd_actor)
        wait_for_condition(lambda: not _pid_exists(wd_info["pid"]), timeout=10)
    finally:
        ray.shutdown()
