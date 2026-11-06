import asyncio
import ctypes
import gc
import hashlib
import json
import os
import random
import signal
import socket
import stat
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pytest

import ray
from ray._common.network_utils import build_address, find_free_port
from ray._common.test_utils import fetch_prometheus, wait_for_condition
from ray._private.workers.fork_server import ForkServerProcess
from ray.runtime_env import RuntimeEnv

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


def _make_openblas_threads_getter():
    """Return a local closure (serialized by value by cloudpickle) querying OpenBLAS thread count."""

    def _get_openblas_num_threads() -> Optional[int]:
        try:
            with open("/proc/self/maps", "r", encoding="utf-8", errors="ignore") as f:
                maps_lines = f.readlines()
        except OSError:
            return None
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
                lib = ctypes.CDLL(so_path, mode=ctypes.RTLD_NOLOAD)
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
                        return int(fn())
                    except Exception:
                        pass
        return None

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


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_default_workers_signals_and_process_groups(shutdown_only):
    """Verify default ForkServer single-threaded state, worker creation, PRNG reseeding, SIGINT cancellation, and actor process-group isolation."""
    ctx = ray.init(
        num_cpus=4,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(lambda: os.path.exists(default_sock), timeout=10)
    assert stat.S_IMODE(os.stat(default_sock).st_mode) == 0o600

    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    default_fs_pgid = os.getpgid(default_fs_pid)
    assert len(os.listdir(f"/proc/{default_fs_pid}/task")) == 1

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
        }

    results = ray.get([get_worker_info.remote(i) for i in range(4)])
    assert len({r["pid"] for r in results}) == 4
    for r in results:
        assert r["ppid"] == default_fs_pid
        assert r["pgid"] == r["pid"]
        assert r["pgid"] != default_fs_pgid
        assert r["freeze_count"] > 0

    assert len({r["rand"] for r in results}) == 4
    assert len({r["np_rand"] for r in results}) == 4

    # Verify graceful task cancellation via SIGINT works on forked workers.
    @ray.remote
    class SignalTracker:
        def __init__(self):
            self.started = False
            self.caught_keyboard_interrupt = False

        def mark_started(self):
            self.started = True

        def mark_interrupted(self):
            self.caught_keyboard_interrupt = True

        def get_state(self):
            return self.started, self.caught_keyboard_interrupt

    tracker = SignalTracker.remote()

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
    """Verify 100ms timeout fallback with zero late duplicate workers, default
    auto-respawn, and cached runtime_env lazy respawn at a deterministic socket."""
    monkeypatch.setenv("RAY_fork_server_monitor_interval_s", "0.05")
    monkeypatch.setenv("RAY_fork_server_respawn_backoff_base_s", "0.6")
    ctx = ray.init(
        num_cpus=1,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

    @ray.remote(num_cpus=1)
    class PingActor:
        def get_ids(self):
            return os.getpid(), os.getppid()

    # Consume the initial pre-started worker with a1.
    a1 = PingActor.remote()
    pid1, ppid1 = ray.get(a1.get_ids.remote())
    assert ppid1 == default_fs_pid

    # Pause ForkServer with SIGSTOP so kernel accepts AF_UNIX connections in backlog
    # without replying; spawning a2 must time out (~100ms), mark the socket unhealthy,
    # and fall back to execvpe. While still SIGSTOPped, a3 must immediately bypass the
    # unhealthy socket without stalling for another 100ms.
    os.kill(default_fs_pid, signal.SIGSTOP)
    try:
        t0 = time.monotonic()
        a2 = PingActor.options(num_cpus=0).remote()
        _, ppid2 = ray.get(a2.get_ids.remote())
        elapsed2 = time.monotonic() - t0
        assert ppid2 != default_fs_pid
        assert elapsed2 < 3.0

        a3 = PingActor.options(num_cpus=0).remote()
        _, ppid3 = ray.get(a3.get_ids.remote())
        assert ppid3 != default_fs_pid
    finally:
        os.kill(default_fs_pid, signal.SIGCONT)

    # After SIGCONT, the ForkServer drains the stale backlog request from a2 and drops it
    # via CLOCK_MONOTONIC deadline_ms BEFORE os.fork(), spawning zero duplicate orphans.
    time.sleep(0.25)
    assert _find_child_pids(default_fs_pid) == [pid1]

    # Cached runtime_env lazy respawn: keep a4 alive so kill_env stays in _env_cache
    # with ref_cnt >= 1, SIGKILL env_fs_pid, then verify a5 lazily respawns a new
    # fork server at the exact same deterministic fork_server_<sha256[:16]>.sock path!
    kill_env = {"preload_modules": ["json"], "env_vars": {"FORK_SERVER_KILL_TEST": "1"}}
    fs_key = RuntimeEnv(preload_modules=["json"]).serialize()
    expected_env_sock = os.path.join(
        session_dir,
        "sockets",
        f"fork_server_{hashlib.sha256(fs_key.encode()).hexdigest()[:16]}.sock",
    )
    a4 = PingActor.options(num_cpus=0, runtime_env=kill_env).remote()
    pid4, env_fs_pid = ray.get(a4.get_ids.remote())
    assert env_fs_pid != default_fs_pid
    assert _find_fork_server_pids(session_dir).get(expected_env_sock) == env_fs_pid

    os.kill(env_fs_pid, signal.SIGKILL)
    wait_for_condition(lambda: not _pid_exists(env_fs_pid), timeout=10)
    # Pre-crash actor a4 continues running independently after its fork server dies.
    assert ray.get(a4.get_ids.remote())[0] == pid4

    a5 = PingActor.options(num_cpus=0, runtime_env=kill_env).remote()
    _, ppid5 = ray.get(a5.get_ids.remote())
    assert ppid5 != env_fs_pid
    assert ppid5 != default_fs_pid
    assert _find_fork_server_pids(session_dir).get(expected_env_sock) == ppid5

    # Default {} supervised auto-respawn: SIGKILL default_fs_pid, verify a1 survives,
    # an immediate actor falls back seamlessly to execvpe during the 0.6s backoff
    # window (putting stale old_default_ino on cooldown), background supervisor
    # atomically replaces fork_server_default.sock (new st_ino clears cooldown),
    # and a6 forks from the respawned server.
    old_default_ino = os.stat(default_sock).st_ino
    os.kill(default_fs_pid, signal.SIGKILL)
    wait_for_condition(lambda: not _pid_exists(default_fs_pid), timeout=5)
    assert ray.get(a1.get_ids.remote())[0] == pid1

    a_dead_fb = PingActor.options(num_cpus=0).remote()
    _, ppid_dead_fb = ray.get(a_dead_fb.get_ids.remote())
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
    _, ppid6 = ray.get(a6.get_ids.remote())
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
        },
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
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

    task_results = ray.get([check_env_task.remote(i) for i in range(4)])
    task_res = task_results[0]
    actor = EnvTorchActor.remote()
    actor_res = ray.get(actor.info.remote())

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
    if task_res["openblas_threads"] is not None:
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
    if actor_res["openblas_threads"] is not None:
        assert actor_res["openblas_threads"] == 3
    assert actor_res["C_NESTED_DIR"] == "/tmp/ray_fork_test/sub/nested"

    assert other_res["ppid"] == env_fs_pid
    assert other_res["custom_pkg_pid"] == env_fs_pid
    assert other_res["PER_ACTOR_RANK"] == "7"
    assert other_res["omp_num_threads"] == "2"
    assert other_res["torch_threads"] == 2
    if other_res["openblas_threads"] is not None:
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

    # Killing the actor drops the runtime_env refcount to 0; the ForkServer remains warm
    # in the idle cache so a subsequent actor reuses it without re-importing torch.
    ray.kill(actor)
    wait_for_condition(lambda: not _pid_exists(actor_res["pid"]), timeout=15)
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
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
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
    if res2["openblas_threads"] is not None:
        assert res2["openblas_threads"] == 2

    res1 = ray.get(check_threads_one.remote())
    assert res1["ppid"] == default_fs_pid
    assert res1["omp_env"] == "1"
    assert res1["torch_threads"] == 1
    if res1["openblas_threads"] is not None:
        assert res1["openblas_threads"] == 1


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_object_spilling_and_restoration_io_workers(
    shutdown_only, tmp_path
):
    """Verify SPILL_WORKER and RESTORE_WORKER I/O workers fork cleanly without RAY_JOB_ID."""
    spill_dir = tmp_path / "spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "enable_worker_fork_server": True,
            "max_io_workers": 2,
            "min_spilling_size": 1,
            "object_spilling_config": json.dumps(
                {"type": "filesystem", "params": {"directory_path": str(spill_dir)}}
            ),
        },
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
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
    assert all(ppid == default_fs_pid for ppid in io_ppids["spill"])
    assert all(ppid == default_fs_pid for ppid in io_ppids["restore"])
    assert _pid_exists(default_fs_pid)


def _send_raw_fork_request(sock_path: str) -> int:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(5.0)
        s.connect(sock_path)
        req = {
            "args": [],
            "env": dict(os.environ),
            "runtime_env_vars": {},
            "deadline_ms": time.clock_gettime_ns(time.CLOCK_MONOTONIC) // 1_000_000
            + 5000,
        }
        s.sendall((json.dumps(req) + "\n").encode("utf-8"))
        resp = json.loads(s.recv(4096).decode("utf-8"))
    assert "error" not in resp, resp
    return int(resp["pid"])


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
        "import time\n"
        "import ray._private.workers.default_worker as _dw\n"
        "_dw.initialize_default_worker = lambda: time.sleep(300)\n"
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
    try:
        asyncio.run(fs_a.wait_ready(timeout_s=15.0))
        time.sleep(0.1)
        assert fs_a.is_alive()
        assert os.path.exists(sock_a)
        child_a_pid = _send_raw_fork_request(sock_a)
        wait_for_condition(lambda: _pid_exists(child_a_pid), timeout=5)
    finally:
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
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
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
            # Disable boot-time JobID::Nil() ("ffffffff") worker prestarting so Job 1's
            # task worker is forked on-demand with Job 1's real RAY_JOB_ID.
            "enable_worker_prestart": False,
            "prestart_worker_first_driver": False,
        },
    )

    # Job 1
    ctx1 = ray.init(address=cluster.address)
    session_dir = ctx1.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
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

        # Sibling actors Z1 (zero-copy=1, record_log=1) and Z0 (zero-copy=0, record_log=0)
        # forked from the SAME preload_modules=["torch"] fork server!
        @ray.remote(num_cpus=0)
        class RayConstProbeActor:
            def verify(self):
                import torch as _torch

                from ray._private import ray_constants as rc

                gw = ray._private.worker.global_worker
                ser_ctx = gw.get_serialization_context()
                ref = ray.put(_torch.arange(65536, dtype=_torch.float32))
                t_a = ray.get(ref)
                t_b = ray.get(ref)
                return {
                    "ppid": os.getppid(),
                    "cpp_job_id": ray._raylet.Config.JOB_ID(),
                    "rc_zero_copy": rc.RAY_ENABLE_ZERO_COPY_TORCH_TENSORS,
                    "ctx_zero_copy": ser_ctx._zero_copy_tensors_enabled,
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
        assert res_z1["shares_plasma_ptr"] is True
        assert res_z1["rc_actor_log"] is True
        assert res_z1["gw_actor_log"] is True

        assert res_z0["rc_zero_copy"] is False
        assert res_z0["ctx_zero_copy"] is False
        assert res_z0["shares_plasma_ptr"] is False
        assert res_z0["rc_actor_log"] is False
        assert res_z0["gw_actor_log"] is False
    finally:
        ray.shutdown()


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_prometheus_metrics_observability(shutdown_only, monkeypatch):
    """Verify live Prometheus scrape reports ray_node_fork_server_count (1 -> 2 -> 1),
    Component='ray::fork_server' stats, and Raylet forked/fallback worker counters."""
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

    @ray.remote(num_cpus=0)
    class HoldActor:
        def ids(self):
            return os.getpid(), os.getppid()

    # Keep a forked default actor alive and confirm it is NOT miscounted as a fork server.
    default_actor = HoldActor.remote()
    _, default_fs_pid = ray.get(default_actor.ids.remote())

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
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
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
