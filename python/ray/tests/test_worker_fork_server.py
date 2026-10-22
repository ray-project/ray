import gc
import json
import os
import random
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict

import numpy as np
import pytest

import ray
from ray._common.test_utils import wait_for_condition
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
        if not any(
            "ray._private.workers.fork_server" in part or "fork_server.py" in part
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

    ray.kill(actor)
    wait_for_condition(lambda: not _pid_exists(info["pid"]), timeout=10)
    wait_for_condition(lambda: not _pid_exists(info["child_pid"]), timeout=10)
    assert _pid_exists(default_fs_pid)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_timeout_and_dead_server_fallback(shutdown_only):
    """Verify 500ms SO_RCVTIMEO fallback on hung ForkServer and Spawnvpe fallback when ForkServer is killed."""
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
    _, ppid1 = ray.get(a1.get_ids.remote())
    assert ppid1 == default_fs_pid

    # Pause ForkServer with SIGSTOP so kernel accepts AF_UNIX connections in backlog
    # without replying; spawning a new actor must time out (~500ms) and fall back to execvpe.
    os.kill(default_fs_pid, signal.SIGSTOP)
    try:
        t0 = time.monotonic()
        a2 = PingActor.options(num_cpus=0).remote()
        _, raylet_ppid = ray.get(a2.get_ids.remote())
        elapsed = time.monotonic() - t0
        assert raylet_ppid != default_fs_pid
        assert elapsed < 10.0
    finally:
        os.kill(default_fs_pid, signal.SIGCONT)

    # Once resumed, subsequent actors fork from ForkServer again.
    a3 = PingActor.options(num_cpus=0).remote()
    _, ppid3 = ray.get(a3.get_ids.remote())
    assert ppid3 == default_fs_pid

    # Kill ForkServer with SIGKILL; WorkerPool must seamlessly fall back to Spawnvpe.
    os.kill(default_fs_pid, signal.SIGKILL)
    wait_for_condition(lambda: not _pid_exists(default_fs_pid), timeout=10)

    a4 = PingActor.options(num_cpus=0).remote()
    _, ppid4 = ray.get(a4.get_ids.remote())
    assert ppid4 != default_fs_pid


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
@pytest.mark.skipif(torch is None, reason="PyTorch is not installed.")
def test_fork_server_per_runtime_env_preload_env_vars_and_cleanup(
    shutdown_only, monkeypatch, tmp_path
):
    """Verify per-runtime_env ForkServer preloads modules (including py_modules), interpolates ${VAR}/$VAR, runs worker_process_setup_hook, handles >64 KiB env payloads, and cleans up on refcount 0."""
    monkeypatch.setenv("EXISTING_HOST_VAR", "host_val")
    pkg_dir = tmp_path / "custom_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text(
        "import os\n"
        "PRELOADED_PID = os.getpid()\n"
        "def setup_hook():\n"
        "    os.environ['SETUP_HOOK_RAN_PID'] = str(os.getpid())\n"
    )

    ctx = ray.init(
        num_cpus=4,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]

    large_payload = "x" * (70 * 1024)
    runtime_env = {
        "py_modules": [f"local://{tmp_path}"],
        "preload_modules": ["json", "torch", "custom_pkg"],
        "env_vars": {
            "__RAY_WORKER_PROCESS_SETUP_HOOK_ENV_VAR": "custom_pkg.setup_hook",
            "A_BASE_DIR": "/tmp/ray_fork_test",
            "B_SUB_DIR": "${A_BASE_DIR}/sub",
            "C_NESTED_DIR": "${B_SUB_DIR}/nested",
            "D_DOLLAR_DIR": "$C_NESTED_DIR/leaf",
            "E_WITH_EXISTING": "prefix_${EXISTING_HOST_VAR}_suffix",
            "F_UNKNOWN_REF": "${DEFINITELY_UNSET_VAR_XYZ}/tail",
            "G_LARGE_PAYLOAD": large_payload,
        },
    }

    @ray.remote(runtime_env=runtime_env, max_calls=1)
    def check_env_task():
        torch_preloaded = "torch" in sys.modules
        custom_pkg_preloaded = "custom_pkg" in sys.modules
        import custom_pkg
        import torch as _torch

        return {
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "torch_preloaded": torch_preloaded,
            "custom_pkg_preloaded": custom_pkg_preloaded,
            "custom_pkg_pid": custom_pkg.PRELOADED_PID,
            "setup_hook_pid": int(os.environ.get("SETUP_HOOK_RAN_PID", "0")),
            "tensor_sum": float(_torch.ones((2, 2)).sum().item()),
            "A_BASE_DIR": os.environ.get("A_BASE_DIR"),
            "B_SUB_DIR": os.environ.get("B_SUB_DIR"),
            "C_NESTED_DIR": os.environ.get("C_NESTED_DIR"),
            "D_DOLLAR_DIR": os.environ.get("D_DOLLAR_DIR"),
            "E_WITH_EXISTING": os.environ.get("E_WITH_EXISTING"),
            "F_UNKNOWN_REF": os.environ.get("F_UNKNOWN_REF"),
            "G_LARGE_PAYLOAD_LEN": len(os.environ.get("G_LARGE_PAYLOAD", "")),
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
                "C_NESTED_DIR": os.environ.get("C_NESTED_DIR"),
            }

    task_res = ray.get(check_env_task.remote())
    actor = EnvTorchActor.remote()
    actor_res = ray.get(actor.info.remote())

    env_fs_pid = actor_res["ppid"]
    assert env_fs_pid != default_fs_pid
    assert len(os.listdir(f"/proc/{env_fs_pid}/task")) == 1
    assert task_res["ppid"] == env_fs_pid
    assert task_res["torch_preloaded"] is True
    assert task_res["custom_pkg_preloaded"] is True
    # custom_pkg was imported inside the ForkServer before os.fork(), so its recorded PID
    # matches the ForkServer PID, while _setup_hook ran post-fork in the child worker PID.
    assert task_res["custom_pkg_pid"] == env_fs_pid
    assert task_res["setup_hook_pid"] == task_res["pid"]
    assert task_res["tensor_sum"] == 4.0
    assert task_res["A_BASE_DIR"] == "/tmp/ray_fork_test"
    assert task_res["B_SUB_DIR"] == "/tmp/ray_fork_test/sub"
    assert task_res["C_NESTED_DIR"] == "/tmp/ray_fork_test/sub/nested"
    assert task_res["D_DOLLAR_DIR"] == "/tmp/ray_fork_test/sub/nested/leaf"
    assert task_res["E_WITH_EXISTING"] == "prefix_host_val_suffix"
    assert task_res["F_UNKNOWN_REF"] == "/tail"
    assert task_res["G_LARGE_PAYLOAD_LEN"] == 70 * 1024

    assert actor_res["torch_preloaded"] is True
    assert actor_res["custom_pkg_preloaded"] is True
    assert actor_res["custom_pkg_pid"] == env_fs_pid
    assert actor_res["setup_hook_pid"] == actor_res["pid"]
    assert actor_res["tensor_sum"] == 9.0
    assert actor_res["C_NESTED_DIR"] == "/tmp/ray_fork_test/sub/nested"

    fs_pids = _find_fork_server_pids(session_dir)
    env_socks = [s for s, p in fs_pids.items() if p == env_fs_pid]
    assert len(env_socks) == 1
    env_sock = env_socks[0]
    assert os.path.exists(env_sock)
    assert stat.S_IMODE(os.stat(env_sock).st_mode) == 0o600

    # Killing the actor drops the runtime_env refcount to 0, terminating its ForkServer.
    ray.kill(actor)
    wait_for_condition(lambda: not _pid_exists(actor_res["pid"]), timeout=15)
    wait_for_condition(lambda: not _pid_exists(env_fs_pid), timeout=15)
    wait_for_condition(lambda: not os.path.exists(env_sock), timeout=15)
    assert _pid_exists(default_fs_pid)

    ray.shutdown()
    wait_for_condition(lambda: not _pid_exists(default_fs_pid), timeout=15)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
@pytest.mark.skipif(torch is None, reason="PyTorch is not installed.")
def test_fork_server_preloaded_torch_cow_memory_and_thread_reconfig(
    shutdown_only, monkeypatch
):
    """Verify CoW page sharing (Shared_Dirty vs Private_Dirty) and post-fork torch thread reconfiguration when torch is preloaded."""
    monkeypatch.setenv("RAY_preload_python_modules", "torch")
    ctx = ray.init(
        num_cpus=4,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = os.path.join(session_dir, "sockets", "fork_server_default.sock")
    wait_for_condition(
        lambda: default_sock in _find_fork_server_pids(session_dir), timeout=10
    )
    default_fs_pid = _find_fork_server_pids(session_dir)[default_sock]
    assert len(os.listdir(f"/proc/{default_fs_pid}/task")) == 1

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

    # Verify num_cpus=2 reconfigures torch intra-op threads to 2, and num_cpus=1 resets to 1.
    @ray.remote(num_cpus=2)
    def check_threads_two():
        import torch as _torch

        return {
            "ppid": os.getppid(),
            "omp_env": os.environ.get("OMP_NUM_THREADS"),
            "torch_threads": _torch.get_num_threads(),
        }

    @ray.remote(num_cpus=1)
    def check_threads_one():
        import torch as _torch

        return {
            "ppid": os.getppid(),
            "omp_env": os.environ.get("OMP_NUM_THREADS"),
            "torch_threads": _torch.get_num_threads(),
        }

    res2 = ray.get(check_threads_two.remote())
    assert res2["ppid"] == default_fs_pid
    assert res2["omp_env"] == "2"
    assert res2["torch_threads"] == 2

    res1 = ray.get(check_threads_one.remote())
    assert res1["ppid"] == default_fs_pid
    assert res1["omp_env"] == "1"
    assert res1["torch_threads"] == 1


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
    assert _pid_exists(default_fs_pid)
