"""End-to-end cluster tests for the Linux Python worker fork server."""

import json
import os
import random
import signal
import sys
from pathlib import Path
from typing import Dict

import numpy as np
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="ForkServer is Linux-only."
)


def _pid_exists(pid: int) -> bool:
    """Returns True if a live (non-zombie) process with `pid` exists."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    stat_path = Path(f"/proc/{pid}/stat")
    if stat_path.exists():
        try:
            state = stat_path.read_text().rsplit(")", 1)[1].split()[0]
            if state == "Z":
                return False
        except OSError:
            pass
    return True


def _find_fork_server_pids(session_dir: str) -> Dict[str, int]:
    """Returns mapping of socket path -> PID for live fork_server processes."""
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
        except OSError:
            continue
        if not cmdline or cmdline[0].startswith("ray::"):
            continue
        if any(
            os.path.basename(part) in ("setup_worker.py", "_cgroup_bootstrap.py")
            for part in cmdline
        ):
            continue
        if not any(
            part == "ray._private.workers.fork_server"
            or os.path.basename(part) == "fork_server.py"
            for part in cmdline
        ):
            continue
        for i, arg in enumerate(cmdline):
            if arg == "--socket-path" and i + 1 < len(cmdline):
                sock = cmdline[i + 1]
            elif arg.startswith("--socket-path="):
                sock = arg.split("=", 1)[1]
            else:
                continue
            if session_dir in sock:
                pid = int(entry.name)
                if _pid_exists(pid):
                    result[sock] = pid
    return result


def _wait_for_fork_server_ready(
    session_dir: str,
    sock_path: str,
    *,
    exclude_pid: int = -1,
    exclude_stat=None,
    timeout: float = 15.0,
) -> int:
    """Waits until the fork server process is running AND its Unix socket is published."""
    matched_pid = -1

    def _ready() -> bool:
        nonlocal matched_pid
        pids = _find_fork_server_pids(session_dir)
        pid = pids.get(sock_path)
        if pid is None or pid == exclude_pid:
            return False
        try:
            st = os.lstat(sock_path)
        except OSError:
            return False
        if (
            exclude_stat is not None
            and (st.st_dev, st.st_ino, st.st_ctime_ns) == exclude_stat
        ):
            return False
        matched_pid = pid
        return True

    wait_for_condition(_ready, timeout=timeout)
    return matched_pid


def _default_fork_server_socket(ctx) -> str:
    """Returns the node-scoped default fork server socket path for `ctx`."""
    return os.path.join(
        ctx.address_info["session_dir"],
        "sockets",
        f"fs_{ctx.address_info['node_id'][:8]}_0",
    )


def test_fork_server_default_tasks_actors_and_prng_independence(shutdown_only):
    """Default fork server forks tasks and actors single-threaded with independent PRNGs."""
    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = _default_fork_server_socket(ctx)
    fs_pid = _wait_for_fork_server_ready(session_dir, default_sock)
    assert len(os.listdir(f"/proc/{fs_pid}/task")) == 1

    @ray.remote
    def probe_task():
        return {
            "ppid": os.getppid(),
            "argv0": os.path.basename(sys.argv[0]),
        }

    task_info = ray.get(probe_task.remote())
    assert task_info == {"ppid": fs_pid, "argv0": "default_worker.py"}

    @ray.remote
    class ProbeActor:
        def info(self):
            return {
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "argv0": os.path.basename(sys.argv[0]),
                "py_rand": random.random(),
                "np_rand": float(np.random.rand()),
                "obj_id": ray.ObjectID.from_random().hex(),
            }

    actors = [ProbeActor.remote() for _ in range(3)]
    infos = ray.get([a.info.remote() for a in actors])
    assert len({i["pid"] for i in infos}) == 3
    assert all(i["ppid"] == fs_pid for i in infos)
    assert all(i["argv0"] == "default_worker.py" for i in infos)
    assert len({i["py_rand"] for i in infos}) == 3
    assert len({i["np_rand"] for i in infos}) == 3
    assert len({i["obj_id"] for i in infos}) == 3

    for a in actors:
        ray.kill(a)
    ray.shutdown()
    wait_for_condition(lambda: not _find_fork_server_pids(session_dir), timeout=10)


def test_fork_server_env_vars_config_and_setup_hook_sharing(shutdown_only):
    """Distinct env_vars, config, and worker_process_setup_hook reuse fs_0."""

    def _setup_hook():
        os.environ["SETUP_HOOK_RAN"] = f"hook-{os.environ.get('RANK', '')}"

    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={"worker_process_setup_hook": _setup_hook},
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = _default_fork_server_socket(ctx)
    fs_pid = _wait_for_fork_server_ready(session_dir, default_sock)

    @ray.remote
    class EnvActor:
        def read(self):
            return {
                "ppid": os.getppid(),
                "rank": os.environ.get("RANK"),
                "hook": os.environ.get("SETUP_HOOK_RAN"),
                "log_level": ray._private.ray_constants.LOGGER_LEVEL,
            }

    a0 = EnvActor.options(
        runtime_env={
            "env_vars": {"RANK": "0", "RAY_LOGGER_LEVEL": "debug"},
            "config": {"setup_timeout_seconds": 90},
        }
    ).remote()
    a1 = EnvActor.options(
        runtime_env={
            "env_vars": {"RANK": "1"},
            "config": {"setup_timeout_seconds": 120},
        }
    ).remote()
    r0, r1 = ray.get([a0.read.remote(), a1.read.remote()])
    assert r0 == {
        "ppid": fs_pid,
        "rank": "0",
        "hook": "hook-0",
        "log_level": "debug",
    }
    assert r1["ppid"] == fs_pid
    assert r1["rank"] == "1"
    assert r1["hook"] == "hook-1"
    # No extra per-env fork servers should have been started.
    assert set(_find_fork_server_pids(session_dir).keys()) == {default_sock}


def test_fork_server_working_dir_preload_exec_bound_and_teardown(
    shutdown_only, tmp_path
):
    """Per-env fork servers share across normal env_vars, split on exec-bound vars, and stop at refcount 0."""
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    (work_dir / "preloaded_helper.py").write_text("VALUE = 42\n")
    (work_dir / "ignored.tmp").write_text("ignore me\n")

    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = _default_fork_server_socket(ctx)
    default_pid = _wait_for_fork_server_ready(session_dir, default_sock)

    @ray.remote
    class WorkActor:
        def __init__(self):
            self.preloaded = "preloaded_helper" in sys.modules

        def info(self):
            import preloaded_helper

            return {
                "ppid": os.getppid(),
                "preloaded": self.preloaded,
                "value": preloaded_helper.VALUE,
                "rank": os.environ.get("RANK"),
                "hash_seed": os.environ.get("PYTHONHASHSEED"),
                "excluded_present": os.path.exists("ignored.tmp"),
            }

    base_env = upload_working_dir_if_needed(
        {
            "working_dir": str(work_dir),
            "excludes": ["*.tmp"],
            "preload_modules": ["preloaded_helper"],
        },
        include_gitignore=False,
        scratch_dir=str(tmp_path),
    )
    # Warm working_dir URI cache and first per-env fork server before concurrent actors.
    a0 = WorkActor.options(runtime_env={**base_env, "env_vars": {"RANK": "0"}}).remote()
    i0 = ray.get(a0.info.remote())

    a1 = WorkActor.options(runtime_env={**base_env, "env_vars": {"RANK": "1"}}).remote()
    a_exec = WorkActor.options(
        runtime_env={**base_env, "env_vars": {"PYTHONHASHSEED": "7"}}
    ).remote()
    i1, i_exec = ray.get([a1.info.remote(), a_exec.info.remote()])
    active_fs_pids = set(_find_fork_server_pids(session_dir).values())
    assert i0["preloaded"] is True and i0["value"] == 42 and i0["rank"] == "0"
    assert i0["excluded_present"] is False
    assert i1["preloaded"] is True and i1["rank"] == "1"
    # Non-exec-bound RANK shares one per-env fork server distinct from fs_0.
    assert i0["ppid"] == i1["ppid"]
    assert i0["ppid"] != default_pid
    assert i0["ppid"] in active_fs_pids
    # Exec-bound PYTHONHASHSEED splits into its own fork server (not execvpe fallback).
    assert i_exec["preloaded"] is True
    assert i_exec["ppid"] in active_fs_pids
    assert i_exec["ppid"] != i0["ppid"]
    assert i_exec["ppid"] != default_pid
    assert i_exec["hash_seed"] == "7"

    # Killing all per-env actors drops refcount to 0 and tears down per-env servers.
    for a in (a0, a1, a_exec):
        ray.kill(a)
    wait_for_condition(
        lambda: set(_find_fork_server_pids(session_dir).keys()) == {default_sock},
        timeout=15,
    )


def test_fork_server_multi_arg_wrapper_bypass_and_crash_recovery(shutdown_only):
    """Multi-arg py_executable falls back to exec, and killed fs_0 respawns cleanly."""
    ctx = ray.init(
        num_cpus=2,
        include_dashboard=False,
        _system_config={"enable_worker_fork_server": True},
    )
    session_dir = ctx.address_info["session_dir"]
    default_sock = _default_fork_server_socket(ctx)
    fs_pid_1 = _wait_for_fork_server_ready(session_dir, default_sock)
    st_1 = os.lstat(default_sock)
    old_stat = (st_1.st_dev, st_1.st_ino, st_1.st_ctime_ns)

    @ray.remote
    class SimpleActor:
        def ppid(self):
            return os.getppid()

    # Multi-argument py_executable wrapper must bypass fork server.
    wrapped = SimpleActor.options(
        runtime_env={"py_executable": f"{sys.executable} -u"}
    ).remote()
    assert ray.get(wrapped.ppid.remote()) != fs_pid_1
    assert set(_find_fork_server_pids(session_dir).keys()) == {default_sock}
    ray.kill(wrapped)

    # Kill fs_0: supervisor must respawn it and new actors fork from the new server.
    os.kill(fs_pid_1, signal.SIGKILL)
    fs_pid_2 = _wait_for_fork_server_ready(
        session_dir, default_sock, exclude_pid=fs_pid_1, exclude_stat=old_stat
    )
    recovered = SimpleActor.remote()
    assert ray.get(recovered.ppid.remote()) == fs_pid_2
    ray.kill(recovered)


def test_fork_server_io_workers_spilling_and_restoration(shutdown_only, tmp_path):
    """SPILL_WORKER and RESTORE_WORKER operate cleanly with fork server enabled."""
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
    default_sock = _default_fork_server_socket(ctx)
    _wait_for_fork_server_ready(session_dir, default_sock)

    refs = [ray.put(np.full((10 * 1024 * 1024,), i, dtype=np.uint8)) for i in range(10)]
    for i, ref in enumerate(refs):
        arr = ray.get(ref)
        assert int(arr[0]) == i and int(arr[-1]) == i
    assert default_sock in _find_fork_server_pids(session_dir)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
