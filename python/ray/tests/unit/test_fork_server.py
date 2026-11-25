import json
import os
import select
import signal
import socket
import stat
import sys
import tempfile
import time
from contextlib import suppress
from types import SimpleNamespace
from typing import List, Tuple

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.workers.fork_server import (
    ForkServerProcess,
    _preload_modules,
    _prepare_child_environment,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="ForkServer is Linux-only."
)

# Preloaded into the fork server; replaces the worker entrypoint so each forked child
# reports its post-fork state and exits instead of connecting to a raylet.
_PROBE_MODULE = """
import gc, json, os, random, signal, subprocess, sys, time
import numpy as np
import ray._private.utils as ray_utils
import ray._private.workers.default_worker as default_worker

np.random.rand()  # Initialize numpy's lazily-seeded global RNG before fork().
_PRELOAD_SUBPROC_RC = subprocess.run(
    [sys.executable, "-c", "import sys; sys.exit(3)"], check=False
).returncode

def _get_openblas_threads():
    for kind, lib, _ in ray_utils._cached_native_thread_pool_setters or ():
        if kind != "openblas":
            continue
        for sym in (
            "scipy_openblas_get_num_threads64_",
            "scipy_openblas_get_num_threads",
            "openblas_get_num_threads64_",
            "openblas_get_num_threads",
        ):
            if (fn := getattr(lib, sym, None)) is not None:
                return int(fn())
    return None

def _report_and_exit():
    grandchild_pid = None
    if os.environ.get("SPAWN_GRANDCHILD") == "1":
        gpid = os.fork()
        if gpid == 0:
            time.sleep(30)
            os._exit(0)
        grandchild_pid = gpid

    info = {
        "pid": os.getpid(), "pgid": os.getpgid(0), "argv": sys.argv[1:],
        "grandchild_pid": grandchild_pid,
        "preload_subproc_rc": _PRELOAD_SUBPROC_RC,
        "freeze_count": gc.get_freeze_count(),
        "rand": random.random(), "np_rand": float(np.random.rand()),
        "omp": os.environ.get("OMP_NUM_THREADS"),
        "je_arrow": os.environ.get("JE_ARROW_MALLOC_CONF"),
        "job_id": os.environ.get("RAY_JOB_ID"),
        "pythonpath": os.environ.get("PYTHONPATH", ""),
        "sys_path": sys.path[:4],
        "native_setter_kinds": [
            k for k, _, _ in (ray_utils._cached_native_thread_pool_setters or ())
        ],
        "openblas_threads": _get_openblas_threads(),
        "sigint_ok": signal.getsignal(signal.SIGINT) is signal.default_int_handler
        and signal.SIGINT not in signal.pthread_sigmask(signal.SIG_BLOCK, []),
        "sig_dfl": signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL
        and signal.getsignal(signal.SIGTERM) == signal.SIG_DFL,
        "stdin_eof": os.read(0, 1) == b"",
    }
    out = os.environ["PROBE_OUT"]
    with open(out + ".tmp", "w") as f:
        json.dump(info, f)
    os.replace(out + ".tmp", out)
    hold_path = os.environ.get("HOLD_UNTIL_FILE")
    if hold_path:
        deadline = time.monotonic() + 10.0
        while not os.path.exists(hold_path) and time.monotonic() < deadline:
            time.sleep(0.01)
    os._exit(0)

default_worker.initialize_default_worker = _report_and_exit
"""


def _request(
    sock_path: str, payload: bytes, half_close: bool = False
) -> Tuple[bytes, List[int]]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(5.0)
        sock.connect(sock_path)
        sock.sendall(payload)
        if half_close:
            sock.shutdown(socket.SHUT_WR)
        msg, fds, _flags, _addr = socket.recv_fds(sock, 4096, 4)
        while b"\n" not in msg:
            chunk, more_fds, _flags, _addr = socket.recv_fds(sock, 4096, 4)
            fds.extend(more_fds)
            if not chunk:
                break
            msg += chunk
        return msg, fds


def _wait_pipe_eof(fd: int, timeout_s: float = 5.0) -> bool:
    poller = select.poll()
    poller.register(fd, select.POLLIN | select.POLLHUP)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        events = poller.poll(50)
        for _ev_fd, mask in events:
            if mask & (select.POLLHUP | select.POLLERR | select.POLLNVAL):
                return True
            if mask & select.POLLIN and not os.read(fd, 64):
                return True
    return False


def test_preload_modules_and_prepare_child_environment(monkeypatch, tmp_path):
    """Verify preload list merging and in-process post-fork state reset."""
    (tmp_path / "preload_pkg.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("RAY_preload_python_modules", " preload_pkg ,json")
    _preload_modules("json,nonexistent_module_xyz")
    assert sys.modules.pop("preload_pkg", None) is not None

    fake_torch = SimpleNamespace(threads=1, seeded=False)
    fake_torch.set_num_threads = lambda n: setattr(fake_torch, "threads", n)
    fake_torch.seed = lambda: setattr(fake_torch, "seeded", True)
    fake_torch_cuda = SimpleNamespace(_cached_device_count=8)
    monkeypatch.setitem(sys.modules, "torch", fake_torch)
    monkeypatch.setitem(sys.modules, "torch.cuda", fake_torch_cuda)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(sys, "argv", ["fork_server.py"])
    monkeypatch.setattr(ray._private.utils, "_torch_backends_autoload_pending", False)
    for attr in (
        "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING",
        "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS",
    ):
        monkeypatch.setattr(
            ray._private.ray_constants,
            attr,
            getattr(ray._private.ray_constants, attr),
        )
    gw = ray._private.worker.global_worker
    monkeypatch.setattr(gw, "_enable_record_actor_task_log", False)
    gw.serialization_context_map["stale"] = object()

    monkeypatch.setattr(tempfile, "tempdir", "/stale_tmp")
    py_dirs = [str(tmp_path / "py1"), str(tmp_path / "py2")]
    child_env = {
        "RAY_JOB_ID": "01000000",
        "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1",
        "PYTHONPATH": os.pathsep.join(py_dirs),
        "TZ": "EST+5",
        "TMPDIR": str(tmp_path),
    }
    runtime_env_vars = {
        "JOB_SCOPED": "${RAY_JOB_ID}/job",
        "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS": "1",
        "OMP_NUM_THREADS": "8",
    }
    saved_env = {k: os.environ.pop(k, None) for k in (*child_env, *runtime_env_vars)}
    try:
        _prepare_child_environment(
            {"OMP_NUM_THREADS": "4"}, child_env, ["--worker-id=abc"], runtime_env_vars
        )
        assert os.environ["JOB_SCOPED"] == "01000000/job"
        assert not hasattr(time, "tzset") or time.tzname[0] == "EST"
        assert tempfile.tempdir is None
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        if hasattr(time, "tzset"):
            time.tzset()
    assert sys.path[1:3] == py_dirs
    assert gw._enable_record_actor_task_log is True
    assert "stale" not in gw.serialization_context_map
    assert (fake_torch.threads, fake_torch.seeded) == (8, True)
    assert fake_torch_cuda._cached_device_count is None
    assert sys.argv == ["fork_server.py", "--worker-id=abc"]


@pytest.mark.parametrize(
    "runtime_env_pp,expected_pp,expect_cluster_in_sys_path",
    [
        ("user", ["user"], False),
        ("user:${PYTHONPATH}", ["user", "cluster", "prefix"], True),
        ("", [], False),
    ],
)
def test_prepare_child_environment_pythonpath_default_server(
    monkeypatch, tmp_path, runtime_env_pp, expected_pp, expect_cluster_in_sys_path
):
    """Default fork server with parent PYTHONPATH and worker env omitting it."""
    cluster_dir = str(tmp_path / "cluster")
    user_dir = str(tmp_path / "user")
    prefix_dir = os.path.join(os.path.abspath(sys.prefix), "lib")
    parent_pp = os.pathsep.join([cluster_dir, prefix_dir])
    monkeypatch.setenv("PYTHONPATH", parent_pp)
    monkeypatch.setattr(sys, "path", ["", cluster_dir, prefix_dir, "/other"])
    monkeypatch.setattr(sys, "argv", ["fork_server.py"])

    expanded_input = (
        runtime_env_pp.replace("user", user_dir).replace(":", os.pathsep)
        if runtime_env_pp
        else ""
    )
    _prepare_child_environment(
        {},
        {},  # BuildProcessCommandArgs omits PYTHONPATH from worker env.
        [],
        {"PYTHONPATH": expanded_input},
        plugin_py_dirs=(),
        cluster_py_dirs=(cluster_dir, prefix_dir),
    )

    name_to_dir = {"user": user_dir, "cluster": cluster_dir, "prefix": prefix_dir}
    expected_dirs = [name_to_dir[k] for k in expected_pp]
    if expected_dirs:
        assert os.environ["PYTHONPATH"] == os.pathsep.join(expected_dirs)
    else:
        assert "PYTHONPATH" not in os.environ
    assert sys.path[1 : 1 + len(expected_dirs)] == expected_dirs
    assert prefix_dir in sys.path
    assert (cluster_dir in sys.path) is expect_cluster_in_sys_path


@pytest.mark.parametrize("runtime_env_pp", ["user", "user:${PYTHONPATH}"])
def test_prepare_child_environment_pythonpath_working_dir_server(
    monkeypatch, tmp_path, runtime_env_pp
):
    """Working-dir server preserves plugin > user > cluster order without worker PYTHONPATH."""
    plugin_dir = str(tmp_path / "working_dir")
    cluster_dir = str(tmp_path / "cluster")
    user_dir = str(tmp_path / "user")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([plugin_dir, cluster_dir]))
    monkeypatch.setattr(sys, "path", ["", plugin_dir, cluster_dir, "/other"])
    monkeypatch.setattr(sys, "argv", ["fork_server.py"])

    expanded_input = runtime_env_pp.replace("user", user_dir).replace(":", os.pathsep)
    _prepare_child_environment(
        {},
        {},  # BuildProcessCommandArgs omits PYTHONPATH from worker env.
        [],
        {"PYTHONPATH": expanded_input},
        plugin_py_dirs=(plugin_dir,),
        cluster_py_dirs=(cluster_dir,),
    )

    expected = [plugin_dir, user_dir, cluster_dir]
    assert os.environ["PYTHONPATH"] == os.pathsep.join(expected)
    assert sys.path[1:4] == expected


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_end_to_end(monkeypatch, tmp_path, request):
    """Verify socket security, request handling, and forked-child isolation."""
    (tmp_path / "probe_pkg.py").write_text(_PROBE_MODULE)
    monkeypatch.setenv("OMP_NUM_THREADS", "6")
    monkeypatch.delenv("JE_ARROW_MALLOC_CONF", raising=False)
    monkeypatch.setenv("RAY_JOB_ID", "parent_job")
    # Use a 107-byte socket path (Linux AF_UNIX limit) so staging .f<pid> exercises
    # the /proc/self/fd/<dir_fd>/.f<pid> bind fallback. Root it in /tmp since
    # Bazel's TEST_TMPDIR can already be too long.
    sock_dir = tempfile.TemporaryDirectory(dir="/tmp", prefix="rfs_")
    request.addfinalizer(sock_dir.cleanup)
    pad_len = max(1, 107 - len(sock_dir.name) - len("//s"))
    sock_path = os.path.join(sock_dir.name, "d" * pad_len, "s")
    (tmp_path / "cgroup.procs").write_text("")
    ctx = RuntimeEnvContext(
        env_vars={"PYTHONPATH": f"{tmp_path}{os.pathsep}${{PYTHONPATH}}"}
    )

    # Start the server with SIGINT ignored and blocked; children must restore both.
    old_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
    try:
        server = ForkServerProcess(
            sock_path, ["probe_pkg"], ctx, cgroup_path=str(tmp_path)
        )
    finally:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
        signal.signal(signal.SIGINT, old_sigint)

    try:
        assert (tmp_path / "cgroup.procs").read_text() == str(server.process.pid)
        assert stat.S_IMODE(os.stat(sock_path).st_mode) == 0o600
        assert len(os.listdir(f"/proc/{server.process.pid}/task")) == 1
        baseline_fd_count = len(os.listdir(f"/proc/{server.process.pid}/fd"))

        # A client that disconnects before accept is dropped cleanly without crashing.
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(sock_path)

        # Malformed payloads fail in the child before replying (EOF on request socket).
        for bad_payload in (b"not-json\n", b'{"args": null}\n'):
            raw, fds = _request(sock_path, bad_payload)
            try:
                assert raw == b""
                assert fds == []
            finally:
                for fd in fds:
                    os.close(fd)

        # Oversized (>4 MiB) payload closes the child socket (EOF) without harming server.
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as big_sock:
            big_sock.settimeout(5.0)
            big_sock.connect(sock_path)
            with suppress(BrokenPipeError, ConnectionResetError):
                big_sock.sendall(b"x" * (4 * 1024 * 1024 + 64) + b"\n")
            raw, fds, _flags, _addr = socket.recv_fds(big_sock, 4096, 4)
            try:
                assert raw == b""
                assert fds == []
            finally:
                for fd in fds:
                    os.close(fd)

        # An idle client must not block subsequent fork requests (fork-before-read).
        children = []
        user_py_dir = str(tmp_path / "user_py")
        grandchild_pids = []
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as idle_sock:
                idle_sock.settimeout(5.0)
                idle_sock.connect(sock_path)
                for idx in range(2):
                    out = tmp_path / f"child_{idx}.json"
                    hold = tmp_path / f"release_{idx}"
                    env = {"PROBE_OUT": str(out), "HOLD_UNTIL_FILE": str(hold)}
                    if idx == 0:
                        # Grandchild forks must close the lifetime pipe write end via at_fork.
                        env["SPAWN_GRANDCHILD"] = "1"
                    req = {
                        "args": [f"--idx={idx}"],
                        "env": env,
                        "runtime_env_vars": (
                            {"PYTHONPATH": user_py_dir} if idx == 1 else {}
                        ),
                    }
                    raw, fds = _request(
                        sock_path,
                        json.dumps(req).encode() + b"\n",
                        half_close=(idx == 1),
                    )
                    assert len(fds) == 1
                    life_r = fds[0]
                    try:
                        resp = json.loads(raw)
                        wait_for_condition(out.exists, timeout=10)
                        info = json.loads(out.read_text())
                        if info["grandchild_pid"] is not None:
                            grandchild_pids.append(info["grandchild_pid"])
                        assert info["pid"] == info["pgid"] == resp["pid"]
                        assert info["argv"] == [f"--idx={idx}"]
                        # While the child is alive, its lifetime pipe must not report EOF.
                        poller = select.poll()
                        poller.register(life_r, select.POLLHUP)
                        assert poller.poll(0) == []
                        # Release child and verify lifetime pipe hits EOF even if a
                        # grandchild is still sleeping.
                        hold.write_text("go")
                        assert _wait_pipe_eof(life_r, timeout_s=5.0)
                        children.append(info)
                    finally:
                        os.close(life_r)
        finally:
            for grandchild_pid in grandchild_pids:
                with suppress(OSError):
                    os.kill(grandchild_pid, signal.SIGKILL)

        assert children[0]["rand"] != children[1]["rand"]
        assert children[0]["np_rand"] != children[1]["np_rand"]
        # Working-dir plugin path (tmp_path) must stay ahead of user_py_dir.
        assert children[1]["sys_path"][1:3] == [str(tmp_path), user_py_dir]
        assert children[1]["pythonpath"].split(os.pathsep)[:2] == [
            str(tmp_path),
            user_py_dir,
        ]
        for info in children:
            assert info["preload_subproc_rc"] == 3
            assert info["freeze_count"] > 0
            assert info["native_setter_kinds"]
            if info["openblas_threads"] is not None:
                assert info["openblas_threads"] == 6
            assert (info["omp"], info["je_arrow"], info["job_id"]) == ("6", None, None)
            assert info["sigint_ok"] and info["sig_dfl"] and info["stdin_eof"]
            # Forked children must be auto-reaped by the kernel (no zombies).
            wait_for_condition(
                lambda pid=info["pid"]: not os.path.exists(f"/proc/{pid}"),
                timeout=5,
            )

        # Parent server FD count must remain flat (no leaked life_r or conn FDs).
        wait_for_condition(
            lambda: len(os.listdir(f"/proc/{server.process.pid}/fd"))
            == baseline_fd_count,
            timeout=5,
        )

        # Closing stdin triggers --monitor-stdin lifeline EOF, clean exit, and unlink.
        assert server.process.stdin is not None
        server.process.stdin.close()
        assert server.process.wait(timeout=5.0) == 0
        assert not os.path.exists(sock_path)
    finally:
        server.stop()

    assert server.process.poll() is not None
    assert not os.path.exists(sock_path)

    # SIGTERM must exit cleanly (code 0) via wakeup pipe and unlink its own socket.
    sigterm_sock = os.path.join(sock_dir.name, "fs_sigterm")
    sigterm_server = ForkServerProcess(sigterm_sock, [])
    try:
        os.kill(sigterm_server.process.pid, signal.SIGTERM)
        assert sigterm_server.process.wait(timeout=5.0) == 0
        assert not os.path.exists(sigterm_sock)
    finally:
        sigterm_server.stop()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
