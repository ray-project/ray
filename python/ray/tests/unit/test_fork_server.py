import json
import os
import signal
import socket
import stat
import sys
import tempfile
import time
from types import SimpleNamespace

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
import gc, json, os, random, signal, sys
import numpy as np
import ray._private.utils as ray_utils
import ray._private.workers.default_worker as default_worker

np.random.rand()  # Initialize numpy's lazily-seeded global RNG before fork().

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
    info = {
        "pid": os.getpid(), "pgid": os.getpgid(0), "argv": sys.argv[1:],
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
    }
    out = os.environ["PROBE_OUT"]
    with open(out + ".tmp", "w") as f:
        json.dump(info, f)
    os.replace(out + ".tmp", out)
    os._exit(0)

default_worker.initialize_default_worker = _report_and_exit
"""


def _request(sock_path: str, payload: bytes) -> bytes:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(5.0)
        s.connect(sock_path)
        s.sendall(payload)
        return s.makefile("rb").readline()


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

        # Malformed and expired requests must not fork (error reply, no pid).
        assert "error" in json.loads(_request(sock_path, b"not-json\n"))
        assert "error" in json.loads(_request(sock_path, b'{"args": null}\n'))
        expired = json.dumps({"env": {"PROBE_OUT": "x"}, "deadline_ms": 1})
        reply = _request(sock_path, expired.encode() + b"\n")
        assert json.loads(reply) == {"error": "request expired"}

        children = []
        user_py_dir = str(tmp_path / "user_py")
        for idx in range(2):
            out = tmp_path / f"child_{idx}.json"
            req = {
                "args": [f"--idx={idx}"],
                "env": {"PROBE_OUT": str(out)},
                "runtime_env_vars": {"PYTHONPATH": user_py_dir} if idx == 1 else {},
            }
            resp = json.loads(_request(sock_path, json.dumps(req).encode() + b"\n"))
            wait_for_condition(out.exists, timeout=10)
            info = json.loads(out.read_text())
            assert info["pid"] == info["pgid"] == resp["pid"]
            assert info["argv"] == [f"--idx={idx}"]
            children.append(info)

        assert children[0]["rand"] != children[1]["rand"]
        assert children[0]["np_rand"] != children[1]["np_rand"]
        # Working-dir plugin path (tmp_path) must stay ahead of user_py_dir.
        assert children[1]["sys_path"][1:3] == [str(tmp_path), user_py_dir]
        assert children[1]["pythonpath"].split(os.pathsep)[:2] == [
            str(tmp_path),
            user_py_dir,
        ]
        for info in children:
            assert info["freeze_count"] > 0
            assert info["native_setter_kinds"]
            if info["openblas_threads"] is not None:
                assert info["openblas_threads"] == 6
            assert (info["omp"], info["je_arrow"], info["job_id"]) == ("6", None, None)
            assert info["sigint_ok"] and info["sig_dfl"]
            # Verify forked children are auto-reaped while the server is still alive
            # (no zombie /proc/<pid> entries left behind).
            wait_for_condition(
                lambda pid=info["pid"]: not os.path.exists(f"/proc/{pid}"),
                timeout=5,
            )
    finally:
        server.stop()

    assert server.process.poll() is not None
    assert not os.path.exists(sock_path)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
