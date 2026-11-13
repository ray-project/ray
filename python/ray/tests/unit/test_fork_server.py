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

# Preloaded into the fork server; replaces the worker entrypoint so each forked child
# reports its post-fork state and exits instead of connecting to a raylet.
_PROBE_MODULE = """
import gc, json, os, random, signal, sys
import numpy as np
import ray._private.workers.default_worker as default_worker

np.random.rand()  # Initialize numpy's lazily-seeded global RNG before fork().

def _report_and_exit():
    info = {
        "pid": os.getpid(), "pgid": os.getpgid(0), "argv": sys.argv[1:],
        "freeze_count": gc.get_freeze_count(),
        "rand": random.random(), "np_rand": float(np.random.rand()),
        "omp": os.environ.get("OMP_NUM_THREADS"),
        "je_arrow": os.environ.get("JE_ARROW_MALLOC_CONF"),
        "job_id": os.environ.get("RAY_JOB_ID"),
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

    py_dirs = [str(tmp_path / "py1"), str(tmp_path / "py2")]
    child_env = {
        "RAY_JOB_ID": "01000000",
        "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1",
        "PYTHONPATH": os.pathsep.join(py_dirs),
        "TZ": "UTC",
    }
    runtime_env_vars = {
        "JOB_SCOPED": "${RAY_JOB_ID}/job",
        "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS": "1",
        "OMP_NUM_THREADS": "8",
    }
    for k in (*child_env, *runtime_env_vars, "OMP_NUM_THREADS"):
        monkeypatch.delenv(k, raising=False)
    _prepare_child_environment(
        {"OMP_NUM_THREADS": "4"}, child_env, ["--worker-id=abc"], runtime_env_vars
    )

    assert os.environ["JOB_SCOPED"] == "01000000/job"
    assert not hasattr(time, "tzset") or time.tzname[0] == "UTC"
    assert sys.path[1:3] == py_dirs
    assert gw._enable_record_actor_task_log is True
    assert "stale" not in gw.serialization_context_map
    assert (fake_torch.threads, fake_torch.seeded) == (8, True)
    assert fake_torch_cuda._cached_device_count is None
    assert sys.argv == ["fork_server.py", "--worker-id=abc"]


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
        for idx in range(2):
            out = tmp_path / f"child_{idx}.json"
            req = {"args": [f"--idx={idx}"], "env": {"PROBE_OUT": str(out)}}
            resp = json.loads(_request(sock_path, json.dumps(req).encode() + b"\n"))
            wait_for_condition(out.exists, timeout=10)
            info = json.loads(out.read_text())
            assert info["pid"] == info["pgid"] == resp["pid"]
            assert info["argv"] == [f"--idx={idx}"]
            children.append(info)

        assert children[0]["rand"] != children[1]["rand"]
        assert children[0]["np_rand"] != children[1]["np_rand"]
        for info in children:
            assert info["freeze_count"] > 0
            assert (info["omp"], info["je_arrow"], info["job_id"]) == ("6", None, None)
            assert info["sigint_ok"] and info["sig_dfl"]
    finally:
        server.stop()

    assert server.process.poll() is not None
    assert not os.path.exists(sock_path)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
