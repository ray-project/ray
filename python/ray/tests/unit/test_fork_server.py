import json
import os
import signal
import socket
import stat
import subprocess
import sys
from pathlib import Path
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


def test_preload_modules_and_prepare_child_environment(monkeypatch, tmp_path):
    """Verify _preload_modules and _prepare_child_environment in-process behavior."""
    pkg_dir = tmp_path / "dummy_preload_mod"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("LOADED = True\n")
    broken_dir = tmp_path / "broken_runtime_mod"
    broken_dir.mkdir()
    (broken_dir / "__init__.py").write_text("raise RuntimeError('preload boom')\n")

    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv(
        "RAY_preload_python_modules", "json, broken_runtime_mod, dummy_preload_mod "
    )

    _preload_modules("math,nonexistent_module_xyz_123,json")
    assert "broken_runtime_mod" not in sys.modules
    assert "dummy_preload_mod" in sys.modules
    assert sys.modules["dummy_preload_mod"].LOADED is True

    fake_torch = SimpleNamespace(
        threads=1,
        seeded=False,
        set_num_threads=lambda n: setattr(fake_torch, "threads", n),
        seed=lambda: setattr(fake_torch, "seeded", True),
    )
    monkeypatch.setitem(sys.modules, "torch", fake_torch)
    monkeypatch.setenv("RAY_JOB_ID", "stale_job_id")
    monkeypatch.setenv("EXISTING_HOST_VAR", "host_val")
    monkeypatch.setenv("OMP_NUM_THREADS", "1")
    monkeypatch.setenv("JE_ARROW_MALLOC_CONF", "background_thread:false")
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(sys, "argv", list(sys.argv))

    extra_py_dir_1 = str(tmp_path / "extra_py_1")
    extra_py_dir_2 = str(tmp_path / "extra_py_2")
    saved_pre_fork_env = {
        "OMP_NUM_THREADS": "4",
        "JE_ARROW_MALLOC_CONF": None,
    }
    child_env = {
        "A_BASE": "/tmp/base",
        "B_NESTED": "${A_BASE}/${EXISTING_HOST_VAR}/leaf",
        "C_UNSET": "${UNSET_VAR_XYZ_999}/tail",
        "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1",
        "PYTHONPATH": f"{extra_py_dir_1}{os.pathsep}{extra_py_dir_2}",
    }
    for k in child_env:
        monkeypatch.delenv(k, raising=False)
    child_args = ["--node-ip-address=127.0.0.1", "--worker-id=abc"]

    monkeypatch.setattr(
        ray._private.worker.global_worker,
        "_enable_record_actor_task_log",
        False,
    )
    _prepare_child_environment(
        saved_pre_fork_env=saved_pre_fork_env,
        child_env=child_env,
        child_args=child_args,
        default_worker_file="/path/to/default_worker.py",
    )

    assert "RAY_JOB_ID" not in os.environ
    assert os.environ.get("OMP_NUM_THREADS") == "4"
    assert "JE_ARROW_MALLOC_CONF" not in os.environ
    assert os.environ.get("A_BASE") == "/tmp/base"
    assert os.environ.get("B_NESTED") == "/tmp/base/host_val/leaf"
    assert os.environ.get("C_UNSET") == "/tail"
    assert sys.path[:2] == [extra_py_dir_1, extra_py_dir_2]
    assert ray._private.worker.global_worker._enable_record_actor_task_log is True
    assert fake_torch.threads == 4
    assert fake_torch.seeded is True
    assert sys.argv == ["/path/to/default_worker.py", *child_args]


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_process_lifecycle_and_socket_permissions(tmp_path):
    """Verify ForkServerProcess starts single-threaded with 0o600 socket permissions and stops cleanly."""
    pkg_dir = tmp_path / "ctx_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("VALUE = 42\n")

    sock_path = str(tmp_path / "sockets" / "fork_server_test.sock")
    ctx = RuntimeEnvContext(
        env_vars={"PYTHONPATH": f"{tmp_path}{os.pathsep}${{PYTHONPATH}}"}
    )
    server = ForkServerProcess(
        socket_path=sock_path,
        preload_modules=["json", "ctx_pkg"],
        context=ctx,
        wait=True,
    )
    try:
        assert os.path.exists(sock_path)
        assert stat.S_IMODE(os.stat(sock_path).st_mode) == 0o600
        assert server.process.poll() is None
        assert len(os.listdir(f"/proc/{server.process.pid}/task")) == 1
    finally:
        server.stop()

    assert server.process.poll() is not None
    assert not os.path.exists(sock_path)


@pytest.mark.skipif(sys.platform != "linux", reason="ForkServer is Linux-only.")
def test_fork_server_direct_socket_fork_and_child_isolation(tmp_path):
    """Verify _run_fork_server() forks children over AF_UNIX with CoW freeze, PRNG reseed, env restoration, and process group isolation."""
    sock_path = str(tmp_path / "direct_fs.sock")
    out_dir = tmp_path / "children"
    out_dir.mkdir()

    driver_code = (
        "import gc, json, os, random, signal, sys\n"
        "from ray._private.workers.fork_server import _run_fork_server\n"
        f"mod = _run_fork_server(['--socket-path={sock_path}', '--preload-modules=json,numpy'])\n"
        "if mod is not None:\n"
        "    import numpy as np\n"
        "    info = {\n"
        "        'pid': os.getpid(),\n"
        "        'ppid': os.getppid(),\n"
        "        'pgid': os.getpgid(0),\n"
        "        'freeze_count': gc.get_freeze_count(),\n"
        "        'argv': sys.argv[1:],\n"
        "        'omp_num_threads': os.environ.get('OMP_NUM_THREADS'),\n"
        "        'je_arrow': os.environ.get('JE_ARROW_MALLOC_CONF'),\n"
        "        'ray_job_id': os.environ.get('RAY_JOB_ID'),\n"
        "        'expanded_var': os.environ.get('EXPANDED_VAR'),\n"
        "        'rand': random.random(),\n"
        "        'np_rand': float(np.random.rand()),\n"
        "        'sigint_is_default': signal.getsignal(signal.SIGINT) is signal.default_int_handler,\n"
        "    }\n"
        "    with open(os.environ['CHILD_OUT_FILE'], 'w') as f:\n"
        "        json.dump(info, f)\n"
        "    os._exit(0)\n"
    )

    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = "6"
    env.pop("JE_ARROW_MALLOC_CONF", None)
    env["RAY_JOB_ID"] = "parent_job_999"
    env["HOST_BASE"] = "/var/ray_test"

    proc = subprocess.Popen(
        [sys.executable, "-c", driver_code],
        env=env,
        start_new_session=True,
        close_fds=True,
    )
    try:
        wait_for_condition(lambda: os.path.exists(sock_path), timeout=15)
        server_pid = proc.pid
        server_pgid = os.getpgid(server_pid)
        assert len(os.listdir(f"/proc/{server_pid}/task")) == 1

        # Verify invalid JSON and malformed payload types do not crash the fork server.
        for bad_payload in (b"not-valid-json\n", b'{"args": null, "env": null}\n'):
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as bad_sock:
                bad_sock.connect(sock_path)
                bad_sock.sendall(bad_payload)

        children_info = []
        for idx in range(2):
            out_file = str(out_dir / f"child_{idx}.json")
            req = {
                "args": [f"--worker-idx={idx}"],
                "env": {
                    "CHILD_OUT_FILE": out_file,
                    "EXPANDED_VAR": "${HOST_BASE}/worker_" + str(idx),
                },
            }
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.settimeout(5.0)
                s.connect(sock_path)
                s.sendall(json.dumps(req).encode("utf-8") + b"\n")
                resp = json.loads(s.makefile("rb").readline())

            child_pid = resp["pid"]
            assert child_pid > 0
            wait_for_condition(lambda: os.path.exists(out_file), timeout=5)
            info = json.loads(Path(out_file).read_text())
            assert info["pid"] == child_pid
            children_info.append(info)

        assert len({c["pid"] for c in children_info}) == 2
        assert children_info[0]["rand"] != children_info[1]["rand"]
        assert children_info[0]["np_rand"] != children_info[1]["np_rand"]

        for idx, info in enumerate(children_info):
            assert info["ppid"] == server_pid
            assert info["pgid"] == info["pid"]
            assert info["pgid"] != server_pgid
            assert info["freeze_count"] > 0
            assert info["argv"] == [f"--worker-idx={idx}"]
            assert info["omp_num_threads"] == "6"
            assert info["je_arrow"] is None
            assert info["ray_job_id"] is None
            assert info["expanded_var"] == f"/var/ray_test/worker_{idx}"
            assert info["sigint_is_default"] is True
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
