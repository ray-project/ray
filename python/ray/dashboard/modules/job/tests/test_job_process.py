import asyncio
import ctypes
import json
import os
import secrets
import select
import shlex
import signal
import subprocess
import sys
import textwrap
import time
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

import psutil

os.environ["RAY_AUTH_MODE"] = "token"
os.environ.setdefault("RAY_AUTH_TOKEN", secrets.token_hex(32))

from ray.dashboard.modules.job.job_process import JobProcess

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux Jobs cleanup")


def wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("Timed out waiting for process state")


def running(pid):
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


@contextmanager
def job_subreaper():
    # Adopt orphaned job descendants, but only clean registered job groups.
    # Other fixtures and plugins may start children during the test as well.
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    assert libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0
    assert libc.prctl(36, 1, 0, 0, 0) == 0
    groups = set()

    def job_children():
        children = []
        for child in psutil.Process().children():
            try:
                if os.getpgid(child.pid) in groups:
                    children.append(child)
            except ProcessLookupError:
                pass
        return children

    try:
        yield groups
    finally:
        try:
            for _ in range(100):
                adopted = job_children()
                if not adopted:
                    break
                for child in adopted:
                    try:
                        child.kill()
                        os.waitpid(child.pid, os.WNOHANG)
                    except (psutil.NoSuchProcess, ChildProcessError):
                        pass
                time.sleep(0.02)
            assert not job_children()
        finally:
            assert libc.prctl(36, previous.value, 0, 0, 0) == 0


@pytest.fixture(autouse=True)
def reap_children():
    with job_subreaper() as groups:
        yield groups


def test_reaper_preserves_unrelated_child():
    unrelated = None
    try:
        with job_subreaper() as groups:
            owned = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
            )
            groups.add(owned.pid)
            unrelated = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
            )
        assert not psutil.pid_exists(owned.pid)
        assert unrelated.poll() is None
        owned.wait(timeout=5)
    finally:
        if unrelated is not None:
            unrelated.kill()
            unrelated.wait(timeout=5)


def command(code):
    return "exec " + shlex.join([sys.executable, "-c", textwrap.dedent(code)])


@pytest.fixture
def launch(tmp_path, reap_children):
    processes = []

    def start(code, env=None):
        groups = []

        def register(pid):
            groups.append(pid)
            reap_children.add(pid)

        def cleanup():
            if groups:
                os.killpg(groups.pop(), signal.SIGKILL)

        with (tmp_path / "job.log").open("a") as log:
            process = JobProcess(
                command(code), env or os.environ.copy(), log, register, cleanup
            )
        processes.append(process)
        return process

    yield start
    for process in processes:
        process.close()


def test_driver_pid_reserved_until_cleanup(launch):
    process = launch("pass")
    poller = select.poll()
    poller.register(process._helper.stdout, select.POLLIN)
    assert poller.poll(5000)
    driver = psutil.Process(process.pid)
    assert driver.status() == psutil.STATUS_ZOMBIE
    assert driver.ppid() == process._helper.pid
    wait_for(lambda: process.poll() is not None)
    assert process.returncode == 0


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
@pytest.mark.parametrize("handle_signal", [False, True])
def test_group_stop_signal(launch, tmp_path, sig, handle_signal):
    ready = tmp_path / "ready"
    process = launch(
        f"""
        import signal
        import sys
        import time
        from pathlib import Path

        def stop(*args):
            time.sleep(0.2)
            sys.exit(7)

        if {handle_signal!r}:
            signal.signal({int(sig)}, stop)
        Path({str(ready)!r}).touch()
        time.sleep(60)
        """,
        env=dict(os.environ, RAY_JOB_STOP_SIGNAL=sig.name),
    )
    wait_for(ready.exists)
    os.killpg(process._helper.pid, sig)
    wait_for(lambda: process.poll() is not None)
    assert process.returncode == (7 if handle_signal else -sig)


def test_high_file_descriptors(launch):
    import resource

    if resource.getrlimit(resource.RLIMIT_NOFILE)[0] < 1100:
        pytest.skip("Requires at least 1100 file descriptors")
    descriptors = []
    try:
        while not descriptors or descriptors[-1] < 1024:
            descriptors.append(os.open(os.devnull, os.O_RDONLY))
        process = launch("pass")
        assert process._helper.stdout.fileno() > 1024
        wait_for(lambda: process.poll() is not None)
        assert process.returncode == 0
    finally:
        for fd in descriptors:
            os.close(fd)


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_before_stop", [False, True])
async def test_supervisor_stop(launch, tmp_path, monkeypatch, exit_before_stop):
    import ray.dashboard.modules.job.job_supervisor as module
    from ray.job_submission import JobStatus

    process = launch("pass" if exit_before_stop else "import time; time.sleep(60)")
    if exit_before_stop:
        # Report the exit before run() schedules its polling task so both the
        # polling task and stop event are ready when asyncio.wait() returns.
        poller = select.poll()
        poller.register(process._helper.stdout, select.POLLIN)
        assert poller.poll(5000)
    supervisor = object.__new__(module.JobSupervisor)
    supervisor._job_id = "stop-after-exit"
    supervisor._entrypoint = "unused"
    supervisor._job_process = process
    supervisor._win32_job_object = None
    supervisor._stop_event = asyncio.Event()
    supervisor._stop_event.set()
    supervisor._logger = Mock()
    supervisor._job_info_client = Mock(
        get_info=AsyncMock(return_value=Mock(status=JobStatus.PENDING, message=None)),
        put_status=AsyncMock(),
    )
    supervisor._log_client = Mock()
    supervisor._log_client.get_log_file_path.return_value = str(tmp_path / "job.log")
    supervisor._exec_entrypoint = Mock(return_value=process)
    supervisor._get_driver_env_vars = Mock(return_value={"RAY_ADDRESS": "local"})
    monkeypatch.setattr(module.ray, "get_runtime_context", Mock())
    monkeypatch.setattr(
        module.ray._private.worker.global_worker,
        "node",
        Mock(node_ip_address="127.0.0.1", dashboard_agent_listen_port=1234),
    )
    monkeypatch.setattr(module.ray.actor, "exit_actor", Mock())
    monkeypatch.setattr(module, "submission_job_events_enabled", lambda: False)
    await asyncio.wait_for(supervisor.run(), timeout=5)
    assert supervisor._job_info_client.put_status.call_args.args == (
        supervisor._job_id,
        JobStatus.STOPPED,
    )
    supervisor._logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_graceful_stop_waits_only_for_live_processes(launch):
    from ray.dashboard.modules.job.job_supervisor import JobSupervisor

    process = launch("pass")
    wait_for(lambda: not running(process.pid))
    driver = psutil.Process(process.pid)
    live_process = launch("import time; time.sleep(60)")
    supervisor = object.__new__(JobSupervisor)
    task = asyncio.create_task(
        supervisor._poll_all([driver, psutil.Process(live_process.pid)])
    )
    try:
        await asyncio.sleep(0.15)
        assert not task.done()
        os.kill(live_process.pid, signal.SIGTERM)
        await asyncio.wait_for(task, timeout=2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("exit_code", [0, 7])
def test_driver_status_and_remaining_child(launch, tmp_path, exit_code):
    child_file = tmp_path / "child"
    process = launch(
        f"""
        import subprocess
        import sys
        from pathlib import Path

        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
        Path({str(child_file)!r}).write_text(str(child.pid))
        print('driver-output', flush=True)
        sys.exit({exit_code})
    """
    )
    wait_for(lambda: process.poll() is not None)
    assert process.returncode == exit_code
    wait_for(lambda: not running(int(child_file.read_text())))
    assert "driver-output" in (tmp_path / "job.log").read_text()
    process.close()
    assert process.poll() == exit_code


def test_registration_failure_prevents_entrypoint(tmp_path):
    marker = tmp_path / "started"
    groups = []

    def reject(pid):
        groups.append(pid)
        raise RuntimeError("worker is shutting down")

    with (tmp_path / "job.log").open("w") as log:
        with pytest.raises(RuntimeError, match="worker is shutting down"):
            JobProcess(
                command(f"from pathlib import Path; Path({str(marker)!r}).touch()"),
                os.environ.copy(),
                log,
                reject,
                lambda: pytest.fail("Registration failed"),
            )
    assert not marker.exists()
    assert not psutil.pid_exists(groups[0])


def test_watchdog_failure_cleans_driver(launch):
    process = launch("import time; time.sleep(60)")
    os.kill(process._helper.pid, signal.SIGKILL)
    with pytest.raises(RuntimeError, match="watchdog exited"):
        wait_for(lambda: process.poll() is not None)
    wait_for(lambda: not running(process.pid))


def test_watchdog_imports_are_isolated(launch, tmp_path, monkeypatch):
    (tmp_path / "signal.py").write_text("raise RuntimeError('shadowed signal')")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    process = launch("print('driver-output')")
    wait_for(lambda: process.poll() is not None)
    assert process.returncode == 0


def test_driver_environment_and_stop_signal(launch, tmp_path):
    ready = tmp_path / "ready"
    env = dict(os.environ, RAY_JOB_STOP_SIGNAL="SIGINT", JOB_TEST_VALUE="value")
    process = launch(
        f"""
        import os
        import signal
        import sys
        import time
        from pathlib import Path

        assert os.environ['JOB_TEST_VALUE'] == 'value'
        signal.signal(signal.SIGINT, lambda *args: sys.exit(3))
        Path({str(ready)!r}).touch()
        time.sleep(60)
        """,
        env=env,
    )
    wait_for(ready.exists)
    os.kill(process.pid, signal.SIGINT)
    wait_for(lambda: process.poll() is not None)
    assert process.returncode == 3


@pytest.mark.parametrize("during_registration", [False, True])
def test_supervisor_process_loss(tmp_path, during_registration, reap_children):
    # The supervisor runs in a separate interpreter so its death closes the
    # lifetime pipe without calling JobProcess.close().
    import ray.dashboard.modules.job.job_process as module

    ready = tmp_path / "ready"
    marker = tmp_path / "driver-started"
    driver_code = f"""
        from pathlib import Path
        import time
        Path({str(marker)!r}).touch()
        time.sleep(60)
    """
    supervisor_code = f"""
import json, os, runpy, signal, time
from pathlib import Path
JobProcess = runpy.run_path({module.__file__!r})['JobProcess']
groups = []
def register(pid):
    groups.append(pid)
    if {during_registration!r}:
        Path({str(ready)!r}).write_text(json.dumps([None, pid]))
        time.sleep(60)
with open({str(tmp_path / 'job.log')!r}, 'w') as log:
    process = JobProcess(
        {command(driver_code)!r}, os.environ.copy(), log,
        register, lambda: os.killpg(groups[0], signal.SIGKILL))
Path({str(ready)!r}).write_text(json.dumps([process.pid, groups[0]]))
time.sleep(60)
"""
    supervisor = subprocess.Popen([sys.executable, "-I", "-c", supervisor_code])
    try:
        wait_for(ready.exists)
        driver, helper = json.loads(ready.read_text())
        reap_children.add(helper)
        if driver is not None:
            wait_for(marker.exists)
        supervisor.kill()
        supervisor.wait(timeout=5)
        wait_for(lambda: not running(helper))
        if driver is not None:
            wait_for(lambda: not running(driver))
        else:
            assert not marker.exists()
    finally:
        if supervisor.poll() is None:
            supervisor.kill()
        supervisor.wait(timeout=5)


@pytest.mark.parametrize("close_before_register", [False, True])
def test_native_group_registration(close_before_register):
    import ray

    ray.init(address="local", num_cpus=0, include_dashboard=False)
    try:
        core_worker = ray._private.worker.global_worker.core_worker
        assert core_worker.supports_job_process_group_cleanup()
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            start_new_session=True,
        )
        try:
            if close_before_register:
                core_worker.cleanup_job_process_group()
                with pytest.raises(Exception, match="cleanup already started"):
                    core_worker.register_job_process_group(child.pid)
                assert child.poll() is None
            else:
                with pytest.raises(Exception, match="own process group"):
                    core_worker.register_job_process_group(os.getpid())
                core_worker.register_job_process_group(child.pid)
                core_worker.cleanup_job_process_group()
                assert child.wait(timeout=5) == -signal.SIGKILL
                core_worker.cleanup_job_process_group()
                with pytest.raises(Exception, match="cleanup already started"):
                    core_worker.register_job_process_group(child.pid)
        finally:
            # A registered identity has been retired before any wait/poll here.
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
    finally:
        ray.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ray.kill", "os.kill", "stop"])
@pytest.mark.parametrize("legacy_cleanup", [False, True])
async def test_job_supervisor_cleans_descendants(
    tmp_path, mode, legacy_cleanup, reap_children
):
    # This test exercises the compiled CoreWorker bindings and shutdown path.
    import ray
    from ray._common.test_utils import async_wait_for_condition
    from ray.dashboard.modules.job.job_manager import JobManager
    from ray.job_submission import JobErrorType, JobStatus

    pid_file = tmp_path / "pids.json"
    driver_file = tmp_path / "driver.py"
    driver_file.write_text(
        "import os, subprocess, sys, time\n"
        "from pathlib import Path\n"
        "role = int(sys.argv[1])\n"
        "if role < 2:\n"
        "    subprocess.Popen([sys.executable, __file__, str(role + 1)])\n"
        f"Path({str(pid_file)!r} + str(role)).write_text(str(os.getpid()))\n"
        "time.sleep(60)\n"
    )
    ray.init(
        address="local",
        num_cpus=1,
        include_dashboard=False,
        _system_config={
            "kill_child_processes_on_worker_exit": legacy_cleanup,
            "process_group_cleanup_enabled": True,
            "kill_child_processes_on_worker_exit_with_raylet_subreaper": False,
        },
    )
    try:
        manager = JobManager(
            ray._private.worker.global_worker.gcs_client, str(tmp_path)
        )
        manager.JOB_MONITOR_LOOP_PERIOD_S = 0.01
        job_id = await manager.submit_job(
            entrypoint="exec " + shlex.join([sys.executable, str(driver_file), "0"])
        )
        files = [Path(str(pid_file) + str(role)) for role in range(3)]
        await async_wait_for_condition(lambda: all(path.exists() for path in files))
        pids = [int(path.read_text()) for path in files]
        helper = psutil.Process(pids[0]).ppid()
        reap_children.add(helper)
        supervisor_pid = psutil.Process(helper).ppid()
        assert os.getpgid(helper) == helper
        assert all(os.getpgid(pid) == helper for pid in pids)
        if mode == "ray.kill":
            ray.kill(manager._get_actor_for_job(job_id), no_restart=True)
        elif mode == "os.kill":
            os.kill(supervisor_pid, signal.SIGKILL)
        else:
            assert manager.stop_job(job_id)

        async def finished():
            info = await manager.get_job_info(job_id)
            expected = JobStatus.STOPPED if mode == "stop" else JobStatus.FAILED
            if info.status != expected:
                return False
            if mode != "stop":
                assert info.error_type == JobErrorType.JOB_SUPERVISOR_ACTOR_DIED
            return all(not running(pid) for pid in [*pids, helper])

        await async_wait_for_condition(finished)
    finally:
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
