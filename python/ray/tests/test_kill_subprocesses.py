import logging
import os
import subprocess
import sys
import time

import pytest

import ray
from ray._common.test_utils import wait_for_condition

import psutil

logger = logging.getLogger(__name__)


@pytest.fixture
def enable_subreaper():
    os.environ["RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper"] = "true"
    yield
    del os.environ["RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper"]


@pytest.fixture
def enable_pg_cleanup():
    os.environ["RAY_process_group_cleanup_enabled"] = "true"
    yield
    del os.environ["RAY_process_group_cleanup_enabled"]


def sleep_forever():
    while True:
        time.sleep(10000)


def get_process_info(pid):
    # may raise psutil.NoSuchProcess
    process = psutil.Process(pid)
    return {
        "PID": process.pid,
        "Name": process.name(),
        "Status": process.status(),
        "CPU Times": process.cpu_times(),
        "Memory Info": process.memory_info(),
    }


@ray.remote
class BedMaker:
    def make_sleeper(self):
        p = subprocess.Popen(["sleep", "1000"])  # inherits PGID
        return p.pid

    def spawn_daemon(self):
        # Spawns a bash script (shell=True) that starts a daemon process
        # which sleeps 1000s. The bash exits immediately, leaving the daemon
        # running in the background. We don't want to kill the daemon when
        # the actor is alive; we want to kill it after the actor is killed.
        command = "nohup sleep 1000 >/dev/null 2>&1 & echo $!"
        output = subprocess.check_output(command, shell=True, text=True)
        return int(output.strip())

    def my_pid(self):
        return os.getpid()


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Orphan process killing only works on Linux.",
)
def test_ray_kill_can_kill_subprocess(shutdown_only):
    """
    This works becuase of kill_child_processes_on_worker_exit.
    Even if kill_child_processes_on_worker_exit_with_raylet_subreaper
    is not set, the worker will still kill its subprocesses.
    """
    ray.init()
    b = BedMaker.remote()
    pid = ray.get(b.make_sleeper.remote())

    # ray.kill can kill subprocesses.
    logger.info(get_process_info(pid))  # shows the process
    ray.kill(b)
    time.sleep(1)
    with pytest.raises(psutil.NoSuchProcess):
        logger.info(get_process_info(pid))  # subprocess killed


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Orphan process killing only works on Linux.",
)
def test_sigkilled_worker_can_kill_subprocess(enable_subreaper, shutdown_only):
    ray.init()
    # sigkill'd actor can't kill subprocesses
    b = BedMaker.remote()
    pid = ray.get(b.make_sleeper.remote())
    actor_pid = ray.get(b.my_pid.remote())

    logger.info(get_process_info(pid))  # shows the process
    psutil.Process(actor_pid).kill()  # sigkill
    time.sleep(11)  # unowned processes are killed every 10s.
    with pytest.raises(psutil.NoSuchProcess):
        logger.info(get_process_info(pid))  # subprocess killed


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Orphan process killing only works on Linux.",
)
def test_daemon_processes_not_killed_until_actor_dead(enable_subreaper, shutdown_only):
    ray.init()
    # sigkill'd actor can't kill subprocesses
    b = BedMaker.remote()
    daemon_pid = ray.get(b.spawn_daemon.remote())
    actor_pid = ray.get(b.my_pid.remote())

    # The pid refers to a daemon process that should not be killed, although
    # it's already reparented to the core worker.
    time.sleep(11)  # even after a cycle of killing...
    assert psutil.Process(daemon_pid).ppid() == actor_pid

    psutil.Process(actor_pid).kill()  # sigkill
    time.sleep(11)  # unowned processes are killed every 10s.
    with pytest.raises(psutil.NoSuchProcess):
        logger.info(get_process_info(daemon_pid))  # subprocess killed


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Orphan process killing only works on Linux.",
)
def test_default_sigchld_handler(enable_subreaper, shutdown_only):
    """
    Core worker auto-reaps zombies via SIG_IGN. If the user wants to wait for subprocess
    they can add it back.
    """
    ray.init()

    @ray.remote
    class A:
        def auto_reap(self):
            """
            Auto subprocess management. Since the signal handler is set to SIG_IGN
            by the flag, zombies are reaped automatically.
            """
            process = subprocess.Popen(["true"])
            pid = process.pid
            wait_for_condition(
                lambda: not psutil.pid_exists(pid), retry_interval_ms=100
            )

        def manual_reap(self):
            """
            Manual subprocess management. Since the signal handler is set back to
            default, user needs to call `process.wait()` on their own, or the zombie
            process would persist.
            """

            import signal

            signal.signal(signal.SIGCHLD, signal.SIG_DFL)

            process = subprocess.Popen(["true"])
            pid = process.pid
            time.sleep(1)  # wait for the process to exit.

            assert psutil.Process(pid).status() == psutil.STATUS_ZOMBIE

            process.wait()
            # after reaping, it's gone.
            with pytest.raises(psutil.NoSuchProcess):
                psutil.Process(pid)

    a = A.remote()
    # order matters, since `manual_reap` sets the signal handler.
    ray.get(a.auto_reap.remote())
    ray.get(a.manual_reap.remote())


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Orphan process killing only works on Linux.",
)
def test_sigkilled_worker_child_process_cleaned_up(enable_pg_cleanup, shutdown_only):
    ray.init()
    # SIGKILL the actor; PG cleanup should terminate the background child.
    b = BedMaker.remote()
    child_pid = ray.get(b.make_sleeper.remote())
    actor_pid = ray.get(b.my_pid.remote())

    logger.info(get_process_info(child_pid))  # shows the process
    psutil.Process(actor_pid).kill()  # sigkill
    wait_for_condition(lambda: not psutil.pid_exists(child_pid), retry_interval_ms=100)
    with pytest.raises(psutil.NoSuchProcess):
        logger.info(get_process_info(child_pid))


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Orphan process killing only works on Linux.",
)
def test_background_child_survives_while_actor_alive_then_killed_with_pg_cleanup(
    enable_pg_cleanup, shutdown_only
):
    ray.init()
    # Spawn a background child that remains in the same PG as the actor.
    b = BedMaker.remote()
    child_pid = ray.get(b.make_sleeper.remote())
    actor_pid = ray.get(b.my_pid.remote())

    # The background child remains alive while the actor is alive.
    time.sleep(1)
    assert psutil.pid_exists(child_pid)

    # After the actor is killed, PG cleanup should terminate the background child.
    psutil.Process(actor_pid).kill()
    wait_for_condition(lambda: not psutil.pid_exists(child_pid), retry_interval_ms=100)
    with pytest.raises(psutil.NoSuchProcess):
        logger.info(get_process_info(child_pid))


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Orphan process killing only works on Linux.",
)
def test_detached_setsido_escape_with_pg_cleanup(enable_pg_cleanup, shutdown_only):
    ray.init()

    @ray.remote
    class A:
        def spawn_detached(self):
            # Detach into a new session (escape worker PG); sleep long.
            return subprocess.Popen(
                [sys.executable, "-c", "import os,time; os.setsid(); time.sleep(1000)"]
            ).pid

        def pid(self):
            return os.getpid()

    a = A.remote()
    child_pid = ray.get(a.spawn_detached.remote())
    actor_pid = ray.get(a.pid.remote())
    psutil.Process(actor_pid).kill()
    time.sleep(1)
    # Detached child should still be alive (escaped PG cleanup).
    assert psutil.pid_exists(child_pid)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
