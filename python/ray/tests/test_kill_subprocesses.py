import logging
import multiprocessing
import os
import subprocess
import sys
import time

import pytest

import ray

import psutil

logger = logging.getLogger(__name__)


@pytest.fixture
def pg_cleanup_enabled():
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
        p = multiprocessing.Process(target=sleep_forever)
        p.start()
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


def test_ray_kill_can_kill_subprocess(pg_cleanup_enabled, shutdown_only):
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


def test_sigkilled_worker_can_kill_subprocess(pg_cleanup_enabled, shutdown_only):
    ray.init()
    # sigkill'd actor can't kill subprocesses
    b = BedMaker.remote()
    pid = ray.get(b.make_sleeper.remote())
    actor_pid = ray.get(b.my_pid.remote())

    logger.info(get_process_info(pid))  # shows the process
    psutil.Process(actor_pid).kill()  # sigkill
    time.sleep(3)  # process-group cleanup is immediate; small buffer
    with pytest.raises(psutil.NoSuchProcess):
        logger.info(get_process_info(pid))  # subprocess killed


def test_daemon_processes_not_killed_until_actor_dead(
    pg_cleanup_enabled, shutdown_only
):
    ray.init()
    # sigkill'd actor can't kill subprocesses
    b = BedMaker.remote()
    daemon_pid = ray.get(b.spawn_daemon.remote())
    actor_pid = ray.get(b.my_pid.remote())

    # The pid refers to a daemon process that should not be killed, although
    # it's already reparented to the core worker.
    time.sleep(1)
    # Daemon is still a child of the actor until it exits; verify it's alive.
    assert psutil.pid_exists(daemon_pid)

    psutil.Process(actor_pid).kill()  # sigkill
    time.sleep(3)
    with pytest.raises(psutil.NoSuchProcess):
        logger.info(get_process_info(daemon_pid))  # subprocess killed


@pytest.mark.skipif(
    sys.platform == "win32", reason="setsid/PG semantics are POSIX-only"
)
def test_detached_setsido_escape(pg_cleanup_enabled, shutdown_only):
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
    # Crash the actor.
    psutil.Process(actor_pid).kill()
    time.sleep(3)
    # Detached child should still be alive (escaped PG cleanup).
    assert psutil.pid_exists(child_pid)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
