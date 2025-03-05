import ray
import pytest
import multiprocessing
import subprocess
import time
import psutil
import logging
import os
import sys

logger = logging.getLogger(__name__)


@pytest.fixture
def enable_subreaper():
    os.environ["RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper"] = "true"
    yield
    del os.environ["RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper"]


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
            time.sleep(1)  # wait for the process to exit.

            process.wait()
            # after reaping, it's gone.
            with pytest.raises(psutil.NoSuchProcess):
                psutil.Process(pid)

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
