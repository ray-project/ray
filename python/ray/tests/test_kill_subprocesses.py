import ray
import pytest
import multiprocessing
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

    def my_pid(self):
        return os.getpid()


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Orphan process killing only works on Linux.",
)
def test_ray_kill_can_kill_subprocess():
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
    time.sleep(1)
    with pytest.raises(psutil.NoSuchProcess):
        logger.info(get_process_info(pid))  # subprocess killed


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
