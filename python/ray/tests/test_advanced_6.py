# coding: utf-8
import logging
import os
import platform
import signal
import sys
import time

import psutil
import pytest

import ray
import ray.cluster_utils
from ray._private.test_utils import (
    run_string_as_driver_nonblocking,
    wait_for_condition,
    wait_for_pid_to_exit,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def save_gpu_ids_shutdown_only():
    # Record the curent value of this environment variable so that we can
    # reset it after the test.
    original_gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", None)

    yield None

    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Reset the environment variable.
    if original_gpu_ids is not None:
        os.environ["CUDA_VISIBLE_DEVICES"] = original_gpu_ids
    else:
        del os.environ["CUDA_VISIBLE_DEVICES"]


@pytest.mark.skipif(platform.system() == "Windows", reason="Hangs on Windows")
def test_specific_gpus(save_gpu_ids_shutdown_only):
    allowed_gpu_ids = [4, 5, 6]
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in allowed_gpu_ids])
    ray.init(num_gpus=3)

    @ray.remote(num_gpus=1)
    def f():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 1
        assert int(gpu_ids[0]) in allowed_gpu_ids

    @ray.remote(num_gpus=2)
    def g():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 2
        assert int(gpu_ids[0]) in allowed_gpu_ids
        assert int(gpu_ids[1]) in allowed_gpu_ids

    ray.get([f.remote() for _ in range(100)])
    ray.get([g.remote() for _ in range(100)])


def test_local_mode_gpus(save_gpu_ids_shutdown_only):
    allowed_gpu_ids = [4, 5, 6, 7, 8]
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in allowed_gpu_ids])

    from importlib import reload

    reload(ray._private.worker)

    ray.init(num_gpus=3, local_mode=True)

    @ray.remote
    def f():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 3
        for gpu in gpu_ids:
            assert int(gpu) in allowed_gpu_ids

    ray.get([f.remote() for _ in range(100)])


def test_blocking_tasks(ray_start_regular):
    @ray.remote
    def f(i, j):
        return (i, j)

    @ray.remote
    def g(i):
        # Each instance of g submits and blocks on the result of another
        # remote task.
        object_refs = [f.remote(i, j) for j in range(2)]
        return ray.get(object_refs)

    @ray.remote
    def h(i):
        # Each instance of g submits and blocks on the result of another
        # remote task using ray.wait.
        object_refs = [f.remote(i, j) for j in range(2)]
        return ray.wait(object_refs, num_returns=len(object_refs))

    ray.get([h.remote(i) for i in range(4)])

    @ray.remote
    def _sleep(i):
        time.sleep(0.01)
        return i

    @ray.remote
    def sleep():
        # Each instance of sleep submits and blocks on the result of
        # another remote task, which takes some time to execute.
        ray.get([_sleep.remote(i) for i in range(10)])

    ray.get(sleep.remote())


def test_max_call_tasks(ray_start_regular):
    @ray.remote(max_calls=1)
    def f():
        return os.getpid()

    pid = ray.get(f.remote())
    wait_for_pid_to_exit(pid)

    @ray.remote(max_calls=2)
    def f():
        return os.getpid()

    pid1 = ray.get(f.remote())
    pid2 = ray.get(f.remote())
    assert pid1 == pid2
    wait_for_pid_to_exit(pid1)


# This case tests that the worker leaked issue when task finished with errors.
# See https://github.com/ray-project/ray/issues/19639.
#
# Case steps are:
#   1. Start a driver which creates a normal task with a long sleeping. This
#      makes the normal task doesn't return.
#   2. Send a SIGTERM to the normal task to trigger an error for it.
#   3. After the normal task being reconstructed, we send a SIGTERM to the
#      driver to make it offline and expects Ray collects the idle workers for
#      the previous nomral task.
def test_whether_worker_leaked_when_task_finished_with_errors(ray_start_regular):

    driver_template = """
import ray
import os
import ray
import numpy as np
import time

ray.init(address="{address}", namespace="test")

# The util actor to store the pid cross jobs.
@ray.remote
class PidStoreActor:
    def __init(self):
        self._pid = None

    def put(self, pid):
        self._pid = pid
        return True

    def get(self):
        return self._pid

def _store_pid_helper():
    try:
        pid_store_actor = ray.get_actor("pid-store", "test")
    except Exception:
        pid_store_actor = PidStoreActor.options(
            name="pid-store", lifetime="detached").remote()
    assert ray.get(pid_store_actor.put.remote(os.getpid()))

@ray.remote
def normal_task(large1, large2):
    # Record the pid of this normal task.
    _store_pid_helper()
    time.sleep(60 * 60)
    return "normaltask"

large = ray.put(np.zeros(100 * 2**10, dtype=np.int8))
obj = normal_task.remote(large, large)
print(ray.get(obj))
"""
    driver_script = driver_template.format(address=ray_start_regular["address"])
    driver_proc = run_string_as_driver_nonblocking(driver_script)
    try:
        driver_proc.wait(10)
    except Exception:
        pass

    def get_normal_task_pid():
        try:
            pid_store_actor = ray.get_actor("pid-store", "test")
            return ray.get(pid_store_actor.get.remote())
        except Exception:
            return None

    wait_for_condition(lambda: get_normal_task_pid() is not None, 10)
    pid_store_actor = ray.get_actor("pid-store", "test")
    normal_task_pid = ray.get(pid_store_actor.get.remote())
    assert normal_task_pid is not None
    normal_task_proc = psutil.Process(normal_task_pid)
    print("killing normal task process, pid =", normal_task_pid)
    normal_task_proc.send_signal(signal.SIGTERM)

    def normal_task_was_reconstructed():
        curr_pid = get_normal_task_pid()
        return curr_pid is not None and curr_pid != normal_task_pid

    wait_for_condition(lambda: normal_task_was_reconstructed(), 10)
    driver_proc.send_signal(signal.SIGTERM)
    # Sleep here to make sure raylet has triggered cleaning up
    # the idle workers.
    wait_for_condition(lambda: not psutil.pid_exists(normal_task_pid), 10)


@pytest.mark.skipif(platform.system() == "Windows", reason="Niceness is posix-only")
def test_worker_niceness(ray_start_regular):
    @ray.remote
    class PIDReporter:
        def get(self):
            return os.getpid()

    reporter = PIDReporter.remote()
    worker_pid = ray.get(reporter.get.remote())
    worker_proc = psutil.Process(worker_pid)
    assert worker_proc.nice() == 15, worker_proc


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
