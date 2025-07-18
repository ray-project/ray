import os
import signal
import sys
import time

import numpy as np
import pytest

import ray
from ray._common.test_utils import SignalActor
from ray._private.test_utils import run_string_as_driver_nonblocking

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


# This test checks that when a worker dies in the middle of a get, the raylet will not die.
def test_dying_worker_get(ray_start_2_cpus):
    @ray.remote
    def wait_on_signal(signal_1, signal_2):
        ray.get(signal_1.send.remote())
        ray.get(signal_2.wait.remote())
        return np.ones(200 * 1024, dtype=np.uint8)

    @ray.remote
    def get_worker_pid():
        return os.getpid()

    signal_1 = SignalActor.remote()
    signal_2 = SignalActor.remote()

    x_id = wait_on_signal.remote(signal_1, signal_2)
    ray.get(signal_1.wait.remote())
    # Get the PID of the other worker.
    worker_pid = ray.get(get_worker_pid.remote())

    @ray.remote
    def f(id_in_a_list):
        ray.get(id_in_a_list[0])

    # Have the worker wait in a get call.
    result_id = f.remote([x_id])
    time.sleep(1)

    # Make sure the task hasn't finished.
    ready_ids, _ = ray.wait([result_id], timeout=0)
    assert len(ready_ids) == 0

    # Kill the worker.
    os.kill(worker_pid, SIGKILL)
    time.sleep(0.1)

    # Make sure the sleep task hasn't finished.
    ready_ids, _ = ray.wait([x_id], timeout=0)
    assert len(ready_ids) == 0

    # So that we attempt to notify the worker that the object is available.
    ray.get(signal_2.send.remote())
    ray.get(x_id)
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray._private.services.remaining_processes_alive()


# This test checks that when a driver dies in the middle of a get, the raylet will not die.
def test_dying_driver_get(ray_start_regular):
    # Start the Ray processes.
    address_info = ray_start_regular

    @ray.remote
    def wait_on_signal(signal):
        ray.get(signal.wait.remote())
        return np.ones(200 * 1024, dtype=np.uint8)

    signal = SignalActor.remote()
    x_id = wait_on_signal.remote(signal)

    driver = """
import ray
ray.init("{}")
ray.get(ray.ObjectRef(ray._common.utils.hex_to_binary("{}")))
""".format(
        address_info["address"], x_id.hex()
    )

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    assert p.poll() is None

    # Kill the driver process.
    p.kill()
    p.wait()
    time.sleep(0.1)

    # Make sure the original task hasn't finished.
    ready_ids, _ = ray.wait([x_id], timeout=0)
    assert len(ready_ids) == 0
    # So that we attempt to notify the worker that the object is available.
    ray.get(signal.send.remote())
    ray.get(x_id)
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray._private.services.remaining_processes_alive()


# This test checks that when a worker dies in the middle of a wait, the raylet will not die.
def test_dying_worker_wait(ray_start_2_cpus):
    @ray.remote
    def wait_on_signal(signal):
        ray.get(signal.wait.remote())
        return np.ones(200 * 1024, dtype=np.uint8)

    @ray.remote
    def get_pid():
        return os.getpid()

    signal = SignalActor.remote()
    x_id = wait_on_signal.remote(signal)
    # Get the PID of the worker that block_in_wait will run on (sleep a little
    # to make sure that wait_on_signal has already started).
    time.sleep(0.1)
    worker_pid = ray.get(get_pid.remote())

    @ray.remote
    def block_in_wait(object_ref_in_list):
        ray.wait(object_ref_in_list)

    # Have the worker wait in a wait call.
    block_in_wait.remote([x_id])
    time.sleep(0.1)

    # Kill the worker.
    os.kill(worker_pid, SIGKILL)
    time.sleep(0.1)

    # So that we attempt to notify the worker that the object is available.
    ray.get(signal.send.remote())
    ray.get(x_id)
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray._private.services.remaining_processes_alive()


# This test checks that when a driver dies in the middle of a wait, the raylet will not die.
def test_dying_driver_wait(ray_start_regular):
    # Start the Ray processes.
    address_info = ray_start_regular

    @ray.remote
    def wait_on_signal(signal):
        ray.get(signal.wait.remote())
        return np.ones(200 * 1024, dtype=np.uint8)

    signal = SignalActor.remote()
    x_id = wait_on_signal.remote(signal)

    driver = """
import ray
ray.init("{}")
ray.wait([ray.ObjectRef(ray._common.utils.hex_to_binary("{}"))])
""".format(
        address_info["address"], x_id.hex()
    )

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    assert p.poll() is None

    # Kill the driver process.
    p.kill()
    p.wait()
    time.sleep(0.1)

    # Make sure the original task hasn't finished.
    ready_ids, _ = ray.wait([x_id], timeout=0)
    assert len(ready_ids) == 0
    # So that we attempt to notify the worker that the object is available.
    ray.get(signal.send.remote())
    ray.get(x_id)
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray._private.services.remaining_processes_alive()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
