import os
import sys
import signal

import ray

import pytest

from ray.experimental.state.api import list_workers
from ray._private.test_utils import wait_for_condition, run_string_as_driver


def get_worker_by_pid(pid):
    for w in list_workers().values():
        if w["pid"] == pid:
            return w
    assert False


def test_worker_failure_information_system_error(ray_start_cluster):
    """
    SYSTEM_ERROR
    - (tested) Failure from the connection E.g., core worker dead.
    - (tested) Unexpected exception or exit with exit_code !=0 on core worker.
    - (tested for owner node death) Node died. Currently worker failure detection
        upon node death is not detected by Ray. TODO(sang): Fix it.
    - (Cannot test) Direct call failure.

    INTENDED_USER_EXIT
    - (tested) Shutdown driver
    - (tested) exit_actor
    - (tested) exit(0)
    - (tested) Actor kill request
    - (tested) Task cancel request

    INTENDED_SYSTEM_EXIT
    - (not tested, hard to test) Unused resource removed
    - (tested) Pg removed
    - (tested) Idle
    USER_ERROR
    - (tested) Actor init failed

    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"worker": 1})

    @ray.remote
    class Actor:
        def pid(self):
            import os

            return os.getpid()

        def exit(self, exit_code):
            sys.exit(exit_code)

    """
    Failure from the connection
    """
    a = Actor.remote()
    pid = ray.get(a.pid.remote())
    print(pid)
    os.kill(pid, signal.SIGKILL)

    def verify_connection_failure():
        worker = get_worker_by_pid(pid)
        print(worker)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        # If the worker is killed by SIGKILL, it is highly likely by OOM, so
        # the error message should contain information.
        return type == "SYSTEM_ERROR" and "OOM" in detail

    wait_for_condition(verify_connection_failure)

    """
    Unexpected exception or exit with exit_code !=0 on core worker.
    """
    a = Actor.remote()
    pid = ray.get(a.pid.remote())
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.exit.remote(4))

    def verify_exit_failure():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        # If the worker is killed by SIGKILL, it is highly likely by OOM, so
        # the error message should contain information.
        return type == "SYSTEM_ERROR" and "exit code 4" in detail

    wait_for_condition(verify_exit_failure)


def test_worker_failure_information_intended_user_exit(ray_start_cluster):
    """
    INTENDED_USER_EXIT
    - (tested) Shutdown driver
    - (tested) exit_actor
    - (tested) exit(0)
    - (tested) Actor kill request
    - (tested) Task cancel request
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"worker": 1})

    driver = """
import ray
import os
ray.init(address="{address}")
print(os.getpid())
ray.shutdown()
""".format(
        address=cluster.address
    )
    a = run_string_as_driver(driver)
    driver_pid = int(a.strip("\n"))

    def verify_worker_exit_by_shutdown():
        worker = get_worker_by_pid(driver_pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        return type == "INTENDED_USER_EXIT" and "ray.shutdown()" in detail

    wait_for_condition(verify_worker_exit_by_shutdown)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
