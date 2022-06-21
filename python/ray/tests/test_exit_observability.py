import os
import signal
import sys

import pytest

import ray
from ray._private.test_utils import run_string_as_driver, wait_for_condition
from ray.experimental.state.api import list_workers
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def get_worker_by_pid(pid):
    for w in list_workers():
        if w["pid"] == pid:
            return w
    assert False


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_worker_exit_system_error(ray_start_cluster):
    """
    SYSTEM_ERROR
    - (tested) Failure from the connection E.g., core worker dead.
    - (tested) Unexpected exception or exit with exit_code !=0 on core worker.
    - (tested for owner node death) Node died. Currently worker failure detection
        upon node death is not detected by Ray. TODO(sang): Fix it.
    - (Cannot test) Direct call failure.
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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_worker_exit_intended_user_exit(ray_start_cluster):
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

    @ray.remote
    class A:
        def pid(self):
            return os.getpid()

        def exit(self):
            ray.actor.exit_actor()

        def exit_with_exit_code(self):
            sys.exit(0)

    a = A.remote()
    pid = ray.get(a.pid.remote())
    with pytest.raises(ray.exceptions.RayActorError, match="exit_actor"):
        ray.get(a.exit.remote())

    def verify_worker_exit_actor():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        return type == "INTENDED_USER_EXIT" and "exit_actor" in detail

    wait_for_condition(verify_worker_exit_actor)

    a = A.remote()
    pid = ray.get(a.pid.remote())
    with pytest.raises(ray.exceptions.RayActorError, match="exit code 0"):
        ray.get(a.exit_with_exit_code.remote())

    def verify_exit_code_0():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        return type == "INTENDED_USER_EXIT" and "exit code 0" in detail

    wait_for_condition(verify_exit_code_0)

    a = A.remote()
    pid = ray.get(a.pid.remote())
    ray.kill(a)
    with pytest.raises(ray.exceptions.RayActorError, match="ray.kill"):
        ray.get(a.exit_with_exit_code.remote())

    def verify_exit_by_ray_kill():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        return type == "INTENDED_SYSTEM_EXIT" and "ray.kill" in detail

    wait_for_condition(verify_exit_by_ray_kill)

    @ray.remote
    class PidDB:
        def __init__(self):
            self.pid = None

        def record_pid(self, pid):
            self.pid = pid

        def get_pid(self):
            return self.pid

    p = PidDB.remote()

    @ray.remote
    def f():
        ray.get(p.record_pid.remote(os.getpid()))
        import time

        time.sleep(300)

    t = f.remote()
    wait_for_condition(lambda: ray.get(p.get_pid.remote()) is not None, timeout=300)
    ray.cancel(t, force=True)
    pid = ray.get(p.get_pid.remote())

    def verify_exit_by_ray_cancel():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        return type == "INTENDED_USER_EXIT" and "ray.cancel" in detail

    wait_for_condition(verify_exit_by_ray_cancel)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_worker_exit_intended_system_exit_and_user_error(ray_start_cluster):
    """
    INTENDED_SYSTEM_EXIT
    - (not tested, hard to test) Unused resource removed
    - (tested) Pg removed
    - (tested) Idle
    USER_ERROR
    - (tested) Actor init failed
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        return ray.get(g.remote())

    @ray.remote
    def g():
        return os.getpid()

    # Start a task that has a blocking call ray.get with g.remote.
    # g.remote will borrow the CPU and start a new worker.
    # The worker started for g.remote will exit by IDLE timeout.
    pid = ray.get(f.remote())

    def verify_exit_by_idle_timeout():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        return type == "INTENDED_SYSTEM_EXIT" and "it was idle" in detail

    wait_for_condition(verify_exit_by_idle_timeout)

    @ray.remote
    class A:
        def getpid(self):
            return os.getpid()

    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    a = A.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote()
    pid = ray.get(a.getpid.remote())
    ray.util.remove_placement_group(pg)

    def verify_exit_by_pg_removed():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        return (
            type == "INTENDED_SYSTEM_EXIT" and "placement group was removed" in detail
        )

    wait_for_condition(verify_exit_by_pg_removed)

    @ray.remote
    class PidDB:
        def __init__(self):
            self.pid = None

        def record_pid(self, pid):
            self.pid = pid

        def get_pid(self):
            return self.pid

    p = PidDB.remote()

    @ray.remote
    class FaultyActor:
        def __init__(self):
            p.record_pid.remote(os.getpid())
            raise Exception

        def ready(self):
            pass

    a = FaultyActor.remote()
    wait_for_condition(lambda: ray.get(p.get_pid.remote()) is not None)
    pid = ray.get(p.get_pid.remote())

    def verify_exit_by_actor_init_failure():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        print(type, detail)
        return (
            type == "USER_ERROR" and "exception in the initialization method" in detail
        )

    wait_for_condition(verify_exit_by_actor_init_failure)


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
