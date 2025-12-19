import asyncio
import os
import signal
import sys

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.state_api_test_utils import verify_failed_task
from ray._private.test_utils import run_string_as_driver
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.state import list_nodes, list_tasks, list_workers


def get_worker_by_pid(pid, detail=True):
    for w in list_workers(detail=detail):
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
        ray.get(a.exit.options(name="exit").remote(4))

    def verify_exit_failure():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        # If the worker is killed by SIGKILL, it is highly likely by OOM, so
        # the error message should contain information.
        assert type == "SYSTEM_ERROR" and "exit code 4" in detail

        # Verify the task failed with the info.
        return verify_failed_task(
            name="exit", error_type="ACTOR_DIED", error_message="exit code 4"
        )

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
    driver_pid = int(a.strip().split("\n")[-1].strip())

    def verify_worker_exit_by_shutdown():
        worker = get_worker_by_pid(driver_pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert type == "INTENDED_USER_EXIT" and "ray.shutdown()" in detail
        return True

    wait_for_condition(verify_worker_exit_by_shutdown)

    @ray.remote
    class A:
        def pid(self):
            return os.getpid()

        def exit(self):
            ray.actor.exit_actor()

        def exit_with_exit_code(self):
            sys.exit(0)

        def sleep_forever(self):
            import time

            # RIP
            time.sleep(999999)

    a = A.remote()
    pid = ray.get(a.pid.remote())
    with pytest.raises(ray.exceptions.RayActorError, match="exit_actor"):
        ray.get(a.exit.options(name="exit").remote())

    def verify_worker_exit_actor():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert type == "INTENDED_USER_EXIT" and "exit_actor" in detail

        t = list_tasks(filters=[("name", "=", "exit")])[0]
        # exit_actor should be shown as non-task failure.
        assert t["state"] == "FINISHED"
        return True

    wait_for_condition(verify_worker_exit_actor)

    a = A.remote()
    pid = ray.get(a.pid.remote())
    with pytest.raises(ray.exceptions.RayActorError, match="exit code 0"):
        ray.get(a.exit_with_exit_code.options(name="exit_with_exit_code").remote())

    def verify_exit_code_0():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert type == "INTENDED_USER_EXIT" and "exit code 0" in detail
        t = list_tasks(filters=[("name", "=", "exit_with_exit_code")])[0]
        # exit_actor should be shown as non-task failure.
        assert t["state"] == "FINISHED"
        return True

    wait_for_condition(verify_exit_code_0)

    a = A.remote()
    pid = ray.get(a.pid.remote())
    ray.kill(a)
    with pytest.raises(ray.exceptions.RayActorError, match="ray.kill"):
        ray.get(a.sleep_forever.options(name="sleep_forever").remote())

    def verify_exit_by_ray_kill():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert type == "INTENDED_SYSTEM_EXIT" and "ray.kill" in detail
        return verify_failed_task(
            name="sleep_forever",
            error_type="ACTOR_DIED",
            error_message="ray.kill",
        )

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

    t = f.options(name="cancel-f").remote()
    wait_for_condition(lambda: ray.get(p.get_pid.remote()) is not None, timeout=300)
    ray.cancel(t, force=True)
    pid = ray.get(p.get_pid.remote())

    def verify_exit_by_ray_cancel():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert type == "INTENDED_USER_EXIT" and "ray.cancel" in detail
        return verify_failed_task(
            name="cancel-f",
            error_type="TASK_CANCELLED",
        )

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

    @ray.remote(num_cpus=1)
    class A:
        def __init__(self):
            self.sleeping = False

        async def getpid(self):
            while not self.sleeping:
                await asyncio.sleep(0.1)
            return os.getpid()

        async def sleep(self):
            self.sleeping = True
            await asyncio.sleep(9999)

    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    a = A.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote()
    a.sleep.options(name="sleep").remote()
    pid = ray.get(a.getpid.remote())
    ray.util.remove_placement_group(pg)

    def verify_exit_by_pg_removed():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert verify_failed_task(
            name="sleep",
            error_type="ACTOR_DIED",
            error_message=["INTENDED_SYSTEM_EXIT", "placement group was removed"],
        )
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
            raise Exception("exception in the initialization method")

        def ready(self):
            pass

    a = FaultyActor.remote()
    wait_for_condition(lambda: ray.get(p.get_pid.remote()) is not None)
    pid = ray.get(p.get_pid.remote())

    def verify_exit_by_actor_init_failure():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert (
            type == "USER_ERROR" and "exception in the initialization method" in detail
        )
        return verify_failed_task(
            name="FaultyActor.__init__",
            error_type="TASK_EXECUTION_EXCEPTION",
            error_message="exception in the initialization method",
        )

    wait_for_condition(verify_exit_by_actor_init_failure)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows because sigkill doesn't work on Windows",
)
def test_worker_start_end_time(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Worker:
        def ready(self):
            return os.getpid()

    # Test normal exit.
    worker = Worker.remote()
    pid = ray.get(worker.ready.remote())

    def verify():
        workers = list_workers(detail=True, filters=[("pid", "=", pid)])[0]
        print(workers)
        assert workers["start_time_ms"] > 0
        assert workers["end_time_ms"] == 0
        return True

    wait_for_condition(verify)

    ray.kill(worker)

    def verify():
        workers = list_workers(detail=True, filters=[("pid", "=", pid)])[0]
        assert workers["start_time_ms"] > 0
        assert workers["end_time_ms"] > 0
        return True

    wait_for_condition(verify)

    # Test unexpected exit.
    worker = Worker.remote()
    pid = ray.get(worker.ready.remote())
    os.kill(pid, signal.SIGKILL)

    def verify():
        workers = list_workers(detail=True, filters=[("pid", "=", pid)])[0]
        assert workers["start_time_ms"] > 0
        assert workers["end_time_ms"] > 0
        return True

    wait_for_condition(verify)


def test_node_start_end_time(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=0)
    nodes = list_nodes(detail=True)
    head_node_id = nodes[0]["node_id"]

    worker_node = cluster.add_node(num_cpus=0)
    nodes = list_nodes(detail=True)
    worker_node_data = list(
        filter(lambda x: x["node_id"] != head_node_id and x["state"] == "ALIVE", nodes)
    )[0]
    assert worker_node_data["start_time_ms"] > 0
    assert worker_node_data["end_time_ms"] == 0

    # Test expected exit.
    cluster.remove_node(worker_node, allow_graceful=True)
    nodes = list_nodes(detail=True)
    worker_node_data = list(
        filter(lambda x: x["node_id"] != head_node_id and x["state"] == "DEAD", nodes)
    )[0]
    assert worker_node_data["start_time_ms"] > 0
    assert worker_node_data["end_time_ms"] > 0

    # Test unexpected exit.
    worker_node = cluster.add_node(num_cpus=0)
    nodes = list_nodes(detail=True)
    worker_node_data = list(
        filter(lambda x: x["node_id"] != head_node_id and x["state"] == "ALIVE", nodes)
    )[0]
    assert worker_node_data["start_time_ms"] > 0
    assert worker_node_data["end_time_ms"] == 0

    cluster.remove_node(worker_node, allow_graceful=False)
    nodes = list_nodes(detail=True)
    worker_node_data = list(
        filter(lambda x: x["node_id"] != head_node_id and x["state"] == "DEAD", nodes)
    )[0]
    assert worker_node_data["start_time_ms"] > 0
    assert worker_node_data["end_time_ms"] > 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
