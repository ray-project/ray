import sys
import time
import platform
import os
import signal
import multiprocessing

import pytest
import ray

import psutil  # We must import psutil after ray because we bundle it with ray.

from ray._private.test_utils import (
    wait_for_condition,
    run_string_as_driver_nonblocking,
)
from ray.util.state import get_worker, list_tasks
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

WAIT_TIMEOUT = 20


def get_all_ray_worker_processes():
    processes = psutil.process_iter(attrs=["pid", "name", "cmdline", "status"])
    result = []
    for p in processes:
        cmdline = p.info["cmdline"]
        if cmdline is not None and len(cmdline) > 0 and "ray::" in cmdline[0]:
            result.append(p)
    print(f"all ray worker processes: {result}")
    return result


@pytest.fixture
def short_gcs_publish_timeout(monkeypatch):
    monkeypatch.setenv("RAY_MAX_GCS_PUBLISH_RETRIES", "3")
    monkeypatch.setenv("RAY_gcs_rpc_server_reconnect_timeout_s", "1")
    yield


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_ray_shutdown(short_gcs_publish_timeout, shutdown_only):
    """Make sure all ray workers are shutdown when driver is done."""
    ray.init()

    @ray.remote
    def f():
        import time

        time.sleep(10)

    num_cpus = int(ray.available_resources()["CPU"])
    tasks = [f.remote() for _ in range(num_cpus)]  # noqa
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    ray.shutdown()

    wait_for_condition(
        lambda: len(get_all_ray_worker_processes()) == 0, timeout=WAIT_TIMEOUT
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_ray_shutdown_then_call(short_gcs_publish_timeout, shutdown_only):
    """Make sure ray will not kill cpython when using unrecognized ObjectId"""
    # Set include_dashboard=False to have faster startup.
    ray.init(num_cpus=1, include_dashboard=False)

    my_ref = ray.put("anystring")

    @ray.remote
    def f(s):
        print(s)

    ray.shutdown()

    ray.init(num_cpus=1, include_dashboard=False)
    with pytest.raises(ValueError, match="Ray object whose owner is unknown"):
        f.remote(my_ref)  # This would cause full CPython death.

    ray.shutdown()
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_ray_shutdown_then_call_list(short_gcs_publish_timeout, shutdown_only):
    """Make sure ray will not kill cpython when using unrecognized ObjectId"""
    # Set include_dashboard=False to have faster startup.
    ray.init(num_cpus=1, include_dashboard=False)

    my_ref = ray.put("anystring")

    @ray.remote
    def f(s):
        print(s)

    ray.shutdown()

    ray.init(num_cpus=1, include_dashboard=False)
    with pytest.raises(ValueError, match="Ray object whose owner is unknown"):
        f.remote([my_ref])  # This would cause full CPython death.

    ray.shutdown()
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_ray_shutdown_then_get(short_gcs_publish_timeout, shutdown_only):
    """Make sure ray will not hang when trying to Get an unrecognized Obj."""
    # Set include_dashboard=False to have faster startup.
    ray.init(num_cpus=1, include_dashboard=False)

    my_ref = ray.put("anystring")

    ray.shutdown()

    ray.init(num_cpus=1, include_dashboard=False)
    with pytest.raises(ValueError, match="Ray objects whose owner is unknown"):
        # This used to cause ray to hang indefinitely (without timeout) or
        # throw a timeout exception if a timeout was provided. Now it is expected to
        # throw an exception reporting the unknown object.
        ray.get(my_ref, timeout=30)

    ray.shutdown()
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_ray_shutdown_then_wait(short_gcs_publish_timeout, shutdown_only):
    """Make sure ray will not hang when trying to Get an unrecognized Obj."""
    # Set include_dashboard=False to have faster startup.
    ray.init(num_cpus=1, include_dashboard=False)

    my_ref = ray.put("anystring")

    ray.shutdown()

    ray.init(num_cpus=1, include_dashboard=False)
    my_new_ref = ray.put("anyotherstring")

    # If we have some known and some unknown references, we allow the
    # function to wait for the valid references; however, if all the
    # references are unknown, we expect an error.
    ready, not_ready = ray.wait([my_new_ref, my_ref])
    with pytest.raises(ValueError, match="Ray object whose owner is unknown"):
        # This used to cause ray to hang indefinitely (without timeout) or
        # forever return all tasks as not-ready if a timeout was provided.
        # Now it is expected to throw an exception reporting if all objects are
        # unknown.
        ray.wait(not_ready, timeout=30)

    ray.shutdown()
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_driver_dead(short_gcs_publish_timeout, shutdown_only):
    """Make sure all ray workers are shutdown when driver is killed."""
    driver = """
import ray
ray.init(_system_config={"gcs_rpc_server_reconnect_timeout_s": 1})
@ray.remote
def f():
    import time
    time.sleep(10)

num_cpus = int(ray.available_resources()["CPU"])
tasks = [f.remote() for _ in range(num_cpus)]
"""

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    assert p.poll() is None
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    # Kill the driver process.
    p.kill()
    p.wait()
    time.sleep(0.1)

    wait_for_condition(
        lambda: len(get_all_ray_worker_processes()) == 0, timeout=WAIT_TIMEOUT
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_node_killed(short_gcs_publish_timeout, ray_start_cluster):
    """Make sure all ray workers when nodes are dead."""
    cluster = ray_start_cluster
    # head node.
    cluster.add_node(
        num_cpus=0, _system_config={"gcs_rpc_server_reconnect_timeout_s": 1}
    )
    ray.init(address="auto")

    num_worker_nodes = 2
    workers = []
    for _ in range(num_worker_nodes):
        workers.append(cluster.add_node(num_cpus=2))
    cluster.wait_for_nodes()

    @ray.remote
    def f():
        import time

        time.sleep(100)

    num_cpus = int(ray.available_resources()["CPU"])
    tasks = [f.remote() for _ in range(num_cpus)]  # noqa
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    for worker in workers:
        cluster.remove_node(worker)

    wait_for_condition(
        lambda: len(get_all_ray_worker_processes()) == 0, timeout=WAIT_TIMEOUT
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_head_node_down(short_gcs_publish_timeout, ray_start_cluster):
    """Make sure all ray workers when head node is dead."""
    cluster = ray_start_cluster
    # head node.
    head = cluster.add_node(
        num_cpus=2, _system_config={"gcs_rpc_server_reconnect_timeout_s": 1}
    )

    # worker nodes.
    num_worker_nodes = 2
    for _ in range(num_worker_nodes):
        cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Start a driver.
    driver = """
import ray
ray.init(address="{}")
@ray.remote
def f():
    import time
    time.sleep(10)

num_cpus = int(ray.available_resources()["CPU"])
tasks = [f.remote() for _ in range(num_cpus)]
import time
time.sleep(100)
""".format(
        cluster.address
    )

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    wait_for_condition(lambda: p.poll() is None)
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    cluster.remove_node(head)

    wait_for_condition(
        lambda: len(get_all_ray_worker_processes()) == 0, timeout=WAIT_TIMEOUT
    )


def test_raylet_graceful_exit_upon_agent_exit(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=0)

    def get_raylet_agent_procs(worker):
        raylet = None
        for p in worker.live_processes():
            if p[0] == "raylet":
                raylet = p[1]
        assert raylet is not None

        children = psutil.Process(raylet.pid).children()
        target_path = os.path.join("dashboard", "agent.py")
        for child in children:
            if target_path in " ".join(child.cmdline()):
                return raylet, child
        raise ValueError("dashboard agent not found")

    # Make sure raylet exits gracefully upon agent terminated by SIGTERM.
    worker = cluster.add_node(num_cpus=0)
    raylet, agent = get_raylet_agent_procs(worker)
    agent.terminate()
    exit_code = raylet.wait()
    # When the agent is terminated
    assert exit_code == 0

    # Make sure raylet exits gracefully upon agent terminated by SIGKILL.
    # TODO(sang): Make raylet exits ungracefully in this case. It is currently
    # not possible because we cannot detect the exit code of children process
    # from cpp code.
    worker = cluster.add_node(num_cpus=0)
    raylet, agent = get_raylet_agent_procs(worker)
    agent.kill()
    exit_code = raylet.wait()
    # When the agent is terminated
    assert exit_code == 0


def test_raylet_graceful_exit_upon_runtime_env_agent_exit(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=0)

    def get_raylet_runtime_env_agent_procs(worker):
        raylet = None
        for p in worker.live_processes():
            if p[0] == "raylet":
                raylet = p[1]
        assert raylet is not None

        children = psutil.Process(raylet.pid).children()
        target_path = os.path.join("runtime_env", "agent", "main.py")
        for child in children:
            if target_path in " ".join(child.cmdline()):
                return raylet, child
        raise ValueError("runtime env agent not found")

    # Make sure raylet exits gracefully upon agent terminated by SIGTERM.
    worker = cluster.add_node(num_cpus=0)
    raylet, agent = get_raylet_runtime_env_agent_procs(worker)
    agent.terminate()
    exit_code = raylet.wait()
    # When the agent is terminated
    assert exit_code == 0

    # Make sure raylet exits gracefully upon agent terminated by SIGKILL.
    # TODO(sang): Make raylet exits ungracefully in this case. It is currently
    # not possible because we cannot detect the exit code of children process
    # from cpp code.
    worker = cluster.add_node(num_cpus=0)
    raylet, agent = get_raylet_runtime_env_agent_procs(worker)
    agent.kill()
    exit_code = raylet.wait()
    # When the agent is terminated
    assert exit_code == 0


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_worker_sigterm(shutdown_only):
    """Verify a worker process is killed by a sigterm."""
    ray.init(num_cpus=1)

    @ray.remote
    def f():
        return os.getpid(), ray.get_runtime_context().get_worker_id()

    pid, worker_id = ray.get(f.remote())
    os.kill(pid, signal.SIGTERM)

    def verify():
        w = get_worker(id=worker_id)
        alive = w.is_alive
        assert not alive
        assert "SIGTERM" in w.exit_detail
        assert w.exit_type == "SYSTEM_ERROR"
        return True

    wait_for_condition(verify)

    @ray.remote(num_cpus=1)
    class Actor:
        def pid(self):
            return os.getpid(), ray.get_runtime_context().get_worker_id()

    a = Actor.remote()
    pid, worker_id = ray.get(a.pid.remote())
    os.kill(pid, signal.SIGTERM)

    def verify():
        w = get_worker(id=worker_id)
        alive = w.is_alive
        assert not alive
        assert "SIGTERM" in w.exit_detail
        assert w.exit_type == "SYSTEM_ERROR"
        return True

    wait_for_condition(verify)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_worker_proc_child_no_leak(shutdown_only):
    """Verify a worker process is not leaked when placement group is removed"""
    ray.init(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self) -> None:
            self.p = None

        def sleep(self):
            mp_context = multiprocessing.get_context("spawn")
            self.p = mp_context.Process(target=time.sleep, args=(1000,))
            self.p.daemon = True
            self.p.start()

            print(f"[pid={os.getpid()}ppid={os.getppid()}]sleeping for 1")
            time.sleep(1)
            return ray.get_runtime_context().get_worker_id(), self.p.pid

    # Create a placement group
    pg = ray.util.placement_group([{"CPU": 1}])
    ray.get(pg.ready())

    # Create an actor to a placement group.
    actor = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
        )
    ).remote()

    ray.get(actor.__ray_ready__.remote())
    worker_id, child_pid = ray.get(actor.sleep.remote())

    # Remove the placement group
    ray.util.remove_placement_group(pg)

    def verify():
        try:
            psutil.Process(pid=child_pid).status()
        except psutil.NoSuchProcess:
            return True

    wait_for_condition(verify)

    def verify():
        w = get_worker(id=worker_id)
        assert "placement group was removed" in w.exit_detail
        assert w.exit_type == "INTENDED_SYSTEM_EXIT"
        return True

    wait_for_condition(verify)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
@pytest.mark.parametrize("is_get", [False, True])
def test_sigterm_while_ray_get_and_wait(shutdown_only, is_get):
    """Verify when sigterm is received while running
    ray.get, it will clean up the worker process properly.
    """

    @ray.remote(max_retries=0)
    def f():
        time.sleep(300)

    @ray.remote(max_retries=0)
    def ray_get_wait_task():
        ref = f.remote()
        if is_get:
            ray.get(ref)
        else:
            ray.wait([ref])

    # TODO(sang): The task failure is not properly propagated.
    r = ray_get_wait_task.remote()

    def wait_for_task():
        t = list_tasks(filters=[("name", "=", "ray_get_wait_task")])[0]
        return t.state == "RUNNING"

    wait_for_condition(wait_for_task)

    # Kill a task that's blocked by ray.get
    t = list_tasks(filters=[("name", "=", "ray_get_wait_task")])[0]
    os.kill(t.worker_pid, signal.SIGTERM)

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(r)

    def verify():
        t = list_tasks(filters=[("name", "=", "ray_get_wait_task")])[0]
        w = get_worker(t.worker_id)
        assert t.state == "FAILED"
        assert w.exit_type == "SYSTEM_ERROR"
        assert "SIGTERM" in w.exit_detail
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
