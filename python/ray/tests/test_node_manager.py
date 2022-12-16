import ray
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
    get_resource_usage,
)
import pytest
import os


# This tests the queue transitions for infeasible tasks. This has been an issue
# in the past, e.g., https://github.com/ray-project/ray/issues/3275.
def test_infeasible_tasks(ray_start_cluster):
    cluster = ray_start_cluster

    @ray.remote
    def f():
        return

    cluster.add_node(resources={str(0): 100})
    ray.init(address=cluster.address)

    # Submit an infeasible task.
    x_id = f._remote(args=[], kwargs={}, resources={str(1): 1})

    # Add a node that makes the task feasible and make sure we can get the
    # result.
    cluster.add_node(resources={str(1): 100})
    ray.get(x_id)

    # Start a driver that submits an infeasible task and then let it exit.
    driver_script = """
import ray

ray.init(address="{}")

@ray.remote(resources={})
def f():
{}pass  # This is a weird hack to insert some blank space.

f.remote()
""".format(
        cluster.address, "{str(2): 1}", "    "
    )

    run_string_as_driver(driver_script)

    # Now add a new node that makes the task feasible.
    cluster.add_node(resources={str(2): 100})

    # Make sure we can still run tasks on all nodes.
    ray.get([f._remote(args=[], kwargs={}, resources={str(i): 1}) for i in range(3)])


@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head"""],
    indirect=True,
)
def test_kill_driver_clears_backlog(call_ray_start):
    driver = """
import ray

@ray.remote
def f():
    import time
    time.sleep(300)

refs = [f.remote() for _ in range(10000)]

ray.get(refs)
  """
    proc = run_string_as_driver_nonblocking(driver)
    ctx = ray.init(address=call_ray_start)

    def check_has_backlog() -> bool:
        resources_batch = get_resource_usage(
            gcs_address=ctx.address_info["gcs_address"]
        )
        backlog = (
            resources_batch.resource_load_by_shape.resource_demands[0].backlog_size
            if resources_batch.resource_load_by_shape.resource_demands
            else 0
        )
        return backlog > 0

    wait_for_condition(check_has_backlog, timeout=10, retry_interval_ms=1000)

    os.kill(proc.pid, 9)

    def check_no_backlog() -> bool:
        resources_batch = get_resource_usage(
            gcs_address=ctx.address_info["gcs_address"]
        )
        backlog = (
            resources_batch.resource_load_by_shape.resource_demands[0].backlog_size
            if resources_batch.resource_load_by_shape.resource_demands
            else 0
        )
        return backlog == 0

    wait_for_condition(check_no_backlog, timeout=10, retry_interval_ms=1000)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
