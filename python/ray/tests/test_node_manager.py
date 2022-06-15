import ray
from ray._private.test_utils import run_string_as_driver


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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
