import multiprocessing
import numpy as np
import pytest
import time
import warnings

import ray
from ray.cluster_utils import Cluster, cluster_not_supported

if (
    multiprocessing.cpu_count() < 40
    or ray._private.utils.get_system_memory() < 50 * 10 ** 9
):
    warnings.warn("This test must be run on large machines.")


def create_cluster(num_nodes):
    cluster = Cluster()
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 100}, object_store_memory=10 ** 9)

    ray.init(address=cluster.address)
    return cluster


@pytest.fixture()
def ray_start_cluster_with_resource():
    num_nodes = 5
    cluster = create_cluster(num_nodes)
    yield cluster, num_nodes

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
def test_pull_bundles_admission_control_dynamic(
    enable_mac_large_object_store, ray_start_cluster
):
    # This test is the same as test_pull_bundles_admission_control, except that
    # the object store's capacity starts off higher and is later consumed
    # dynamically by concurrent workers.
    cluster = ray_start_cluster
    object_size = int(6e6)
    num_objects = 20
    num_tasks = 20
    # Head node can fit all of the objects at once.
    cluster.add_node(
        num_cpus=0, object_store_memory=2 * num_tasks * num_objects * object_size
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Worker node can fit 2 tasks at a time.
    cluster.add_node(num_cpus=1, object_store_memory=2.5 * num_objects * object_size)
    cluster.wait_for_nodes()

    @ray.remote
    def foo(i, *args):
        print("foo", i)
        return

    @ray.remote
    def allocate(i):
        print("allocate", i)
        return np.zeros(object_size, dtype=np.uint8)

    args = []
    for _ in range(num_tasks):
        task_args = [
            ray.put(np.zeros(object_size, dtype=np.uint8)) for _ in range(num_objects)
        ]
        args.append(task_args)

    allocated = [allocate.remote(i) for i in range(num_objects)]
    ray.get(allocated)

    tasks = [foo.remote(i, *task_args) for i, task_args in enumerate(args)]
    ray.get(tasks)
    del allocated


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
def test_max_pinned_args_memory(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=0,
        object_store_memory=200 * 1024 * 1024,
        _system_config={
            "max_task_args_memory_fraction": 0.7,
        },
    )
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=3, object_store_memory=100 * 1024 * 1024)

    @ray.remote
    def f(arg):
        time.sleep(1)
        return np.zeros(30 * 1024 * 1024, dtype=np.uint8)

    # Each task arg takes about 30% of the remote node's memory. We should
    # execute at most 2 at a time to make sure we have room for at least 1 task
    # output.
    x = np.zeros(30 * 1024 * 1024, dtype=np.uint8)
    ray.get([f.remote(ray.put(x)) for _ in range(3)])

    @ray.remote
    def large_arg(arg):
        return

    # Executing a task whose args are greater than the memory threshold is
    # okay.
    ref = np.zeros(80 * 1024 * 1024, dtype=np.uint8)
    ray.get(large_arg.remote(ref))


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
def test_ray_get_task_args_deadlock(ray_start_cluster):
    cluster = ray_start_cluster
    object_size = int(6e6)
    num_objects = 10
    # Head node can fit all of the objects at once.
    cluster.add_node(num_cpus=0, object_store_memory=4 * num_objects * object_size)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Worker node can only fit 1 task at a time.
    cluster.add_node(num_cpus=1, object_store_memory=1.5 * num_objects * object_size)
    cluster.wait_for_nodes()

    @ray.remote
    def foo(*args):
        return

    @ray.remote
    def test_deadlock(get_args, task_args):
        foo.remote(*task_args)
        ray.get(get_args)

    for i in range(5):
        start = time.time()
        get_args = [
            ray.put(np.zeros(object_size, dtype=np.uint8)) for _ in range(num_objects)
        ]
        task_args = [
            ray.put(np.zeros(object_size, dtype=np.uint8)) for _ in range(num_objects)
        ]
        ray.get(test_deadlock.remote(get_args, task_args))
        print(f"round {i} finished in {time.time() - start}")


def test_object_directory_basic(ray_start_cluster_with_resource):
    cluster, num_nodes = ray_start_cluster_with_resource

    @ray.remote
    def task(x):
        pass

    # Test a single task.
    x_id = ray.put(np.zeros(1024 * 1024, dtype=np.uint8))
    ray.get(task.options(resources={str(3): 1}).remote(x_id), timeout=10)

    # Test multiple tasks on all nodes can find locations properly.
    object_refs = []
    for _ in range(num_nodes):
        object_refs.append(ray.put(np.zeros(1024 * 1024, dtype=np.uint8)))
    ray.get(
        [
            task.options(resources={str(i): 1}).remote(object_refs[i])
            for i in range(num_nodes)
        ]
    )
    del object_refs

    @ray.remote
    class ObjectHolder:
        def __init__(self):
            self.x = ray.put(np.zeros(1024 * 1024, dtype=np.uint8))

        def get_obj(self):
            return self.x

        def ready(self):
            return True

    # Test if tasks can find object location properly
    # when there are multiple owners
    object_holders = [
        ObjectHolder.options(num_cpus=0.01, resources={str(i): 1}).remote()
        for i in range(num_nodes)
    ]
    ray.get([o.ready.remote() for o in object_holders])

    object_refs = []
    for i in range(num_nodes):
        object_refs.append(object_holders[(i + 1) % num_nodes].get_obj.remote())
    ray.get(
        [
            task.options(num_cpus=0.01, resources={str(i): 1}).remote(object_refs[i])
            for i in range(num_nodes)
        ]
    )

    # Test a stressful scenario.
    object_refs = []
    repeat = 10
    for _ in range(num_nodes):
        for _ in range(repeat):
            object_refs.append(ray.put(np.zeros(1024 * 1024, dtype=np.uint8)))
    tasks = []
    for i in range(num_nodes):
        for r in range(repeat):
            tasks.append(
                task.options(num_cpus=0.01, resources={str(i): 0.1}).remote(
                    object_refs[i * r]
                )
            )
    ray.get(tasks)

    object_refs = []
    for i in range(num_nodes):
        object_refs.append(object_holders[(i + 1) % num_nodes].get_obj.remote())
    tasks = []
    for i in range(num_nodes):
        for _ in range(10):
            tasks.append(
                task.options(num_cpus=0.01, resources={str(i): 0.1}).remote(
                    object_refs[(i + 1) % num_nodes]
                )
            )


def test_object_directory_failure(ray_start_cluster):
    cluster = ray_start_cluster
    config = {
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_period_milliseconds": 500,
        "object_timeout_milliseconds": 200,
    }
    # Add a head node.
    cluster.add_node(_system_config=config)
    ray.init(address=cluster.address)

    # Add worker nodes.
    num_nodes = 5
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 100})

    # Add a node to be removed
    index_killing_node = num_nodes
    node_to_kill = cluster.add_node(
        resources={str(index_killing_node): 100}, object_store_memory=10 ** 9
    )

    @ray.remote
    class ObjectHolder:
        def __init__(self):
            self.x = ray.put(np.zeros(1024 * 1024, dtype=np.uint8))

        def get_obj(self):
            return [self.x]

        def ready(self):
            return True

    oh = ObjectHolder.options(
        num_cpus=0.01, resources={str(index_killing_node): 1}
    ).remote()
    obj = ray.get(oh.get_obj.remote())[0]

    @ray.remote
    def task(x):
        pass

    tasks = []
    repeat = 3
    for i in range(num_nodes):
        for _ in range(repeat):
            tasks.append(task.options(resources={str(i): 1}).remote(obj))
    cluster.remove_node(node_to_kill, allow_graceful=False)

    for t in tasks:
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(t, timeout=10)


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "num_cpus": 0,
            "object_store_memory": 75 * 1024 * 1024,
            "_system_config": {
                "worker_lease_timeout_milliseconds": 0,
                "object_manager_pull_timeout_ms": 20000,
                "object_spilling_threshold": 1.0,
            },
        }
    ],
    indirect=True,
)
def test_maximize_concurrent_pull_race_condition(ray_start_cluster_head):
    # Test if https://github.com/ray-project/ray/issues/18062 is mitigated
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=8, object_store_memory=75 * 1024 * 1024)

    @ray.remote
    class RemoteObjectCreator:
        def put(self, i):
            return np.random.rand(i * 1024 * 1024)  # 8 MB data

        def idle(self):
            pass

    @ray.remote
    def f(x):
        print(f"timestamp={time.time()} pulled {len(x)*8} bytes")
        time.sleep(1)
        return

    remote_obj_creator = RemoteObjectCreator.remote()
    remote_refs = [remote_obj_creator.put.remote(1) for _ in range(7)]
    print(remote_refs)
    # Make sure all objects are created.
    ray.get(remote_obj_creator.idle.remote())

    local_refs = [ray.put(np.random.rand(1 * 1024 * 1024)) for _ in range(20)]
    remote_tasks = [f.remote(x) for x in local_refs]

    start = time.time()
    ray.get(remote_tasks)
    end = time.time()
    assert (
        end - start < 20
    ), "Too much time spent in pulling objects, check the amount of time in retries"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
