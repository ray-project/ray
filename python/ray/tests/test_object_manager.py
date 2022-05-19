from collections import defaultdict
import multiprocessing
import numpy as np
import pytest
import time
import warnings

import ray
from ray.cluster_utils import Cluster, cluster_not_supported
from ray.exceptions import GetTimeoutError
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

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


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "num_cpus": 0,
            "object_store_memory": 75 * 1024 * 1024,
        }
    ],
    indirect=True,
)
def test_object_transfer_during_oom(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(object_store_memory=75 * 1024 * 1024)

    @ray.remote
    def put():
        return np.random.rand(5 * 1024 * 1024)  # 40 MB data

    local_ref = ray.put(np.random.rand(5 * 1024 * 1024))
    remote_ref = put.remote()

    with pytest.raises(GetTimeoutError):
        ray.get(remote_ref, timeout=1)
    del local_ref
    ray.get(remote_ref)


# This test is here to make sure that when we broadcast an object to a bunch of
# machines, we don't have too many excess object transfers.
@pytest.mark.skip(reason="TODO(ekl)")
def test_object_broadcast(ray_start_cluster_with_resource):
    cluster, num_nodes = ray_start_cluster_with_resource

    @ray.remote
    def f(x):
        return

    x = np.zeros(1024 * 1024, dtype=np.uint8)

    @ray.remote
    def create_object():
        return np.zeros(1024 * 1024, dtype=np.uint8)

    object_refs = []

    for _ in range(3):
        # Broadcast an object to all machines.
        x_id = ray.put(x)
        object_refs.append(x_id)
        ray.get(
            [
                f._remote(args=[x_id], resources={str(i % num_nodes): 1})
                for i in range(10 * num_nodes)
            ]
        )

    for _ in range(3):
        # Broadcast an object to all machines.
        x_id = create_object.remote()
        object_refs.append(x_id)
        ray.get(
            [
                f._remote(args=[x_id], resources={str(i % num_nodes): 1})
                for i in range(10 * num_nodes)
            ]
        )

    # Wait for profiling information to be pushed to the profile table.
    time.sleep(1)
    transfer_events = ray.state.object_transfer_timeline()

    # Make sure that each object was transferred a reasonable number of times.
    for x_id in object_refs:
        relevant_events = [
            event
            for event in transfer_events
            if event["cat"] == "transfer_send"
            and event["args"][0] == x_id.hex()
            and event["args"][2] == 1
        ]

        # NOTE: Each event currently appears twice because we duplicate the
        # send and receive boxes to underline them with a box (black if it is a
        # send and gray if it is a receive). So we need to remove these extra
        # boxes here.
        deduplicated_relevant_events = [
            event for event in relevant_events if event["cname"] != "black"
        ]
        assert len(deduplicated_relevant_events) * 2 == len(relevant_events)
        relevant_events = deduplicated_relevant_events

        # Each object must have been broadcast to each remote machine.
        assert len(relevant_events) >= num_nodes - 1
        # If more object transfers than necessary have been done, print a
        # warning.
        if len(relevant_events) > num_nodes - 1:
            warnings.warn(
                "This object was transferred {} times, when only {} "
                "transfers were required.".format(len(relevant_events), num_nodes - 1)
            )
        # Each object should not have been broadcast more than once from every
        # machine to every other machine. Also, a pair of machines should not
        # both have sent the object to each other.
        assert len(relevant_events) <= (num_nodes - 1) * num_nodes / 2

        # Make sure that no object was sent multiple times between the same
        # pair of object managers.
        send_counts = defaultdict(int)
        for event in relevant_events:
            # The pid identifies the sender and the tid identifies the
            # receiver.
            send_counts[(event["pid"], event["tid"])] += 1
        assert all(value == 1 for value in send_counts.values())


# When submitting an actor method, we try to pre-emptively push its arguments
# to the actor's object manager. However, in the past we did not deduplicate
# the pushes and so the same object could get shipped to the same object
# manager many times. This test checks that that isn't happening.
def test_actor_broadcast(ray_start_cluster_with_resource):
    cluster, num_nodes = ray_start_cluster_with_resource

    @ray.remote
    class Actor:
        def ready(self):
            pass

        def set_weights(self, x):
            pass

    actors = [
        Actor._remote(
            args=[], kwargs={}, num_cpus=0.01, resources={str(i % num_nodes): 1}
        )
        for i in range(30)
    ]

    # Wait for the actors to start up.
    ray.get([a.ready.remote() for a in actors])

    object_refs = []

    # Broadcast a large object to all actors.
    for _ in range(5):
        x_id = ray.put(np.zeros(1024 * 1024, dtype=np.uint8))
        object_refs.append(x_id)
        # Pass the object into a method for every actor.
        ray.get([a.set_weights.remote(x_id) for a in actors])

    # Wait for profiling information to be pushed to the profile table.
    time.sleep(1)
    # TODO(Sang): Re-enable it after event is introduced.
    # transfer_events = ray.state.object_transfer_timeline()

    # # Make sure that each object was transferred a reasonable number of times. # noqa
    # for x_id in object_refs:
    #     relevant_events = [
    #         event for event in transfer_events if
    #         event["cat"] == "transfer_send" and event["args"][0] == x_id.hex() # noqa
    #     ]

    #     # NOTE: Each event currently appears twice because we duplicate the
    #     # send and receive boxes to underline them with a box (black if it is a # noqa
    #     # send and gray if it is a receive). So we need to remove these extra
    #     # boxes here.
    #     deduplicated_relevant_events = [
    #         event for event in relevant_events if event["cname"] != "black"
    #     ]
    #     assert len(deduplicated_relevant_events) * 2 == len(relevant_events)
    #     relevant_events = deduplicated_relevant_events

    #     # Each object must have been broadcast to each remote machine.
    #     assert len(relevant_events) >= num_nodes - 1
    #     # If more object transfers than necessary have been done, print a
    #     # warning.
    #     if len(relevant_events) > num_nodes - 1:
    #         warnings.warn("This object was transferred {} times, when only {} " # noqa
    #                       "transfers were required.".format(
    #                           len(relevant_events), num_nodes - 1))
    #     # Each object should not have been broadcast more than once from every # noqa
    #     # machine to every other machine. Also, a pair of machines should not
    #     # both have sent the object to each other.
    #     assert len(relevant_events) <= (num_nodes - 1) * num_nodes / 2

    #     # Make sure that no object was sent multiple times between the same
    #     # pair of object managers.
    #     send_counts = defaultdict(int)
    #     for event in relevant_events:
    #         # The pid identifies the sender and the tid identifies the
    #         # receiver.
    #         send_counts[(event["pid"], event["tid"])] += 1
    #     assert all(value == 1 for value in send_counts.values())


# The purpose of this test is to make sure we can transfer many objects. In the
# past, this has caused failures in which object managers create too many open
# files and run out of resources.
def test_many_small_transfers(ray_start_cluster_with_resource):
    cluster, num_nodes = ray_start_cluster_with_resource

    @ray.remote
    def f(*args):
        pass

    # This function creates 1000 objects on each machine and then transfers
    # each object to every other machine.
    def do_transfers():
        id_lists = []
        for i in range(num_nodes):
            id_lists.append(
                [
                    f._remote(args=[], kwargs={}, resources={str(i): 1})
                    for _ in range(1000)
                ]
            )
        ids = []
        for i in range(num_nodes):
            for j in range(num_nodes):
                if i == j:
                    continue
                ids.append(
                    f._remote(args=id_lists[j], kwargs={}, resources={str(i): 1})
                )

        # Wait for all of the transfers to finish.
        ray.get(ids)

    do_transfers()
    do_transfers()
    do_transfers()
    do_transfers()


# This is a basic test to ensure that the pull request retry timer is
# integrated properly. To test it, we create a 2 node cluster then do the
# following:
# (1) Fill up the driver's object store.
# (2) Fill up the remote node's object store.
# (3) Try to get the remote object. This should fail due to an OOM error caused
#     by step 1.
# (4) Allow the local object to be evicted.
# (5) Try to get the object again. Now the retry timer should kick in and
#     successfuly pull the remote object.
def test_pull_request_retry(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, num_gpus=1, object_store_memory=100 * 2 ** 20)
    cluster.add_node(num_cpus=1, num_gpus=0, object_store_memory=100 * 2 ** 20)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote
    def put():
        return np.zeros(64 * 2 ** 20, dtype=np.int8)

    @ray.remote(num_cpus=0, num_gpus=1)
    def driver():
        local_ref = ray.put(np.zeros(64 * 2 ** 20, dtype=np.int8))

        remote_ref = put.remote()

        ready, _ = ray.wait([remote_ref], timeout=30)
        assert len(ready) == 1

        del local_ref

        # This should always complete within 10 seconds.
        ready, _ = ray.wait([remote_ref], timeout=20)
        assert len(ready) > 0

    # Pretend the GPU node is the driver. We do this to force the placement of
    # the driver and `put` task on different nodes.
    ray.get(driver.remote())


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
def test_pull_bundles_admission_control(ray_start_cluster):
    cluster = ray_start_cluster
    object_size = int(6e6)
    num_objects = 10
    num_tasks = 10
    # Head node can fit all of the objects at once.
    cluster.add_node(
        num_cpus=0, object_store_memory=2 * num_tasks * num_objects * object_size
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Worker node can only fit 1 task at a time.
    cluster.add_node(num_cpus=1, object_store_memory=1.5 * num_objects * object_size)
    cluster.wait_for_nodes()

    @ray.remote
    def foo(*args):
        return

    args = []
    for _ in range(num_tasks):
        task_args = [
            ray.put(np.zeros(object_size, dtype=np.uint8)) for _ in range(num_objects)
        ]
        args.append(task_args)

    tasks = [foo.remote(*task_args) for task_args in args]
    ray.get(tasks)


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
def test_pull_bundles_pinning(ray_start_cluster):
    cluster = ray_start_cluster
    object_size = int(50e6)
    num_objects = 10
    # Head node can fit all of the objects at once.
    cluster.add_node(num_cpus=0, object_store_memory=1000e6)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Worker node cannot even fit a single task.
    cluster.add_node(num_cpus=1, object_store_memory=200e6)
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1)
    def foo(*args):
        return

    task_args = [
        ray.put(np.zeros(object_size, dtype=np.uint8)) for _ in range(num_objects)
    ]
    ray.get(foo.remote(*task_args))


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


def test_pull_bundle_deadlock(ray_start_cluster):
    # Test https://github.com/ray-project/ray/issues/13689
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=0,
        _system_config={
            "max_direct_call_object_size": int(1e7),
        },
    )
    ray.init(address=cluster.address)
    worker_node_1 = cluster.add_node(
        num_cpus=8,
        resources={"worker_node_1": 1},
    )
    cluster.add_node(
        num_cpus=8,
        resources={"worker_node_2": 1},
        object_store_memory=int(1e8 * 2 - 10),
    )
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=0)
    def get_node_id():
        return ray.get_runtime_context().node_id

    worker_node_1_id = ray.get(
        get_node_id.options(resources={"worker_node_1": 0.1}).remote()
    )
    worker_node_2_id = ray.get(
        get_node_id.options(resources={"worker_node_2": 0.1}).remote()
    )

    object_a = ray.put(np.zeros(int(1e8), dtype=np.uint8))

    @ray.remote(
        scheduling_strategy=NodeAffinitySchedulingStrategy(worker_node_1_id, soft=True)
    )
    def task_a_to_b(a):
        return np.zeros(int(1e8), dtype=np.uint8)

    object_b = task_a_to_b.remote(object_a)
    ray.wait([object_b], fetch_local=False)

    @ray.remote(
        scheduling_strategy=NodeAffinitySchedulingStrategy(worker_node_2_id, soft=False)
    )
    def task_b_to_c(b):
        return "c"

    object_c = task_b_to_c.remote(object_b)
    # task_a_to_b will be re-executed on worker_node_2 so pull manager there will
    # have object_a pull request after the existing object_b pull request.
    # Make sure object_b pull request won't block the object_a pull request.
    cluster.remove_node(worker_node_1, allow_graceful=False)
    assert ray.get(object_c) == "c"


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
