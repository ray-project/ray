from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

import ray
import ray.tests.cluster_utils
import ray.tests.utils

logger = logging.getLogger(__name__)


def test_dynamic_res_creation(ray_start_regular):
    # This test creates a resource locally (without specifying the client_id)
    res_name = "test_res"
    res_capacity = 1.0

    @ray.remote
    def set_res(resource_name, resource_capacity):
        ray.experimental.set_resource(resource_name, resource_capacity)

    ray.get(set_res.remote(res_name, res_capacity))

    available_res = ray.available_resources()
    cluster_res = ray.cluster_resources()

    assert available_res[res_name] == res_capacity
    assert cluster_res[res_name] == res_capacity


def test_dynamic_res_deletion(shutdown_only):
    # This test deletes a resource locally (without specifying the client_id)
    res_name = "test_res"
    res_capacity = 1.0

    ray.init(num_cpus=1, resources={res_name: res_capacity})

    @ray.remote
    def delete_res(resource_name):
        ray.experimental.set_resource(resource_name, 0)

    ray.get(delete_res.remote(res_name))

    available_res = ray.available_resources()
    cluster_res = ray.cluster_resources()

    assert res_name not in available_res
    assert res_name not in cluster_res


def test_dynamic_res_infeasible_rescheduling(ray_start_regular):
    # This test launches an infeasible task and then creates a
    # resource to make the task feasible. This tests if the
    # infeasible tasks get rescheduled when resources are
    # created at runtime.
    res_name = "test_res"
    res_capacity = 1.0

    @ray.remote
    def set_res(resource_name, resource_capacity):
        ray.experimental.set_resource(resource_name, resource_capacity)

    def f():
        return 1

    remote_task = ray.remote(resources={res_name: res_capacity})(f)
    oid = remote_task.remote()  # This is infeasible
    ray.get(set_res.remote(res_name, res_capacity))  # Now should be feasible

    available_res = ray.available_resources()
    assert available_res[res_name] == res_capacity

    successful, unsuccessful = ray.wait([oid], timeout=1)
    assert successful  # The task completed


def test_dynamic_res_updation_clientid(ray_start_cluster):
    # This test does a simple resource capacity update
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    target_node_id = ray.nodes()[1]["NodeID"]

    @ray.remote
    def set_res(resource_name, resource_capacity, client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=client_id)

    # Create resource
    ray.get(set_res.remote(res_name, res_capacity, target_node_id))

    # Update resource
    new_capacity = res_capacity + 1
    ray.get(set_res.remote(res_name, new_capacity, target_node_id))

    target_node = next(
        node for node in ray.nodes() if node["NodeID"] == target_node_id)
    resources = target_node["Resources"]

    assert res_name in resources
    assert resources[res_name] == new_capacity


def test_dynamic_res_creation_clientid(ray_start_cluster):
    # Creates a resource on a specific client and verifies creation.
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    target_node_id = ray.nodes()[1]["NodeID"]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    ray.get(set_res.remote(res_name, res_capacity, target_node_id))
    target_node = next(
        node for node in ray.nodes() if node["NodeID"] == target_node_id)
    resources = target_node["Resources"]

    assert res_name in resources
    assert resources[res_name] == res_capacity


def test_dynamic_res_creation_clientid_multiple(ray_start_cluster):
    # This test creates resources on multiple clients using the clientid
    # specifier
    cluster = ray_start_cluster

    TIMEOUT = 5
    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    target_node_ids = [node["NodeID"] for node in ray.nodes()]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    results = []
    for nid in target_node_ids:
        results.append(set_res.remote(res_name, res_capacity, nid))
    ray.get(results)

    success = False
    start_time = time.time()

    while time.time() - start_time < TIMEOUT and not success:
        resources_created = []
        for nid in target_node_ids:
            target_node = next(
                node for node in ray.nodes() if node["NodeID"] == nid)
            resources = target_node["Resources"]
            resources_created.append(resources[res_name] == res_capacity)
        success = all(resources_created)
    assert success


def test_dynamic_res_deletion_clientid(ray_start_cluster):
    # This test deletes a resource on a given client id
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 5

    for i in range(num_nodes):
        # Create resource on all nodes, but later we'll delete it from a
        # target node
        cluster.add_node(resources={res_name: res_capacity})

    ray.init(redis_address=cluster.redis_address)

    target_node_id = ray.nodes()[1]["NodeID"]

    # Launch the delete task
    @ray.remote
    def delete_res(resource_name, res_client_id):
        ray.experimental.set_resource(
            resource_name, 0, client_id=res_client_id)

    ray.get(delete_res.remote(res_name, target_node_id))

    target_node = next(
        node for node in ray.nodes() if node["NodeID"] == target_node_id)
    resources = target_node["Resources"]
    print(ray.cluster_resources())
    assert res_name not in resources


def test_dynamic_res_creation_scheduler_consistency(ray_start_cluster):
    # This makes sure the resource is actually created and the state is
    # consistent in the scheduler
    # by launching a task which requests the created resource
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 5

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    node_ids = [node["NodeID"] for node in ray.nodes()]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    # Create the resource on node1
    target_node_id = node_ids[1]
    ray.get(set_res.remote(res_name, res_capacity, target_node_id))

    # Define a task which requires this resource
    @ray.remote(resources={res_name: res_capacity})
    def test_func():
        return 1

    result = test_func.remote()
    successful, unsuccessful = ray.wait([result], timeout=5)
    assert successful  # The task completed


def test_dynamic_res_deletion_scheduler_consistency(ray_start_cluster):
    # This makes sure the resource is actually deleted and the state is
    # consistent in the scheduler by launching an infeasible task which
    # requests the created resource
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 5
    TIMEOUT_DURATION = 1

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    node_ids = [node["NodeID"] for node in ray.nodes()]

    @ray.remote
    def delete_res(resource_name, res_client_id):
        ray.experimental.set_resource(
            resource_name, 0, client_id=res_client_id)

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    # Create the resource on node1
    target_node_id = node_ids[1]
    ray.get(set_res.remote(res_name, res_capacity, target_node_id))
    assert ray.cluster_resources()[res_name] == res_capacity

    # Delete the resource
    ray.get(delete_res.remote(res_name, target_node_id))

    # Define a task which requires this resource. This should not run
    @ray.remote(resources={res_name: res_capacity})
    def test_func():
        return 1

    result = test_func.remote()
    successful, unsuccessful = ray.wait([result], timeout=TIMEOUT_DURATION)
    assert unsuccessful  # The task did not complete because it's infeasible


def test_dynamic_res_concurrent_res_increment(ray_start_cluster):
    # This test makes sure resource capacity is updated (increment) correctly
    # when a task has already acquired some of the resource.

    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 5
    updated_capacity = 10
    num_nodes = 5
    TIMEOUT_DURATION = 1

    # Create a object ID to have the task wait on
    WAIT_OBJECT_ID_STR = ("a" * 20).encode("ascii")

    # Create a object ID to signal that the task is running
    TASK_RUNNING_OBJECT_ID_STR = ("b" * 20).encode("ascii")

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    node_ids = [node["NodeID"] for node in ray.nodes()]
    target_node_id = node_ids[1]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    # Create the resource on node 1
    ray.get(set_res.remote(res_name, res_capacity, target_node_id))
    assert ray.cluster_resources()[res_name] == res_capacity

    # Task to hold the resource till the driver signals to finish
    @ray.remote
    def wait_func(running_oid, wait_oid):
        # Signal that the task is running
        ray.worker.global_worker.put_object(ray.ObjectID(running_oid), 1)
        # Make the task wait till signalled by driver
        ray.get(ray.ObjectID(wait_oid))

    @ray.remote
    def test_func():
        return 1

    # Launch the task with resource requirement of 4, thus the new available
    # capacity becomes 1
    task = wait_func._remote(
        args=[TASK_RUNNING_OBJECT_ID_STR, WAIT_OBJECT_ID_STR],
        resources={res_name: 4})
    # Wait till wait_func is launched before updating resource
    ray.get(ray.ObjectID(TASK_RUNNING_OBJECT_ID_STR))

    # Update the resource capacity
    ray.get(set_res.remote(res_name, updated_capacity, target_node_id))

    # Signal task to complete
    ray.worker.global_worker.put_object(ray.ObjectID(WAIT_OBJECT_ID_STR), 1)
    ray.get(task)

    # Check if scheduler state is consistent by launching a task requiring
    # updated capacity
    task_2 = test_func._remote(args=[], resources={res_name: updated_capacity})
    successful, unsuccessful = ray.wait([task_2], timeout=TIMEOUT_DURATION)
    assert successful  # The task completed

    # Check if scheduler state is consistent by launching a task requiring
    # updated capacity + 1. This should not execute
    task_3 = test_func._remote(
        args=[], resources={res_name: updated_capacity + 1
                            })  # This should be infeasible
    successful, unsuccessful = ray.wait([task_3], timeout=TIMEOUT_DURATION)
    assert unsuccessful  # The task did not complete because it's infeasible
    assert ray.available_resources()[res_name] == updated_capacity


def test_dynamic_res_concurrent_res_decrement(ray_start_cluster):
    # This test makes sure resource capacity is updated (decremented)
    # correctly when a task has already acquired some
    # of the resource.

    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 5
    updated_capacity = 2
    num_nodes = 5
    TIMEOUT_DURATION = 1

    # Create a object ID to have the task wait on
    WAIT_OBJECT_ID_STR = ("a" * 20).encode("ascii")

    # Create a object ID to signal that the task is running
    TASK_RUNNING_OBJECT_ID_STR = ("b" * 20).encode("ascii")

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    node_ids = [node["NodeID"] for node in ray.nodes()]
    target_node_id = node_ids[1]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    # Create the resource on node 1
    ray.get(set_res.remote(res_name, res_capacity, target_node_id))
    assert ray.cluster_resources()[res_name] == res_capacity

    # Task to hold the resource till the driver signals to finish
    @ray.remote
    def wait_func(running_oid, wait_oid):
        # Signal that the task is running
        ray.worker.global_worker.put_object(ray.ObjectID(running_oid), 1)
        # Make the task wait till signalled by driver
        ray.get(ray.ObjectID(wait_oid))

    @ray.remote
    def test_func():
        return 1

    # Launch the task with resource requirement of 4, thus the new available
    # capacity becomes 1
    task = wait_func._remote(
        args=[TASK_RUNNING_OBJECT_ID_STR, WAIT_OBJECT_ID_STR],
        resources={res_name: 4})
    # Wait till wait_func is launched before updating resource
    ray.get(ray.ObjectID(TASK_RUNNING_OBJECT_ID_STR))

    # Decrease the resource capacity
    ray.get(set_res.remote(res_name, updated_capacity, target_node_id))

    # Signal task to complete
    ray.worker.global_worker.put_object(ray.ObjectID(WAIT_OBJECT_ID_STR), 1)
    ray.get(task)

    # Check if scheduler state is consistent by launching a task requiring
    # updated capacity
    task_2 = test_func._remote(args=[], resources={res_name: updated_capacity})
    successful, unsuccessful = ray.wait([task_2], timeout=TIMEOUT_DURATION)
    assert successful  # The task completed

    # Check if scheduler state is consistent by launching a task requiring
    # updated capacity + 1. This should not execute
    task_3 = test_func._remote(
        args=[], resources={res_name: updated_capacity + 1
                            })  # This should be infeasible
    successful, unsuccessful = ray.wait([task_3], timeout=TIMEOUT_DURATION)
    assert unsuccessful  # The task did not complete because it's infeasible
    assert ray.available_resources()[res_name] == updated_capacity


def test_dynamic_res_concurrent_res_delete(ray_start_cluster):
    # This test makes sure resource gets deleted correctly when a task has
    # already acquired the resource

    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 5
    num_nodes = 5
    TIMEOUT_DURATION = 1

    # Create a object ID to have the task wait on
    WAIT_OBJECT_ID_STR = ("a" * 20).encode("ascii")

    # Create a object ID to signal that the task is running
    TASK_RUNNING_OBJECT_ID_STR = ("b" * 20).encode("ascii")

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    node_ids = [node["NodeID"] for node in ray.nodes()]
    target_node_id = node_ids[1]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    @ray.remote
    def delete_res(resource_name, res_client_id):
        ray.experimental.set_resource(
            resource_name, 0, client_id=res_client_id)

    # Create the resource on node 1
    ray.get(set_res.remote(res_name, res_capacity, target_node_id))
    assert ray.cluster_resources()[res_name] == res_capacity

    # Task to hold the resource till the driver signals to finish
    @ray.remote
    def wait_func(running_oid, wait_oid):
        # Signal that the task is running
        ray.worker.global_worker.put_object(ray.ObjectID(running_oid), 1)
        # Make the task wait till signalled by driver
        ray.get(ray.ObjectID(wait_oid))

    @ray.remote
    def test_func():
        return 1

    # Launch the task with resource requirement of 4, thus the new available
    # capacity becomes 1
    task = wait_func._remote(
        args=[TASK_RUNNING_OBJECT_ID_STR, WAIT_OBJECT_ID_STR],
        resources={res_name: 4})
    # Wait till wait_func is launched before updating resource
    ray.get(ray.ObjectID(TASK_RUNNING_OBJECT_ID_STR))

    # Delete the resource
    ray.get(delete_res.remote(res_name, target_node_id))

    # Signal task to complete
    ray.worker.global_worker.put_object(ray.ObjectID(WAIT_OBJECT_ID_STR), 1)
    ray.get(task)

    # Check if scheduler state is consistent by launching a task requiring
    # the deleted resource  This should not execute
    task_2 = test_func._remote(
        args=[], resources={res_name: 1})  # This should be infeasible
    successful, unsuccessful = ray.wait([task_2], timeout=TIMEOUT_DURATION)
    assert unsuccessful  # The task did not complete because it's infeasible
    assert res_name not in ray.available_resources()


def test_dynamic_res_creation_stress(ray_start_cluster):
    # This stress tests creates many resources simultaneously on the same
    # client and then checks if the final state is consistent

    cluster = ray_start_cluster

    TIMEOUT = 5
    res_capacity = 1
    num_nodes = 5
    NUM_RES_TO_CREATE = 500

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    node_ids = [node["NodeID"] for node in ray.nodes()]
    target_node_id = node_ids[1]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    @ray.remote
    def delete_res(resource_name, res_client_id):
        ray.experimental.set_resource(
            resource_name, 0, client_id=res_client_id)

    results = [
        set_res.remote(str(i), res_capacity, target_node_id)
        for i in range(0, NUM_RES_TO_CREATE)
    ]
    ray.get(results)

    success = False
    start_time = time.time()

    while time.time() - start_time < TIMEOUT and not success:
        resources = ray.cluster_resources()
        all_resources_created = []
        for i in range(0, NUM_RES_TO_CREATE):
            all_resources_created.append(str(i) in resources)
        success = all(all_resources_created)
    assert success


def test_release_cpus_when_actor_creation_task_blocking(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote(num_cpus=1)
    def get_100():
        time.sleep(1)
        return 100

    @ray.remote(num_cpus=1)
    class A(object):
        def __init__(self):
            self.num = ray.get(get_100.remote())

        def get_num(self):
            return self.num

    a = A.remote()
    assert 100 == ray.get(a.get_num.remote())

    def wait_until(condition, timeout_ms):
        SLEEP_DURATION_MS = 100
        time_elapsed = 0
        while time_elapsed <= timeout_ms:
            if condition():
                return True
            time.sleep(SLEEP_DURATION_MS)
            time_elapsed += SLEEP_DURATION_MS
        return False

    def assert_available_resources():
        return 1 == ray.available_resources()["CPU"]

    result = wait_until(assert_available_resources, 1000)
    assert result is True
