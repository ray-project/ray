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

    available_res = ray.global_state.available_resources()
    cluster_res = ray.global_state.cluster_resources()

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

    available_res = ray.global_state.available_resources()
    cluster_res = ray.global_state.cluster_resources()

    assert res_name not in available_res
    assert res_name not in cluster_res


def test_dynamic_res_infeasible_rescheduling(ray_start_regular):
    # This test launches an infeasible task and then creates a resource to make the task feasible.
    # This tests if the infeasible tasks get rescheduled when resources are created at runtime.
    res_name = "test_res"
    res_capacity = 1.0

    @ray.remote
    def set_res(resource_name, resource_capacity):
        ray.experimental.set_resource(resource_name, resource_capacity)

    @ray.remote
    def f():
        return 1

    task = f._remote(
        args=[], resources={res_name: res_capacity})  # This is infeasible
    ray.get(set_res.remote(res_name, res_capacity))  # Now should be feasible

    available_res = ray.global_state.available_resources()
    assert available_res[res_name] == res_capacity

    successful, unsuccessful = ray.wait([task], timeout=1)
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

    target_clientid = ray.global_state.client_table()[1]['ClientID']

    @ray.remote
    def set_res(resource_name, resource_capacity, client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=client_id)

    # Create resource
    ray.get(set_res.remote(res_name, res_capacity, target_clientid))

    # Update resource
    new_capacity = res_capacity + 1
    ray.get(set_res.remote(res_name, new_capacity, target_clientid))

    target_client = next(client for client in ray.global_state.client_table()
                         if client['ClientID'] == target_clientid)
    resources = target_client['Resources']

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

    target_clientid = ray.global_state.client_table()[1]['ClientID']

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    ray.get(set_res.remote(res_name, res_capacity, target_clientid))
    target_client = next(client for client in ray.global_state.client_table()
                         if client['ClientID'] == target_clientid)
    resources = target_client['Resources']

    assert res_name in resources
    assert resources[res_name] == res_capacity


def test_dynamic_res_creation_clientid_multiple(ray_start_cluster):
    # This test creates resources on multiple clients using the clientid specifier
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    target_clientids = [
        client['ClientID'] for client in ray.global_state.client_table()
    ]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    results = []
    for cid in target_clientids:
        results.append(set_res.remote(res_name, res_capacity, cid))
    ray.get(results)

    # Wait for heartbeats to be sent out
    time.sleep(0.1)
    for cid in target_clientids:
        target_client = next(client
                             for client in ray.global_state.client_table()
                             if client['ClientID'] == cid)
        resources = target_client['Resources']
        assert resources[res_name] == res_capacity


def test_dynamic_res_deletion_clientid(ray_start_cluster):
    # This test deletes a resource on a given client id
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 5

    for i in range(num_nodes):
        # Create resource on all nodes, but later we'll delete it from a target node
        cluster.add_node(resources={res_name: res_capacity})

    ray.init(redis_address=cluster.redis_address)

    target_clientid = ray.global_state.client_table()[1]['ClientID']

    # Launch the delete task
    @ray.remote
    def delete_res(resource_name, res_client_id):
        ray.experimental.set_resource(
            resource_name, 0, client_id=res_client_id)

    ray.get(delete_res.remote(res_name, target_clientid))

    target_client = next(client for client in ray.global_state.client_table()
                         if client['ClientID'] == target_clientid)
    resources = target_client['Resources']
    print(ray.global_state.cluster_resources())
    assert res_name not in resources


def test_dynamic_res_creation_scheduler_consistency(ray_start_cluster):
    # This makes sure the resource is actually created and the state is consistent in the scheduler
    # by launching a task which requests the created resource
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 5

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    clientids = [
        client['ClientID'] for client in ray.global_state.client_table()
    ]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    # Create the resource on node1
    target_clientid = clientids[1]
    ray.get(set_res.remote(res_name, res_capacity, target_clientid))

    # Define a task which requires this resource
    @ray.remote(resources={res_name: res_capacity})
    def test_func():
        return 1

    result = test_func.remote()
    successful, unsuccessful = ray.wait([result], timeout=5)
    assert successful  # The task completed


def test_dynamic_res_deletion_scheduler_consistency(ray_start_cluster):
    # This makes sure the resource is actually deleted and the state is consistent in the scheduler
    # by launching an infeasible task which requests the created resource
    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 1.0
    num_nodes = 5
    TIMEOUT_DURATION = 1

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    clientids = [
        client['ClientID'] for client in ray.global_state.client_table()
    ]

    @ray.remote
    def delete_res(resource_name, res_client_id):
        ray.experimental.set_resource(
            resource_name, 0, client_id=res_client_id)

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    # Create the resource on node1
    target_clientid = clientids[1]
    ray.get(set_res.remote(res_name, res_capacity, target_clientid))
    assert ray.global_state.cluster_resources()[res_name] == res_capacity

    # Delete the resource
    ray.get(delete_res.remote(res_name, target_clientid))

    # Define a task which requires this resource. This should not run
    @ray.remote(resources={res_name: res_capacity})
    def test_func():
        return 1

    result = test_func.remote()
    successful, unsuccessful = ray.wait([result], timeout=TIMEOUT_DURATION)
    assert unsuccessful  # The task did not complete because it's infeasible


def test_dynamic_res_concurrent_res_increment(ray_start_cluster):
    # This test makes sure resource capacity is updated (increment) correctly when a task has already acquired some of the resource.

    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 5
    updated_capacity = 10
    num_nodes = 5
    TIMEOUT_DURATION = 1

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    clientids = [
        client['ClientID'] for client in ray.global_state.client_table()
    ]
    target_clientid = clientids[1]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    # Create the resource on node 1
    ray.get(set_res.remote(res_name, res_capacity, target_clientid))
    assert ray.global_state.cluster_resources()[res_name] == res_capacity

    # Task to hold the resource till the driver signals to finish
    @ray.remote
    def wait_func(id_str):
        ray.get(ray.ObjectID(
            id_str))  # To make the task wait till signalled by driver

    @ray.remote
    def test_func():
        return 1

    # Create a object ID to have the task wait on
    WAIT_OBJECT_ID_STR = ('a' * 20).encode("ascii")

    # Launch the task with resource requirement of 1, thus the new available capacity becomes 4
    task = wait_func._remote(
        args=[WAIT_OBJECT_ID_STR], resources={res_name: 1})
    # Sleep to make sure the wait_func is launched before updating resource
    time.sleep(0.2)

    # Update the resource capacity
    ray.get(set_res.remote(res_name, updated_capacity, target_clientid))

    # Signal task to complete
    ray.worker.global_worker.put_object(ray.ObjectID(WAIT_OBJECT_ID_STR), 1)
    ray.get(task)

    # Check if scheduler state is consistent by launching a task requiring updated capacity
    task_2 = test_func._remote(args=[], resources={res_name: updated_capacity})
    successful, unsuccessful = ray.wait([task_2], timeout=TIMEOUT_DURATION)
    assert successful  # The task completed

    # Check if scheduler state is consistent by launching a task requiring updated capacity + 1. This should not execute
    task_3 = test_func._remote(
        args=[], resources={res_name: updated_capacity + 1
                            })  # This should be infeasible
    successful, unsuccessful = ray.wait([task_3], timeout=TIMEOUT_DURATION)
    assert unsuccessful  # The task did not complete because it's infeasible
    assert ray.global_state.available_resources()[res_name] == updated_capacity


def test_dynamic_res_concurrent_res_decrement(ray_start_cluster):
    # This test makes sure resource capacity is updated (decremented) correctly when a task has already acquired some of the resource.

    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 5
    updated_capacity = 2
    num_nodes = 5
    TIMEOUT_DURATION = 1

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    clientids = [
        client['ClientID'] for client in ray.global_state.client_table()
    ]
    target_clientid = clientids[1]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    # Create the resource on node 1
    ray.get(set_res.remote(res_name, res_capacity, target_clientid))
    assert ray.global_state.cluster_resources()[res_name] == res_capacity

    @ray.remote
    def wait_func(id_str):
        ray.get(ray.ObjectID(
            id_str))  # To make the task wait till signalled by driver

    @ray.remote
    def test_func():
        return 1

    # Create a object ID to have the task wait on
    WAIT_OBJECT_ID_STR = ('a' * 20).encode("ascii")

    # Launch the task with resource requirement of 4, thus the new available capacity becomes 1
    task = wait_func._remote(
        args=[WAIT_OBJECT_ID_STR], resources={res_name: 4})
    time.sleep(
        0.2
    )  # Sleep to make sure the wait_func is launched before updating resource

    # Decrease the resource capacity
    ray.get(set_res.remote(res_name, updated_capacity, target_clientid))

    # Signal task to complete
    ray.worker.global_worker.put_object(ray.ObjectID(WAIT_OBJECT_ID_STR), 1)
    ray.get(task)

    # Check if scheduler state is consistent by launching a task requiring updated capacity
    task_2 = test_func._remote(args=[], resources={res_name: updated_capacity})
    successful, unsuccessful = ray.wait([task_2], timeout=TIMEOUT_DURATION)
    assert successful  # The task completed

    # Check if scheduler state is consistent by launching a task requiring updated capacity + 1. This should not execute
    task_3 = test_func._remote(
        args=[], resources={res_name: updated_capacity + 1
                            })  # This should be infeasible
    successful, unsuccessful = ray.wait([task_3], timeout=TIMEOUT_DURATION)
    assert unsuccessful  # The task did not complete because it's infeasible
    assert ray.global_state.available_resources()[res_name] == updated_capacity


def test_dynamic_res_concurrent_res_delete(ray_start_cluster):
    # This test makes sure resource gets deleted correctly when a task has already acquired the resource

    cluster = ray_start_cluster

    res_name = "test_res"
    res_capacity = 5
    num_nodes = 5
    TIMEOUT_DURATION = 1

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    clientids = [
        client['ClientID'] for client in ray.global_state.client_table()
    ]
    target_clientid = clientids[1]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    @ray.remote
    def delete_res(resource_name, res_client_id):
        ray.experimental.set_resource(
            resource_name, 0, client_id=res_client_id)

    # Create the resource on node 1
    ray.get(set_res.remote(res_name, res_capacity, target_clientid))
    assert ray.global_state.cluster_resources()[res_name] == res_capacity

    # Task to hold the resource till the driver signals to finish
    @ray.remote
    def wait_func(id_str):
        ray.get(ray.ObjectID(
            id_str))  # To make the task wait till signalled by driver

    @ray.remote
    def test_func():
        return 1

    # Create a object ID to have the task wait on
    WAIT_OBJECT_ID_STR = ('a' * 20).encode("ascii")

    # Launch the task with resource requirement of 1, thus the new available capacity becomes 4
    task = wait_func._remote(
        args=[WAIT_OBJECT_ID_STR], resources={res_name: 1})
    time.sleep(
        0.2
    )  # Sleep to make sure the wait_func is launched before updating resource

    # Delete the resource
    ray.get(delete_res.remote(res_name, target_clientid))

    # Signal task to complete
    ray.worker.global_worker.put_object(ray.ObjectID(WAIT_OBJECT_ID_STR), 1)
    ray.get(task)

    # Check if scheduler state is consistent by launching a task requiring the deleted resource  This should not execute
    task_2 = test_func._remote(
        args=[], resources={res_name: 1})  # This should be infeasible
    successful, unsuccessful = ray.wait([task_2], timeout=TIMEOUT_DURATION)
    assert unsuccessful  # The task did not complete because it's infeasible
    assert res_name not in ray.global_state.available_resources()


def test_dynamic_res_creation_stress(ray_start_cluster):
    # This stress tests creates many resources simultaneously on the same client and then checks if the final state is consistent

    cluster = ray_start_cluster

    res_capacity = 1
    num_nodes = 5
    NUM_RES_TO_CREATE = 500

    for i in range(num_nodes):
        cluster.add_node()

    ray.init(redis_address=cluster.redis_address)

    clientids = [
        client['ClientID'] for client in ray.global_state.client_table()
    ]
    target_clientid = clientids[1]

    @ray.remote
    def set_res(resource_name, resource_capacity, res_client_id):
        ray.experimental.set_resource(
            resource_name, resource_capacity, client_id=res_client_id)

    @ray.remote
    def delete_res(resource_name, res_client_id):
        ray.experimental.set_resource(
            resource_name, 0, client_id=res_client_id)

    results = [
        set_res.remote(str(i), res_capacity, target_clientid)
        for i in range(0, NUM_RES_TO_CREATE)
    ]
    ray.get(results)
    time.sleep(0.1)  # Wait for heartbeats to propagate
    resources = ray.global_state.cluster_resources()
    for i in range(0, NUM_RES_TO_CREATE):
        assert str(i) in resources
