import argparse
import logging
import numpy as np
import time

import ray
from ray.tests.cluster_utils import Cluster

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="Parse arguments for running the performance tests.")
parser.add_argument(
    "--num-nodes",
    required=True,
    type=int,
    help="The number of nodes to simulate in the cluster.")
parser.add_argument(
    "--skip-object-store-warmup",
    default=False,
    action="store_true",
    help="True if the object store should not be warmed up. This could cause "
    "the benchmarks to appear slower than usual.")
parser.add_argument(
    "--address",
    required=False,
    type=str,
    help="The address of the cluster to connect to. If this is ommitted, then "
    "a cluster will be started locally (on a single machine).")


def start_local_cluster(num_nodes, object_store_memory):
    """Start a local Ray cluster.

    The ith node in the cluster will have a resource named "i".

    Args:
        num_nodes: The number of nodes to start in the cluster.

    Returns:
        The cluster object.
    """
    num_redis_shards = 2
    redis_max_memory = 10**8

    cluster = Cluster()
    for i in range(num_nodes):
        cluster.add_node(
            redis_port=6379 if i == 0 else None,
            num_redis_shards=num_redis_shards if i == 0 else None,
            num_cpus=8 if i == 0 else 2,
            num_gpus=0,
            resources={str(i): 500},
            object_store_memory=object_store_memory,
            redis_max_memory=redis_max_memory)
    ray.init(address=cluster.address)

    return cluster


def wait_for_and_check_cluster_configuration(num_nodes):
    """Check that the cluster's custom resources are properly configured.

    The ith node should have a resource labeled 'i' with quantity 500.

    Args:
        num_nodes: The number of nodes that we expect to be in the cluster.

    Raises:
        RuntimeError: This exception is raised if the cluster is not configured
            properly for this test.
    """
    logger.warning("Waiting for cluster to have %s nodes.", num_nodes)
    while True:
        nodes = ray.nodes()
        if len(nodes) == num_nodes:
            break
        if len(nodes) > num_nodes:
            raise RuntimeError(
                "The cluster has %s nodes, but it should "
                "only have %s.", len(nodes), num_nodes)
    if not ([set(node["Resources"].keys())
             for node in ray.nodes()] == [{str(i), "CPU"}
                                          for i in range(num_nodes)]):
        raise RuntimeError(
            "The ith node in the cluster should have a "
            "custom resource called 'i' with quantity "
            "500. The nodes are\n%s", ray.nodes())
    if not ([[
            resource_quantity
            for resource_name, resource_quantity in node["Resources"].items()
            if resource_name != "CPU"
    ] for node in ray.nodes()] == num_nodes * [[500.0]]):
        raise RuntimeError(
            "The ith node in the cluster should have a "
            "custom resource called 'i' with quantity "
            "500. The nodes are\n%s", ray.nodes())
    for node in ray.nodes():
        if ("0" in node["Resources"] and node["ObjectStoreSocketName"] !=
                ray.worker.global_worker.plasma_client.store_socket_name):
            raise RuntimeError("The node that this driver is connected to "
                               "must have a custom resource labeled '0'.")


@ray.remote
def create_array(size):
    return np.zeros(shape=size, dtype=np.uint8)


@ray.remote
def no_op(*values):
    # The reason that this function takes *values is so that we can pass in
    # an arbitrary number of object refs to create task dependencies.
    return 1


@ray.remote
class Actor(object):
    def ping(self, *values):
        pass


def warm_up_cluster(num_nodes, object_store_memory):
    """Warm up the cluster.

    This will allocate enough objects in each object store to cause eviction
    because the first time a driver or worker touches a region of memory in the
    object store, it may be slower.

    Note that remote functions are exported lazily, so the first invocation of
    a given remote function will be slower.
    """
    logger.warning("Warming up the object store.")
    size = object_store_memory * 2 // 5
    num_objects = 2
    while size > 0:
        object_refs = []
        for i in range(num_nodes):
            for _ in range(num_objects):
                object_refs += [
                    create_array._remote(args=[size], resources={str(i): 1})
                ]
        size = size // 2
        num_objects = min(num_objects * 2, 1000)
    for object_ref in object_refs:
        ray.get(object_ref)
    logger.warning("Finished warming up the object store.")

    # Invoke all of the remote functions once so that the definitions are
    # broadcast to the workers.
    ray.get(no_op.remote())
    ray.get(Actor.remote().ping.remote())


def run_multiple_trials(f, num_trials):
    durations = []
    for _ in range(num_trials):
        start = time.time()
        f()
        durations.append(time.time() - start)
    return durations


def test_tasks(num_nodes):
    def one_thousand_serial_tasks_local_node():
        for _ in range(1000):
            ray.get(no_op._remote(resources={"0": 1}))

    durations = run_multiple_trials(one_thousand_serial_tasks_local_node, 10)
    logger.warning(
        "one_thousand_serial_tasks_local_node \n"
        "    min:  %.2gs\n"
        "    mean: %.2gs\n"
        "    std:  %.2gs", np.min(durations), np.mean(durations),
        np.std(durations))

    def one_thousand_serial_tasks_remote_node():
        for _ in range(1000):
            ray.get(no_op._remote(resources={"1": 1}))

    durations = run_multiple_trials(one_thousand_serial_tasks_remote_node, 10)
    logger.warning(
        "one_thousand_serial_tasks_remote_node \n"
        "    min:  %.2gs\n"
        "    mean: %.2gs\n"
        "    std:  %.2gs", np.min(durations), np.mean(durations),
        np.std(durations))

    def ten_thousand_parallel_tasks_local():
        ray.get([no_op._remote(resources={"0": 1}) for _ in range(10000)])

    durations = run_multiple_trials(ten_thousand_parallel_tasks_local, 5)
    logger.warning(
        "ten_thousand_parallel_tasks_local \n"
        "    min:  %.2gs\n"
        "    mean: %.2gs\n"
        "    std:  %.2gs", np.min(durations), np.mean(durations),
        np.std(durations))

    def ten_thousand_parallel_tasks_load_balanced():
        ray.get([
            no_op._remote(resources={str(i % num_nodes): 1})
            for i in range(10000)
        ])

    durations = run_multiple_trials(ten_thousand_parallel_tasks_load_balanced,
                                    5)
    logger.warning(
        "ten_thousand_parallel_tasks_load_balanced \n"
        "    min:  %.2gs\n"
        "    mean: %.2gs\n"
        "    std:  %.2gs", np.min(durations), np.mean(durations),
        np.std(durations))


if __name__ == "__main__":
    args = parser.parse_args()
    num_nodes = args.num_nodes

    object_store_memory = 10**8

    # Configure the cluster or check that it is properly configured.

    if num_nodes < 2:
        raise ValueError("The --num-nodes argument must be at least 2.")

    if args.address:
        ray.init(address=args.address)
        wait_for_and_check_cluster_configuration(num_nodes)
        logger.warning(
            "Running performance benchmarks on the cluster with "
            "address %s.", args.address)
    else:
        logger.warning(
            "Running performance benchmarks on a simulated cluster "
            "of %s nodes.", num_nodes)

        cluster = start_local_cluster(num_nodes, object_store_memory)

    if not args.skip_object_store_warmup:
        warm_up_cluster(num_nodes, object_store_memory)

    # Run the benchmarks.

    test_tasks(num_nodes)

    # TODO(rkn): Test actors, test object transfers, test tasks with many
    # dependencies.
