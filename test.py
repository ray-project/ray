import numpy as np
import ray
from ray.cluster_utils import Cluster


def build_cluster(num_nodes=2, num_cpus=4, object_store_memory=1 * 1024 * 1024 * 1024):
    cluster = Cluster()
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=num_cpus,
            object_store_memory=object_store_memory,
            resources={f"node_{i}": 1},
        )
    cluster.wait_for_nodes()
    return cluster


@ray.remote(resources={"node_0": 1e-3})
def produce(size=100 * 1024 * 1024):
    print("produce", size)
    return np.zeros(size, dtype=np.uint8)


@ray.remote(resources={"node_1": 1e-3})
def consume(arr):
    print("consume", arr.size)
    return arr.size


def main():
    cluster = build_cluster()
    cluster.connect()
    print(ray.available_resources())
    x = produce.remote()
    y = consume.remote(x)
    print(ray.get(y))


if __name__ == "__main__":
    main()
