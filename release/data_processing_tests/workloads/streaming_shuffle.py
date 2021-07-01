import time
import json
import ray
import numpy as np
from typing import List

from ray import ObjectRef
from tqdm import tqdm

from ray.cluster_utils import Cluster

num_nodes = 4
num_cpus = 4
partition_size = int(500e6)  # 500MB
# Number of map & reduce tasks == num_partitions.
# Number of objects == num_partitions ^ 2.
num_partitions = 200
# There are two int64 per row, so we divide by 8 * 2 bytes.
rows_per_partition = partition_size // (8 * 2)
object_store_size = 20 * 1024 * 1024 * 1024  # 20G

system_config = {
    "automatic_object_spilling_enabled": True,
    "max_io_workers": 1,
    "object_spilling_config": json.dumps(
        {
            "type": "filesystem",
            "params": {
                "directory_path": "/tmp/spill"
            }
        },
        separators=(",", ":"))
}


def display_spilling_info(address):
    state = ray.state.GlobalState()
    state._initialize_global_state(address,
                                   ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    raylet = state.node_table()[0]
    memory_summary = ray.internal.internal_api.memory_summary(
        f"{raylet['NodeManagerAddress']}:{raylet['NodeManagerPort']}")
    for line in memory_summary.split("\n"):
        if "Spilled" in line:
            print(line)
        if "Restored" in line:
            print(line)
    print("\n\n")


@ray.remote
class Counter:
    def __init__(self):
        self.num_map = 0
        self.num_reduce = 0

    def inc(self):
        self.num_map += 1
        # print("Num map tasks finished", self.num_map)

    def inc2(self):
        self.num_reduce += 1
        # print("Num reduce tasks finished", self.num_reduce)

    def finish(self):
        pass


# object store peak memory: O(partition size / num partitions)
# heap memory: O(partition size / num partitions)
@ray.remote(num_returns=num_partitions)
def shuffle_map_streaming(i,
                          counter_handle=None) -> List[ObjectRef[np.ndarray]]:
    outputs = [
        ray.put(
            np.ones((rows_per_partition // num_partitions, 2), dtype=np.int64))
        for _ in range(num_partitions)
    ]
    counter_handle.inc.remote()
    return outputs


# object store peak memory: O(partition size / num partitions)
# heap memory: O(partition size) -- TODO can be reduced too
@ray.remote
def shuffle_reduce_streaming(*inputs, counter_handle=None) -> np.ndarray:
    out = None
    for chunk in inputs:
        if out is None:
            out = ray.get(chunk)
        else:
            out = np.concatenate([out, ray.get(chunk)])
    counter_handle.inc2.remote()
    return out


shuffle_map = shuffle_map_streaming
shuffle_reduce = shuffle_reduce_streaming


def run_shuffle():
    counter = Counter.remote()
    start = time.time()
    print("start map")
    shuffle_map_out = [
        shuffle_map.remote(i, counter_handle=counter)
        for i in range(num_partitions)
    ]
    # wait until all map is done before reduce phase.
    for out in tqdm(shuffle_map_out):
        ray.get(out)

    # Start reducing
    shuffle_reduce_out = [
        shuffle_reduce.remote(
            *[shuffle_map_out[i][j] for i in range(num_partitions)],
            counter_handle=counter) for j in range(num_partitions)
    ]

    print("start shuffle.")
    pbar = tqdm(total=num_partitions)
    total_rows = 0
    ready, unready = ray.wait(shuffle_reduce_out)
    while unready:
        ready, unready = ray.wait(unready)
        for output in ready:
            pbar.update(1)
            total_rows += ray.get(output).shape[0]
    delta = time.time() - start

    ray.get(counter.finish.remote())
    print("Shuffled", total_rows * 8 * 2, "bytes in", delta,
          "seconds in a single node.\n")


def run_single_node():
    address = ray.init(
        num_cpus=num_cpus * num_nodes,
        object_store_memory=object_store_size,
        _system_config=system_config)

    # Run shuffle.
    print(
        "\n\nTest streaming shuffle with a single node.\n"
        f"Shuffle size: {partition_size * num_partitions / 1024 / 1024 / 1024}"
        "GB")
    run_shuffle()
    time.sleep(5)
    display_spilling_info(address["redis_address"])
    ray.shutdown()
    time.sleep(5)


def run_multi_nodes():
    c = Cluster()
    c.add_node(
        num_cpus=4,
        object_store_memory=object_store_size,
        _system_config=system_config)
    ray.init(address=c.address)
    for _ in range(num_nodes - 1):  # subtract a head node.
        c.add_node(num_cpus=4, object_store_memory=object_store_size)
    c.wait_for_nodes()

    # Run shuffle.
    print(
        f"\n\nTest streaming shuffle with {num_nodes} nodes.\n"
        f"Shuffle size: {partition_size * num_partitions / 1024 / 1024 / 1024}"
        "GB")
    run_shuffle()
    time.sleep(5)
    display_spilling_info(c.address)
    ray.shutdown()
    c.shutdown()
    time.sleep(5)


run_single_node()
run_multi_nodes()
