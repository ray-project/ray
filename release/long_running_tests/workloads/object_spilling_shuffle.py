import time
import json
import ray
import numpy as np
from typing import List

from ray.cluster_utils import Cluster

num_nodes = 4
object_store_size = 100 * 1024 * 1024  # 100 MB.
partition_size = int(10e6)  # 20 MB
num_partitions = 100
rows_per_partition = partition_size // (8 * 2)

c = Cluster()
c.add_node(
    num_cpus=4,
    object_store_memory=object_store_size,
    _system_config={
        "automatic_object_spilling_enabled": True,
        "max_io_workers": 2,
        "object_spilling_config": json.dumps(
            {
                "type": "filesystem",
                "params": {
                    "directory_path": "/tmp/spill"
                }
            },
            separators=(",", ":"))
    })
# Add fake 4 nodes cluster.
for _ in range(num_nodes - 1):  # subtract a head node.
    c.add_node(num_cpus=4, object_store_memory=object_store_size)

c.wait_for_nodes()
ray.init(address=c.address)


@ray.remote
class Counter:
    def __init__(self):
        self.num_map = 0
        self.num_reduce = 0

    def inc(self):
        self.num_map += 1
        print("Num map tasks finished", self.num_map)

    def inc2(self):
        self.num_reduce += 1
        print("Num reduce tasks finished", self.num_reduce)

    def finish(self):
        pass


# object store peak memory: O(partition size / num partitions)
# heap memory: O(partition size / num partitions)
@ray.remote(num_returns=num_partitions)
def shuffle_map_streaming(
        i, counter_handle=None) -> List["ObjectRef[np.ndarray]"]:
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
    for out in shuffle_map_out:
        ray.get(out)

    # Start reducing
    shuffle_reduce_out = [
        shuffle_reduce.remote(
            *[shuffle_map_out[i][j] for i in range(num_partitions)],
            counter_handle=counter) for j in range(num_partitions)
    ]

    print("start shuffle.")
    total_rows = 0
    ready, unready = ray.wait(shuffle_reduce_out)
    while unready:
        ready, unready = ray.wait(unready)
        for output in ready:
            total_rows += ray.get(output).shape[0]
    delta = time.time() - start

    ray.get(counter.finish.remote())
    return total_rows, delta


iteration = 1
delta_sum = 0
total_bytes = None

while True:
    print(f"Start the iteration, {iteration}")
    total_rows, delta = run_shuffle()
    print(f"Iteration {iteration} done.")
    print("Shuffled", total_rows * 8 * 2, "bytes in", delta, "seconds")
    delta_sum += delta
    total_bytes = total_rows
    assert total_bytes == total_rows
    iteration += 1

assert total_bytes is not None
print(f"Long running shuffle done. Please check if memory usage is steady. "
      f"The average shuffling that shuffles {total_bytes / 1024 / 1024} MB"
      f"took {delta_sum / iteration} "
      f"for {iteration} number of itertaions.")
