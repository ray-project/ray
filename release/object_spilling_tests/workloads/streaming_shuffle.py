import time
import json
import ray
import numpy as np
from typing import List
import time

num_nodes = 4
partition_size = int(100e6)  # 100 MB
num_partitions = 1000
rows_per_partition = partition_size // (8 * 2)


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


max_iteration = 10
deltas = []
total_bytes = None


for i in range(1, max_iteration):
    ray.init(address='auto')
    print(f"Start the iteration, {i}")
    total_rows, delta = run_shuffle()
    print(f"Iteration {i} done.")
    print("Shuffled", total_rows * 8 * 2, "bytes in", delta, "seconds")
    deltas.append(delta)
    # Have some gap before starting the next trial.
    ray.shutdown()
    print(f"Mean over {i} trials: {np.mean(deltas)} += {np.std(deltas)}")
    time.sleep(5)

print(f"Mean over {max_iteration} trials: {np.mean(deltas)} += {np.std(deltas)}")
