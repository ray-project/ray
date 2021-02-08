"""A simple distributed shuffle implementation in Ray.

This utility provides a `simple_shuffle` function that can be used to
redistribute M input partitions into N output partitions. It does this with
a single wave of shuffle map tasks followed by a single wave of shuffle reduce
tasks. Each shuffle map task generates O(N) output objects, and each shuffle
reduce task consumes O(M) input objects, for a total of O(N*M) objects.

To try an example 10GB shuffle, run:

    $ python -m ray.experimental.shuffle \
        --num-partitions=50 --partition-size=200e6 \
        --object-store-memory=1e9

This will print out some statistics on the shuffle execution such as:

    --- Aggregate object store stats across all nodes ---
    Plasma memory usage 0 MiB, 0 objects, 0.0% full
    Spilled 9487 MiB, 2487 objects, avg write throughput 1023 MiB/s
    Restored 9487 MiB, 2487 objects, avg read throughput 1358 MiB/s
    Objects consumed by Ray tasks: 9537 MiB.

    Shuffled 9536 MiB in 16.579771757125854 seconds
"""

from typing import List, Iterable, Tuple, Callable, Any

import ray
from ray import ObjectRef

# TODO(ekl) why doesn't TypeVar() deserialize properly in Ray?
# The type produced by the input reader function.
InType = Any
# The type produced by the output writer function.
OutType = Any
# Integer identifying the partition number.
PartitionID = int


class ObjectStoreWriter:
    """This class is used to stream shuffle map outputs to the object store.

    It can be subclassed to optimize writing (e.g., batching together small
    records into larger objects). This will be performance critical if your
    input records are small (the example shuffle uses very large records, so
    the naive strategy works well).
    """

    def __init__(self):
        self.results = []

    def add(self, item: InType) -> None:
        """Queue a single item to be written to the object store.

        This base implementation immediately writes each given item to the
        object store as a standalone object.
        """
        self.results.append(ray.put(item))

    def finish(self) -> List[ObjectRef]:
        """Return list of object refs representing written items."""
        return self.results


def round_robin_partitioner(input_stream: Iterable[InType], num_partitions: int
                            ) -> Iterable[Tuple[PartitionID, InType]]:
    """Round robin partitions items from the input reader.

    You can write custom partitioning functions for your use case.

    Args:
        input_stream: Iterator over items from the input reader.
        num_partitions: Number of output partitions.

    Yields:
        Tuples of (partition id, input item).
    """
    i = 0
    for item in input_stream:
        yield (i, item)
        i += 1
        i %= num_partitions


def simple_shuffle(
        *,
        input_reader: Callable[[PartitionID], Iterable[InType]],
        input_num_partitions: int,
        output_num_partitions: int,
        output_writer: Callable[[PartitionID, List[ObjectRef]], OutType],
        partitioner: Callable[[Iterable[InType], int], Iterable[
            PartitionID]] = round_robin_partitioner,
        object_store_writer: ObjectStoreWriter = ObjectStoreWriter,
) -> List[OutType]:
    """Simple distributed shuffle in Ray.

    Args:
        input_reader: Function that generates the input items for a
            partition (e.g., data records).
        input_num_partitions: The number of input partitions.
        output_num_partitions: The desired number of output partitions.
        output_writer: Function that consumes a iterator of items for a
            given output partition. It returns a single value that will be
            collected across all output partitions.
        partitioner: Partitioning function to use. Defaults to round-robin
            partitioning of input items.
        object_store_writer: Class used to write input items to the
            object store in an efficient way. Defaults to a naive
            implementation that writes each input record as one object.

    Returns:
        List of outputs from the output writers.
    """

    @ray.remote(num_returns=output_num_partitions)
    def shuffle_map(i: PartitionID) -> List[List[ObjectRef]]:
        writers = [object_store_writer() for _ in range(output_num_partitions)]
        for out_i, item in partitioner(input_reader(i), output_num_partitions):
            writers[out_i].add(item)
        return [c.finish() for c in writers]

    @ray.remote
    def shuffle_reduce(i: PartitionID,
                       *mapper_outputs: List[List[ObjectRef]]) -> OutType:
        input_objects = []
        assert len(mapper_outputs) == input_num_partitions
        for obj_refs in mapper_outputs:
            for obj_ref in obj_refs:
                input_objects.append(obj_ref)
        return output_writer(i, input_objects)

    shuffle_map_out = [
        shuffle_map.remote(i) for i in range(input_num_partitions)
    ]

    shuffle_reduce_out = [
        shuffle_reduce.remote(
            j, *[shuffle_map_out[i][j] for i in range(input_num_partitions)])
        for j in range(output_num_partitions)
    ]

    return ray.get(shuffle_reduce_out)


@ray.remote
class _StatusTracker:
    def __init__(self):
        self.num_map = 0
        self.num_reduce = 0

    def inc(self):
        self.num_map += 1
        print("Num map tasks finished", self.num_map)

    def inc2(self):
        self.num_reduce += 1
        print("Num reduce tasks finished", self.num_reduce)


def main():
    import argparse
    import numpy as np
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument("--ray-address", type=str, default=None)
    parser.add_argument("--object-store-memory", type=float, default=1e9)
    parser.add_argument("--num-partitions", type=int, default=5)
    parser.add_argument("--partition-size", type=float, default=200e6)
    args = parser.parse_args()

    ray.init(
        address=args.ray_address, object_store_memory=args.object_store_memory)

    partition_size = int(args.partition_size)
    num_partitions = args.num_partitions
    rows_per_partition = partition_size // (8 * 2)
    tracker = _StatusTracker.remote()

    def input_reader(i: PartitionID) -> Iterable[InType]:
        for _ in range(num_partitions):
            yield np.ones(
                (rows_per_partition // num_partitions, 2), dtype=np.int64)
        tracker.inc.remote()

    def output_writer(i: PartitionID,
                      shuffle_inputs: List[ObjectRef]) -> OutType:
        total = 0
        # TODO(ekl) using ray.wait can be more efficient for pipelining.
        for obj_ref in shuffle_inputs:
            arr = ray.get(obj_ref)
            total += arr.size * arr.itemsize
        tracker.inc2.remote()
        return total

    start = time.time()
    output_sizes = simple_shuffle(
        input_reader=input_reader,
        input_num_partitions=num_partitions,
        output_num_partitions=num_partitions,
        output_writer=output_writer)
    delta = time.time() - start

    time.sleep(.5)
    print()
    print(ray.internal.internal_api.memory_summary(stats_only=True))
    print()
    print("Shuffled", int(sum(output_sizes) / (1024 * 1024)), "MiB in", delta,
          "seconds")


if __name__ == "__main__":
    main()
