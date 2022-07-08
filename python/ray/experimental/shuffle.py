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

import time
from typing import Any, Callable, Iterable, List, Tuple, Union

import ray
from ray import ObjectRef
from ray.cluster_utils import Cluster

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


class ObjectStoreWriterNonStreaming(ObjectStoreWriter):
    def __init__(self):
        self.results = []

    def add(self, item: InType) -> None:
        self.results.append(item)

    def finish(self) -> List[Any]:
        return self.results


def round_robin_partitioner(
    input_stream: Iterable[InType], num_partitions: int
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


@ray.remote
class _StatusTracker:
    def __init__(self):
        self.num_map = 0
        self.num_reduce = 0
        self.map_refs = []
        self.reduce_refs = []

    def register_objectrefs(self, map_refs, reduce_refs):
        self.map_refs = map_refs
        self.reduce_refs = reduce_refs

    def get_progress(self):
        if self.map_refs:
            ready, self.map_refs = ray.wait(
                self.map_refs,
                timeout=1,
                num_returns=len(self.map_refs),
                fetch_local=False,
            )
            self.num_map += len(ready)
        elif self.reduce_refs:
            ready, self.reduce_refs = ray.wait(
                self.reduce_refs,
                timeout=1,
                num_returns=len(self.reduce_refs),
                fetch_local=False,
            )
            self.num_reduce += len(ready)
        return self.num_map, self.num_reduce


def render_progress_bar(tracker, input_num_partitions, output_num_partitions):
    from tqdm import tqdm

    num_map = 0
    num_reduce = 0
    map_bar = tqdm(total=input_num_partitions, position=0)
    map_bar.set_description("Map Progress.")
    reduce_bar = tqdm(total=output_num_partitions, position=1)
    reduce_bar.set_description("Reduce Progress.")

    while num_map < input_num_partitions or num_reduce < output_num_partitions:
        new_num_map, new_num_reduce = ray.get(tracker.get_progress.remote())
        map_bar.update(new_num_map - num_map)
        reduce_bar.update(new_num_reduce - num_reduce)
        num_map = new_num_map
        num_reduce = new_num_reduce
        time.sleep(0.1)
    map_bar.close()
    reduce_bar.close()


def simple_shuffle(
    *,
    input_reader: Callable[[PartitionID], Iterable[InType]],
    input_num_partitions: int,
    output_num_partitions: int,
    output_writer: Callable[[PartitionID, List[Union[ObjectRef, Any]]], OutType],
    partitioner: Callable[
        [Iterable[InType], int], Iterable[PartitionID]
    ] = round_robin_partitioner,
    object_store_writer: ObjectStoreWriter = ObjectStoreWriter,
    tracker: _StatusTracker = None,
    streaming: bool = True,
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
        tracker: Tracker actor that is used to display the progress bar.
        streaming: Whether or not if the shuffle will be streaming.

    Returns:
        List of outputs from the output writers.
    """

    @ray.remote(num_returns=output_num_partitions)
    def shuffle_map(i: PartitionID) -> List[List[Union[Any, ObjectRef]]]:
        writers = [object_store_writer() for _ in range(output_num_partitions)]
        for out_i, item in partitioner(input_reader(i), output_num_partitions):
            writers[out_i].add(item)
        return [c.finish() for c in writers]

    @ray.remote
    def shuffle_reduce(
        i: PartitionID, *mapper_outputs: List[List[Union[Any, ObjectRef]]]
    ) -> OutType:
        input_objects = []
        assert len(mapper_outputs) == input_num_partitions
        for obj_refs in mapper_outputs:
            for obj_ref in obj_refs:
                input_objects.append(obj_ref)
        return output_writer(i, input_objects)

    shuffle_map_out = [shuffle_map.remote(i) for i in range(input_num_partitions)]

    shuffle_reduce_out = [
        shuffle_reduce.remote(
            j, *[shuffle_map_out[i][j] for i in range(input_num_partitions)]
        )
        for j in range(output_num_partitions)
    ]

    if tracker:
        tracker.register_objectrefs.remote(
            [map_out[0] for map_out in shuffle_map_out], shuffle_reduce_out
        )
        render_progress_bar(tracker, input_num_partitions, output_num_partitions)

    return ray.get(shuffle_reduce_out)


def build_cluster(num_nodes, num_cpus, object_store_memory):
    cluster = Cluster()
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus, object_store_memory=object_store_memory)
    cluster.wait_for_nodes()
    return cluster


def run(
    ray_address=None,
    object_store_memory=1e9,
    num_partitions=5,
    partition_size=200e6,
    num_nodes=None,
    num_cpus=8,
    no_streaming=False,
    use_wait=False,
    tracker=None,
):
    import time

    import numpy as np

    is_multi_node = num_nodes
    if ray_address:
        print("Connecting to a existing cluster...")
        ray.init(address=ray_address, ignore_reinit_error=True)
    elif is_multi_node:
        print("Emulating a cluster...")
        print(f"Num nodes: {num_nodes}")
        print(f"Num CPU per node: {num_cpus}")
        print(f"Object store memory per node: {object_store_memory}")
        cluster = build_cluster(num_nodes, num_cpus, object_store_memory)
        ray.init(address=cluster.address)
    else:
        print("Start a new cluster...")
        ray.init(num_cpus=num_cpus, object_store_memory=object_store_memory)

    partition_size = int(partition_size)
    num_partitions = num_partitions
    rows_per_partition = partition_size // (8 * 2)
    if tracker is None:
        tracker = _StatusTracker.remote()
    use_wait = use_wait

    def input_reader(i: PartitionID) -> Iterable[InType]:
        for _ in range(num_partitions):
            yield np.ones((rows_per_partition // num_partitions, 2), dtype=np.int64)

    def output_writer(i: PartitionID, shuffle_inputs: List[ObjectRef]) -> OutType:
        total = 0
        if not use_wait:
            for obj_ref in shuffle_inputs:
                arr = ray.get(obj_ref)
                total += arr.size * arr.itemsize
        else:
            while shuffle_inputs:
                [ready], shuffle_inputs = ray.wait(shuffle_inputs, num_returns=1)
                arr = ray.get(ready)
                total += arr.size * arr.itemsize

        return total

    def output_writer_non_streaming(
        i: PartitionID, shuffle_inputs: List[Any]
    ) -> OutType:
        total = 0
        for arr in shuffle_inputs:
            total += arr.size * arr.itemsize
        return total

    if no_streaming:
        output_writer_callable = output_writer_non_streaming
        object_store_writer = ObjectStoreWriterNonStreaming
    else:
        object_store_writer = ObjectStoreWriter
        output_writer_callable = output_writer

    start = time.time()
    output_sizes = simple_shuffle(
        input_reader=input_reader,
        input_num_partitions=num_partitions,
        output_num_partitions=num_partitions,
        output_writer=output_writer_callable,
        object_store_writer=object_store_writer,
        tracker=tracker,
    )
    delta = time.time() - start

    time.sleep(0.5)
    print()

    summary = None
    for i in range(5):
        try:
            summary = ray._private.internal_api.memory_summary(stats_only=True)
        except Exception:
            time.sleep(1)
            pass
        if summary:
            break
    print(summary)
    print()
    print(
        "Shuffled", int(sum(output_sizes) / (1024 * 1024)), "MiB in", delta, "seconds"
    )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--ray-address", type=str, default=None)
    parser.add_argument("--object-store-memory", type=float, default=1e9)
    parser.add_argument("--num-partitions", type=int, default=5)
    parser.add_argument("--partition-size", type=float, default=200e6)
    parser.add_argument("--num-nodes", type=int, default=None)
    parser.add_argument("--num-cpus", type=int, default=8)
    parser.add_argument("--no-streaming", action="store_true", default=False)
    parser.add_argument("--use-wait", action="store_true", default=False)
    args = parser.parse_args()

    run(
        ray_address=args.ray_address,
        object_store_memory=args.object_store_memory,
        num_partitions=args.num_partitions,
        partition_size=args.partition_size,
        num_nodes=args.num_nodes,
        num_cpus=args.num_cpus,
        no_streaming=args.no_streaming,
        use_wait=args.use_wait,
    )


if __name__ == "__main__":
    main()
