from typing import TypeVar, List, Iterable, Tuple, Callable, Any

import ray
from ray import ObjectRef

PartitionID = int
InType = Any
OutType = Any


class InputCombiner:
    def __init__(self):
        self.results = []

    def add(self, data: InType) -> None:
        self.results.append(ray.put(data))

    def finish(self) -> List[ObjectRef]:
        return self.results


def round_robin_partitioner(input_stream: Iterable[InType], num_partitions: int
                            ) -> Iterable[Tuple[PartitionID, InType]]:
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
        output_writer: Callable[[PartitionID, List[ObjectRef]],
                                OutType],
        partitioner: Callable[[Iterable[InType], int], Iterable[
            PartitionID]] = round_robin_partitioner,
        input_combiner: InputCombiner = InputCombiner,
) -> List[OutType]:

    @ray.remote(num_returns=output_num_partitions)
    def shuffle_map(i: PartitionID) -> List[List[ObjectRef]]:
        combiners = [input_combiner() for _ in range(output_num_partitions)]
        for out_i, item in partitioner(input_reader(i), output_num_partitions):
            combiners[out_i].add(item)
        return [c.finish() for c in combiners]

    @ray.remote
    def shuffle_reduce(i: PartitionID,
                       *mapper_outputs: List[List[ObjectRef]]) -> OutType:
        combined_outputs = []
        assert len(mapper_outputs) == input_num_partitions
        for m in mapper_outputs:
            for combiner_output_ref in m:
                combined_outputs.append(combiner_output_ref)
        return output_writer(i, combined_outputs)

    shuffle_map_out = [shuffle_map.remote(i) for i in range(input_num_partitions)]

    shuffle_reduce_out = [
        shuffle_reduce.remote(
            j,
            *[shuffle_map_out[i][j] for i in range(input_num_partitions)])
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


if __name__ == "__main__":
    import numpy as np
    import time

    ray.init()

    partition_size = int(200e6)
    num_partitions = 5
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

    time.sleep(1)
    print(ray.internal.internal_api.memory_summary(stats_only=True))
    print("Shuffled", int(sum(output_sizes) / (1024 * 1024)), "MiB in", delta,
          "seconds")
