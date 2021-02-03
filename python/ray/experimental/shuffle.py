from typing import TypeVar, List, Iterable, Tuple, Callable

import ray
from ray import ObjectRef

PartitionID = int
InType = TypeVar("InType")
OutType = TypeVar("OutType")


class InputCombiner:
    def __init__(self):
        self.results = []

    def add(self, data: InType) -> None:
        self.results.append(ray.put(data))

    def finish(self) -> List[ObjectRef[InType]]:
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
        output_writer: Callable[[PartitionID, List[ObjectRef[InType]]],
                                OutType],
        partitioner: Callable[[Iterable[InType], int], Iterable[
            PartitionID]] = round_robin_partitioner,
        input_combiner: InputCombiner = InputCombiner,
) -> List[OutType]:
    @ray.remote(num_returns=output_num_partitions)
    def shuffle_map(i: PartitionID) -> List[List[ObjectRef[InType]]]:
        combiners = [input_combiner() for _ in range(output_num_partitions)]
        for out_i, item in partitioner(input_reader(i)):
            combiners[out_i].add(item)
        outputs = [[ray.put(r) for r in c.results()] for c in range(combiners)]
        return outputs

    @ray.remote
    def shuffle_reduce(i: PartitionID,
                       inputs: List[ObjectRef[InType]]) -> OutType:
        return output_writer(i, inputs)

    shuffle_map_out = ray.get(
        [shuffle_map.remote(i) for i in range(input_num_partitions)])

    shuffle_reduce_out = []
    for j in range(output_num_partitions):
        reduce_input = []
        for map_output in shuffle_map_out:
            reduce_input.extend(map_output[j])
        shuffle_reduce_out.append(shuffle_reduce.remote(j, reduce_input))

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

    partition_size = int(200e6)
    num_partitions = 50
    rows_per_partition = partition_size // (8 * 2)

    tracker = _StatusTracker.remote()

    def input_reader(i: PartitionID) -> Iterable[InType]:
        for _ in range(num_partitions):
            yield np.ones(
                (rows_per_partition // num_partitions, 2), dtype=np.int64)
        tracker.inc.remote()

    def output_writer(i: PartitionID,
                      shuffle_inputs: List[ObjectRef[InType]]) -> OutType:
        for obj_ref in shuffle_inputs:
            ray.get(obj_ref)
        tracker.inc2.remote()

    start = time.time()

    output_size = simple_shuffle(
        input_reader=input_reader,
        input_num_partition=num_partitions,
        output_num_partitions=num_partitions,
        output_writer=output_writer)
    delta = time.time() - start

    print("Shuffled", int(output_size / (1024 * 1024)), "MiB in", delta,
          "seconds")
