import time
import collections
from collections import Counter
import pytest

import ray
from ray.util.iter import from_items, from_iterators, from_range, \
    from_actors, ParallelIteratorWorker, LocalIterator

# ray.init(num_cpus=1, object_store_memory=150*1024*1024)
ray.init()


def test_remote(ray_start_regular_shared):
    it = from_iterators([[0, 1], [3, 4], [5, 6, 7]])
    assert it.num_shards() == 3

    @ray.remote
    def to_list(local_it):
        return list(local_it)

    it = it.repartition(3)
    assert set(ray.get(to_list.remote(it.get_shard(0)))) == set([0, 3, 5])
    # assert set(to_list(it.get_shard(0))) == set([0, 3, 5])


test_remote(None)
