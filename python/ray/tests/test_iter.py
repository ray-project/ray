from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray
from ray.experimental.iter import from_items, from_iterators, from_range, \
    from_actors, _ParIteratorWorker


def test_from_items(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4])
    assert repr(it) == "from_items[int, 4, shards=2]"
    assert list(it.sync_iterator()) == [1, 2, 3, 4]
    assert next(it.sync_iterator()) == 1


def test_from_iterators(ray_start_regular_shared):
    it = from_iterators([range(2), range(2)])
    assert repr(it) == "from_iterators[shards=2]"
    assert list(it.sync_iterator()) == [0, 0, 1, 1]


def test_from_range(ray_start_regular_shared):
    it = from_range(4)
    assert repr(it) == "from_range[4, shards=2]"
    assert list(it.sync_iterator()) == [0, 2, 1, 3]


def test_from_actors(ray_start_regular_shared):
    worker = ray.remote(_ParIteratorWorker)
    a = worker.remote([1, 2])
    b = worker.remote([3, 4])
    it = from_actors([a, b])
    assert repr(it) == "from_actors[shards=2]"
    assert list(it.sync_iterator()) == [1, 3, 2, 4]


def test_for_each(ray_start_regular_shared):
    it = from_range(4).for_each(lambda x: x * 2)
    assert repr(it) == "from_range[4, shards=2].for_each()"
    assert list(it.sync_iterator()) == [0, 4, 2, 6]


def test_chain(ray_start_regular_shared):
    it = from_range(4).for_each(lambda x: x * 2).for_each(lambda x: x * 2)
    assert repr(it) == "from_range[4, shards=2].for_each().for_each()"
    assert list(it.sync_iterator()) == [0, 8, 4, 12]


def test_filter(ray_start_regular_shared):
    it = from_range(4).filter(lambda x: x < 3)
    assert repr(it) == "from_range[4, shards=2].filter()"
    assert list(it.sync_iterator()) == [0, 2, 1]


def test_batch(ray_start_regular_shared):
    it = from_range(4, 1).batch(2)
    assert repr(it) == "from_range[4, shards=1].batch(2)"
    assert list(it.sync_iterator()) == [[0, 1], [2, 3]]


def test_flatten(ray_start_regular_shared):
    it = from_items([[1, 2], [3, 4]], 1).flatten()
    assert repr(it) == "from_items[list, 2, shards=1].flatten()"
    assert list(it.sync_iterator()) == [1, 2, 3, 4]


def test_sync_iterator(ray_start_regular_shared):
    it = from_range(4)
    it = it.sync_iterator()
    assert repr(it) == "from_range[4, shards=2].sync_iterator()"
    assert sorted(list(it)) == [0, 1, 2, 3]


def test_async_iterator(ray_start_regular_shared):
    it = from_range(4)
    it = it.async_iterator()
    assert repr(it) == "from_range[4, shards=2].async_iterator()"
    assert sorted(list(it)) == [0, 1, 2, 3]


def test_batch_across_shards(ray_start_regular_shared):
    it = from_iterators([[0, 1], [2, 3]])
    it = it.batch_across_shards()
    assert repr(it) == "from_iterators[shards=2].batch_across_shards()"
    assert sorted(list(it)) == [[0, 2], [1, 3]]


def test_remote(ray_start_regular_shared):
    it = from_iterators([[0, 1], [3, 4], [5, 6, 7]])
    assert it.num_shards() == 3

    @ray.remote
    def get_shard(it, i):
        return list(it.get_shard(i))

    assert ray.get(get_shard.remote(it, 0)) == [0, 1]
    assert ray.get(get_shard.remote(it, 1)) == [3, 4]
    assert ray.get(get_shard.remote(it, 2)) == [5, 6, 7]

    @ray.remote
    def check_remote(it):
        assert ray.get(get_shard.remote(it, 0)) == [0, 1]
        assert ray.get(get_shard.remote(it, 1)) == [3, 4]
        assert ray.get(get_shard.remote(it, 2)) == [5, 6, 7]

    ray.get(check_remote.remote(it))


def test_union(ray_start_regular_shared):
    it1 = from_items(["a", "b", "c"], 1)
    it2 = from_items(["x", "y", "z"], 1)
    it = it1.union(it2)
    assert (
        repr(it) ==
        "ParUnion[from_items[str, 3, shards=1], from_items[str, 3, shards=1]]")
    assert list(it.sync_iterator()) == ["a", "x", "b", "y", "c", "z"]


def test_union_local(ray_start_regular_shared):
    it1 = from_items(["a", "b", "c"], 1).async_iterator()
    it2 = from_range(5, 2).for_each(str).async_iterator()
    it = it1.union(it2)
    assert sorted(list(it)) == ["0", "1", "2", "3", "4", "a", "b", "c"]


def test_union_async(ray_start_regular_shared):
    def gen_fast():
        for i in range(10):
            time.sleep(0.05)
            print("PRODUCE FAST", i)
            yield i

    def gen_slow():
        for i in range(10):
            time.sleep(0.3)
            print("PRODUCE SLOW", i)
            yield i

    it1 = from_iterators([gen_fast]).for_each(lambda x: ("fast", x))
    it2 = from_iterators([gen_slow]).for_each(lambda x: ("slow", x))
    it = it1.union(it2)
    results = list(it.async_iterator())
    assert all(x[0] == "slow" for x in results[-3:]), results


def test_union_local_async(ray_start_regular_shared):
    def gen_fast():
        for i in range(10):
            time.sleep(0.05)
            print("PRODUCE FAST", i)
            yield i

    def gen_slow():
        for i in range(10):
            time.sleep(0.3)
            print("PRODUCE SLOW", i)
            yield i

    it1 = from_iterators([gen_fast]).for_each(lambda x: ("fast", x))
    it2 = from_iterators([gen_slow]).for_each(lambda x: ("slow", x))
    it = it1.async_iterator().union(it2.async_iterator())
    assert (repr(it) ==
            "LocalUnion[from_iterators[shards=1].for_each().async_iterator(), "
            "from_iterators[shards=1].for_each().async_iterator()]")
    results = list(it)
    assert all(x[0] == "slow" for x in results[-3:]), results


def test_serialization(ray_start_regular_shared):
    it = (from_items([1, 2, 3, 4]).sync_iterator().for_each(lambda x: x)
          .filter(lambda x: True).batch(2).flatten())
    assert (repr(it) == "from_items[int, 4, shards=2].sync_iterator()."
            "for_each().filter().batch(2).flatten()")

    @ray.remote
    def get(it):
        return list(it)

    assert ray.get(get.remote(it)) == [1, 2, 3, 4]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
