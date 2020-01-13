import time

import ray
from ray.experimental.iter import from_items, from_iterators, from_range, \
    from_actors, ParallelIteratorWorker


def test_from_items(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4])
    assert repr(it) == "ParallelIterator[from_items[int, 4, shards=2]]"
    assert list(it.gather_sync()) == [1, 2, 3, 4]
    assert next(it.gather_sync()) == 1


def test_from_items_repeat(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4], repeat=True)
    assert repr(
        it) == "ParallelIterator[from_items[int, 4, shards=2, repeat=True]]"
    assert it.take(8) == [1, 2, 3, 4, 1, 2, 3, 4]


def test_from_iterators(ray_start_regular_shared):
    it = from_iterators([range(2), range(2)])
    assert repr(it) == "ParallelIterator[from_iterators[shards=2]]"
    assert list(it.gather_sync()) == [0, 0, 1, 1]


def test_from_range(ray_start_regular_shared):
    it = from_range(4)
    assert repr(it) == "ParallelIterator[from_range[4, shards=2]]"
    assert list(it.gather_sync()) == [0, 2, 1, 3]


def test_from_actors(ray_start_regular_shared):
    @ray.remote
    class CustomWorker(ParallelIteratorWorker):
        def __init__(self, data):
            ParallelIteratorWorker.__init__(self, data, False)

    a = CustomWorker.remote([1, 2])
    b = CustomWorker.remote([3, 4])
    it = from_actors([a, b])
    assert repr(it) == "ParallelIterator[from_actors[shards=2]]"
    assert list(it.gather_sync()) == [1, 3, 2, 4]


def test_for_each(ray_start_regular_shared):
    it = from_range(4).for_each(lambda x: x * 2)
    assert repr(it) == "ParallelIterator[from_range[4, shards=2].for_each()]"
    assert list(it.gather_sync()) == [0, 4, 2, 6]


def test_combine(ray_start_regular_shared):
    it = from_range(4, 1).combine(lambda x: [x, x])
    assert repr(it) == "ParallelIterator[from_range[4, shards=1].combine()]"
    assert list(it.gather_sync()) == [0, 0, 1, 1, 2, 2, 3, 3]


def test_chain(ray_start_regular_shared):
    it = from_range(4).for_each(lambda x: x * 2).for_each(lambda x: x * 2)
    assert repr(
        it
    ) == "ParallelIterator[from_range[4, shards=2].for_each().for_each()]"
    assert list(it.gather_sync()) == [0, 8, 4, 12]


def test_filter(ray_start_regular_shared):
    it = from_range(4).filter(lambda x: x < 3)
    assert repr(it) == "ParallelIterator[from_range[4, shards=2].filter()]"
    assert list(it.gather_sync()) == [0, 2, 1]


def test_batch(ray_start_regular_shared):
    it = from_range(4, 1).batch(2)
    assert repr(it) == "ParallelIterator[from_range[4, shards=1].batch(2)]"
    assert list(it.gather_sync()) == [[0, 1], [2, 3]]


def test_flatten(ray_start_regular_shared):
    it = from_items([[1, 2], [3, 4]], 1).flatten()
    assert repr(
        it) == "ParallelIterator[from_items[list, 2, shards=1].flatten()]"
    assert list(it.gather_sync()) == [1, 2, 3, 4]


def test_gather_sync(ray_start_regular_shared):
    it = from_range(4)
    it = it.gather_sync()
    assert (
        repr(it) == "LocalIterator[ParallelIterator[from_range[4, shards=2]]"
        ".gather_sync()]")
    assert sorted(it) == [0, 1, 2, 3]


def test_gather_async(ray_start_regular_shared):
    it = from_range(4)
    it = it.gather_async()
    assert (
        repr(it) == "LocalIterator[ParallelIterator[from_range[4, shards=2]]"
        ".gather_async()]")
    assert sorted(it) == [0, 1, 2, 3]


def test_batch_across_shards(ray_start_regular_shared):
    it = from_iterators([[0, 1], [2, 3]])
    it = it.batch_across_shards()
    assert (
        repr(it) == "LocalIterator[ParallelIterator[from_iterators[shards=2]]"
        ".batch_across_shards()]")
    assert sorted(it) == [[0, 2], [1, 3]]


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
    assert (repr(it) == "ParallelIterator[ParallelUnion[ParallelIterator["
            "from_items[str, 3, shards=1]], ParallelIterator["
            "from_items[str, 3, shards=1]]]]")
    assert list(it.gather_sync()) == ["a", "x", "b", "y", "c", "z"]


def test_union_local(ray_start_regular_shared):
    it1 = from_items(["a", "b", "c"], 1).gather_async()
    it2 = from_range(5, 2).for_each(str).gather_async()
    it = it1.union(it2)
    assert sorted(it) == ["0", "1", "2", "3", "4", "a", "b", "c"]


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
    results = list(it.gather_async())
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
    it = it1.gather_async().union(it2.gather_async())
    assert (repr(it) == "LocalIterator[LocalUnion[LocalIterator["
            "ParallelIterator[from_iterators[shards=1].for_each()]"
            ".gather_async()], LocalIterator[ParallelIterator["
            "from_iterators[shards=1].for_each()].gather_async()]]]")
    results = list(it)
    assert all(x[0] == "slow" for x in results[-3:]), results


def test_serialization(ray_start_regular_shared):
    it = (from_items([1, 2, 3, 4]).gather_sync().for_each(lambda x: x)
          .filter(lambda x: True).batch(2).flatten())
    assert (repr(it) == "LocalIterator[ParallelIterator["
            "from_items[int, 4, shards=2]].gather_sync()."
            "for_each().filter().batch(2).flatten()]")

    @ray.remote
    def get(it):
        return list(it)

    assert ray.get(get.remote(it)) == [1, 2, 3, 4]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
