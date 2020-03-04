import time
from collections import Counter
import pytest

import ray
from ray.util.iter import from_items, from_iterators, from_range, \
    from_actors, ParallelIteratorWorker, LocalIterator


def test_metrics(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4], num_shards=1)
    it2 = from_items([1, 2, 3, 4], num_shards=1)

    def f(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["foo"] += x
        return metrics.counters["foo"]

    it = it.gather_sync().for_each(f)
    it2 = it2.gather_sync().for_each(f)

    # Context cannot be accessed outside the iterator.
    with pytest.raises(ValueError):
        LocalIterator.get_metrics()

    # Tests iterators have isolated contexts.
    assert it.take(4) == [1, 3, 6, 10]
    assert it2.take(4) == [1, 3, 6, 10]

    # Context cannot be accessed outside the iterator.
    with pytest.raises(ValueError):
        LocalIterator.get_metrics()


def test_metrics_union(ray_start_regular_shared):
    it1 = from_items([1, 2, 3, 4], num_shards=1)
    it2 = from_items([1, 2, 3, 4], num_shards=1)

    def foo_metrics(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["foo"] += x
        return metrics.counters["foo"]

    def bar_metrics(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["bar"] += 100
        return metrics.counters["bar"]

    def verify_metrics(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["n"] += 1
        # Check the unioned iterator gets a new metric context.
        assert "foo" not in metrics.counters
        assert "bar" not in metrics.counters
        # Check parent metrics are accessible.
        if metrics.counters["n"] > 2:
            assert "foo" in metrics.parent_metrics[0].counters
            assert "bar" in metrics.parent_metrics[1].counters
        return x

    it1 = it1.gather_async().for_each(foo_metrics)
    it2 = it2.gather_async().for_each(bar_metrics)
    it3 = it1.union(it2, deterministic=True)
    it3 = it3.for_each(verify_metrics)
    assert it3.take(10) == [1, 100, 3, 200, 6, 300, 10, 400]


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


def test_local_shuffle(ray_start_regular_shared):
    # confirm that no data disappears, and they all stay within the same shard
    it = from_range(8, num_shards=2).local_shuffle(shuffle_buffer_size=2)
    assert repr(it) == ("ParallelIterator[from_range[8, shards=2]" +
                        ".local_shuffle(shuffle_buffer_size=2, seed=None)]")
    shard_0 = it.get_shard(0)
    shard_1 = it.get_shard(1)
    assert set(shard_0) == {0, 1, 2, 3}
    assert set(shard_1) == {4, 5, 6, 7}

    # check that shuffling results in different orders
    it1 = from_range(100, num_shards=10).local_shuffle(shuffle_buffer_size=5)
    it2 = from_range(100, num_shards=10).local_shuffle(shuffle_buffer_size=5)
    assert list(it1.gather_sync()) != list(it2.gather_sync())

    # buffer size of 1 should not result in any shuffling
    it3 = from_range(10, num_shards=1).local_shuffle(shuffle_buffer_size=1)
    assert list(it3.gather_sync()) == list(range(10))

    # statistical test
    it4 = from_items(
        [0, 1] * 10000, num_shards=1).local_shuffle(shuffle_buffer_size=100)
    result = "".join(it4.gather_sync().for_each(str))
    freq_counter = Counter(zip(result[:-1], result[1:]))
    assert len(freq_counter) == 4
    for key, value in freq_counter.items():
        assert value / len(freq_counter) > 0.2


def test_repartition_less(ray_start_regular_shared):
    it = from_range(9, num_shards=3)
    it1 = it.repartition(2)
    assert repr(it1) == ("ParallelIterator[from_range[9, " +
                         "shards=3].repartition[num_partitions=2]]")

    assert it1.num_shards() == 2
    shard_0_set = set(it1.get_shard(0))
    shard_1_set = set(it1.get_shard(1))
    assert shard_0_set == {0, 2, 3, 5, 6, 8}
    assert shard_1_set == {1, 4, 7}


def test_repartition_more(ray_start_regular_shared):
    it = from_range(100, 2).repartition(3)
    assert it.num_shards() == 3
    assert set(it.get_shard(0)) == set(range(0, 50, 3)) | set(
        (range(50, 100, 3)))
    assert set(
        it.get_shard(1)) == set(range(1, 50, 3)) | set(range(51, 100, 3))
    assert set(
        it.get_shard(2)) == set(range(2, 50, 3)) | set(range(52, 100, 3))


def test_repartition_consistent(ray_start_regular_shared):
    # repartition should be deterministic
    it1 = from_range(9, num_shards=1).repartition(2)
    it2 = from_range(9, num_shards=1).repartition(2)
    assert it1.num_shards() == 2
    assert it2.num_shards() == 2
    assert set(it1.get_shard(0)) == set(it2.get_shard(0))
    assert set(it1.get_shard(1)) == set(it2.get_shard(1))


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
    import sys
    sys.exit(pytest.main(["-v", __file__]))
