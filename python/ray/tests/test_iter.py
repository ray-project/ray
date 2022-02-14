import sys
import time
import collections
from collections import Counter
import pytest

import ray
from ray.util.iter import (
    from_items,
    from_iterators,
    from_range,
    from_actors,
    ParallelIteratorWorker,
    LocalIterator,
)
from ray._private.test_utils import Semaphore


def test_select_shards(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4], num_shards=4)
    it1 = it.select_shards([0, 2])
    it2 = it.select_shards([1, 3])
    assert it1.take(4) == [1, 3]
    assert it2.take(4) == [2, 4]


def test_transform(ray_start_regular_shared):
    def f(it):
        for item in it:
            yield item * 2

    def g(it):
        for item in it:
            if item >= 2:
                yield item

    it = from_range(4).transform(f)
    assert repr(it) == "ParallelIterator[from_range[4, shards=2].transform()]"
    assert list(it.gather_sync()) == [0, 4, 2, 6]

    it = from_range(4)
    assert list(it.gather_sync().transform(g)) == [2, 3]


def test_metrics(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4], num_shards=1)
    it2 = from_items([1, 2, 3, 4], num_shards=1)

    def f(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["foo"] += x
        return metrics.counters["foo"]

    it = it.gather_sync().for_each(f)
    it2 = it2.gather_sync().for_each(f)

    # Tests iterators have isolated contexts.
    assert it.take(4) == [1, 3, 6, 10]
    assert it2.take(4) == [1, 3, 6, 10]


def test_zip_with_source_actor(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4], num_shards=2)
    counts = collections.defaultdict(int)
    for actor, value in it.gather_async().zip_with_source_actor():
        counts[actor] += 1
    assert len(counts) == 2
    for a, count in counts.items():
        assert count == 2


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
        # Check the metrics context is shared.
        if metrics.counters["n"] >= 2:
            assert "foo" in metrics.counters
            assert "bar" in metrics.counters
        return x

    it1 = it1.gather_async().for_each(foo_metrics)
    it2 = it2.gather_async().for_each(bar_metrics)
    it3 = it1.union(it2, deterministic=True)
    it3 = it3.for_each(verify_metrics)
    assert it3.take(10) == [1, 100, 3, 200, 6, 300, 10, 400]


def test_metrics_union_recursive(ray_start_regular_shared):
    it1 = from_items([1, 2, 3, 4], num_shards=1)
    it2 = from_items([1, 2, 3, 4], num_shards=1)
    it3 = from_items([1, 2, 3, 4], num_shards=1)

    def foo_metrics(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["foo"] += 1
        return metrics.counters["foo"]

    def bar_metrics(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["bar"] += 1
        return metrics.counters["bar"]

    def baz_metrics(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["baz"] += 1
        return metrics.counters["baz"]

    def verify_metrics(x):
        metrics = LocalIterator.get_metrics()
        metrics.counters["n"] += 1
        # Check the metrics context is shared recursively.
        print(metrics.counters)
        if metrics.counters["n"] >= 3:
            assert "foo" in metrics.counters
            assert "bar" in metrics.counters
            assert "baz" in metrics.counters
        return x

    it1 = it1.gather_async().for_each(foo_metrics)
    it2 = it2.gather_async().for_each(bar_metrics)
    it3 = it3.gather_async().for_each(baz_metrics)
    it12 = it1.union(it2, deterministic=True)
    it123 = it12.union(it3, deterministic=True)
    out = it123.for_each(verify_metrics)
    assert out.take(20) == [1, 1, 1, 2, 2, 3, 2, 4, 3, 3, 4, 4]


def test_from_items(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4])
    assert repr(it) == "ParallelIterator[from_items[int, 4, shards=2]]"
    assert list(it.gather_sync()) == [1, 2, 3, 4]
    assert next(it.gather_sync()) == 1


def test_from_items_repeat(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4], repeat=True)
    assert repr(it) == "ParallelIterator[from_items[int, 4, shards=2, repeat=True]]"
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


def test_for_each_concur_async(ray_start_regular_shared):
    main_wait = Semaphore.remote(value=0)
    test_wait = Semaphore.remote(value=0)

    def task(x):
        i, main_wait, test_wait = x
        ray.get(main_wait.release.remote())
        ray.get(test_wait.acquire.remote())
        return i + 10

    @ray.remote(num_cpus=0.01)
    def to_list(it):
        return list(it)

    it = from_items([(i, main_wait, test_wait) for i in range(8)], num_shards=2)
    it = it.for_each(task, max_concurrency=2, resources={"num_cpus": 0.01})

    list_promise = to_list.remote(it.gather_async())

    for i in range(4):
        assert i in [0, 1, 2, 3]
        ray.get(main_wait.acquire.remote())

    # There should be exactly 4 tasks executing at this point.
    assert ray.get(main_wait.locked.remote()) is True, "Too much parallelism"

    # When we finish one task, exactly one more should start.
    ray.get(test_wait.release.remote())
    ray.get(main_wait.acquire.remote())
    assert ray.get(main_wait.locked.remote()) is True, "Too much parallelism"

    # Finish everything and make sure the output matches a regular iterator.
    for i in range(7):
        ray.get(test_wait.release.remote())

    assert repr(it) == "ParallelIterator[from_items[tuple, 8, shards=2].for_each()]"
    result_list = ray.get(list_promise)
    assert set(result_list) == set(range(10, 18))


def test_for_each_concur_sync(ray_start_regular_shared):
    main_wait = Semaphore.remote(value=0)
    test_wait = Semaphore.remote(value=0)

    def task(x):
        i, main_wait, test_wait = x
        ray.get(main_wait.release.remote())
        ray.get(test_wait.acquire.remote())
        return i + 10

    @ray.remote(num_cpus=0.01)
    def to_list(it):
        return list(it)

    it = from_items([(i, main_wait, test_wait) for i in range(8)], num_shards=2)
    it = it.for_each(task, max_concurrency=2, resources={"num_cpus": 0.01})

    list_promise = to_list.remote(it.gather_sync())

    for i in range(4):
        assert i in [0, 1, 2, 3]
        ray.get(main_wait.acquire.remote())

    # There should be exactly 4 tasks executing at this point.
    assert ray.get(main_wait.locked.remote()) is True, "Too much parallelism"

    for i in range(8):
        ray.get(test_wait.release.remote())

    assert repr(it) == "ParallelIterator[from_items[tuple, 8, shards=2].for_each()]"
    result_list = ray.get(list_promise)
    assert set(result_list) == set(range(10, 18))


def test_combine(ray_start_regular_shared):
    it = from_range(4, 1).combine(lambda x: [x, x])
    assert repr(it) == "ParallelIterator[from_range[4, shards=1].combine()]"
    assert list(it.gather_sync()) == [0, 0, 1, 1, 2, 2, 3, 3]


def test_duplicate(ray_start_regular_shared):
    it = from_range(5, num_shards=1)

    it1, it2 = it.gather_sync().duplicate(2)
    it1 = it1.batch(2)

    it3 = it1.union(it2, deterministic=False)
    results = it3.take(20)
    assert results == [0, [0, 1], 1, 2, [2, 3], 3, 4, [4]]


def test_chain(ray_start_regular_shared):
    it = from_range(4).for_each(lambda x: x * 2).for_each(lambda x: x * 2)
    assert repr(it) == "ParallelIterator[from_range[4, shards=2].for_each().for_each()]"
    assert list(it.gather_sync()) == [0, 8, 4, 12]


def test_filter(ray_start_regular_shared):
    it = from_range(4).filter(lambda x: x < 3)
    assert repr(it) == "ParallelIterator[from_range[4, shards=2].filter()]"
    assert list(it.gather_sync()) == [0, 2, 1]


def test_local_shuffle(ray_start_regular_shared):
    # confirm that no data disappears, and they all stay within the same shard
    it = from_range(8, num_shards=2).local_shuffle(shuffle_buffer_size=2)
    assert repr(it) == (
        "ParallelIterator[from_range[8, shards=2]"
        + ".local_shuffle(shuffle_buffer_size=2, seed=None)]"
    )
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
    it4 = from_items([0, 1] * 10000, num_shards=1).local_shuffle(
        shuffle_buffer_size=100
    )
    result = "".join(it4.gather_sync().for_each(str))
    freq_counter = Counter(zip(result[:-1], result[1:]))
    assert len(freq_counter) == 4
    for key, value in freq_counter.items():
        assert value / len(freq_counter) > 0.2


def test_repartition_less(ray_start_regular_shared):
    it = from_range(9, num_shards=3)
    # chaining operations after a repartition should work
    it1 = it.repartition(2).for_each(lambda x: 2 * x)
    assert repr(it1) == (
        "ParallelIterator[from_range[9, "
        + "shards=3].repartition[num_partitions=2].for_each()]"
    )

    assert it1.num_shards() == 2
    shard_0_set = set(it1.get_shard(0))
    shard_1_set = set(it1.get_shard(1))
    assert shard_0_set == {0, 4, 6, 10, 12, 16}
    assert shard_1_set == {2, 8, 14}


def test_repartition_more(ray_start_regular_shared):
    it = from_range(100, 2).repartition(3)
    assert it.num_shards() == 3
    assert set(it.get_shard(0)) == set(range(0, 50, 3)) | set((range(50, 100, 3)))
    assert set(it.get_shard(1)) == set(range(1, 50, 3)) | set(range(51, 100, 3))
    assert set(it.get_shard(2)) == set(range(2, 50, 3)) | set(range(52, 100, 3))


def test_repartition_consistent(ray_start_regular_shared):
    # repartition should be deterministic
    it1 = from_range(9, num_shards=1).repartition(2)
    it2 = from_range(9, num_shards=1).repartition(2)
    # union should work after repartition
    it3 = it1.union(it2)
    assert it1.num_shards() == 2
    assert it2.num_shards() == 2
    assert set(it1.get_shard(0)) == set(it2.get_shard(0))
    assert set(it1.get_shard(1)) == set(it2.get_shard(1))

    assert it3.num_shards() == 4
    assert set(it3.gather_async()) == set(it1.gather_async()) | set(it2.gather_async())


def test_batch(ray_start_regular_shared):
    it = from_range(4, 1).batch(2)
    assert repr(it) == "ParallelIterator[from_range[4, shards=1].batch(2)]"
    assert list(it.gather_sync()) == [[0, 1], [2, 3]]


def test_flatten(ray_start_regular_shared):
    it = from_items([[1, 2], [3, 4]], 1).flatten()
    assert repr(it) == "ParallelIterator[from_items[list, 2, shards=1].flatten()]"
    assert list(it.gather_sync()) == [1, 2, 3, 4]


def test_gather_sync(ray_start_regular_shared):
    it = from_range(4)
    it = it.gather_sync()
    assert (
        repr(it) == "LocalIterator[ParallelIterator[from_range[4, shards=2]]"
        ".gather_sync()]"
    )
    assert sorted(it) == [0, 1, 2, 3]


def test_gather_async(ray_start_regular_shared):
    it = from_range(4)
    it = it.gather_async()
    assert (
        repr(it) == "LocalIterator[ParallelIterator[from_range[4, shards=2]]"
        ".gather_async()]"
    )
    assert sorted(it) == [0, 1, 2, 3]


def test_gather_async_optimized(ray_start_regular_shared):
    it = from_range(100)
    it = it.gather_async(batch_ms=100, num_async=4)
    assert sorted(it) == list(range(100))


def test_get_shard_optimized(ray_start_regular_shared):
    it = from_range(6, num_shards=3)
    shard1 = it.get_shard(shard_index=0, batch_ms=25, num_async=2)
    shard2 = it.get_shard(shard_index=1, batch_ms=15, num_async=3)
    shard3 = it.get_shard(shard_index=2, batch_ms=5, num_async=4)
    assert list(shard1) == [0, 1]
    assert list(shard2) == [2, 3]
    assert list(shard3) == [4, 5]


# Tested on 5/13/20
# Run on 2019 Macbook Pro with 8 cores, 16 threads
# 14.52 sec
# 14.64 sec
# 0.935 sec
# 0.515 sec
"""
def test_gather_async_optimized_benchmark(ray_start_regular_shared):
    import numpy as np
    import tensorflow as tf
    train, _ = tf.keras.datasets.fashion_mnist.load_data()
    images, labels = train
    num_bytes = images.nbytes / 1e6
    items = list(images)
    it = from_items(items, num_shards=4)
    it = it.for_each(lambda img: img/255)
    #local_it = it.gather_async(batch_ms=0, num_async=1)
    #local_it = it.gather_async(batch_ms=0, num_async=3)
    #local_it = it.gather_async(batch_ms=10, num_async=1)
    #local_it = it.gather_async(batch_ms=10, num_async=3)

    # dummy iterations
    for i in range(20):
        record = next(local_it)

    start_time = time.time()
    #print(start_time)
    count = 0
    for record in local_it:
        count += 1
    assert count == len(items) - 20
    end_time = time.time() - start_time
    print(end_time)
"""


def test_batch_across_shards(ray_start_regular_shared):
    it = from_iterators([[0, 1], [2, 3]])
    it = it.batch_across_shards()
    assert (
        repr(it) == "LocalIterator[ParallelIterator[from_iterators[shards=2]]"
        ".batch_across_shards()]"
    )
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
    assert (
        repr(it) == "ParallelIterator[ParallelUnion[ParallelIterator["
        "from_items[str, 3, shards=1]], ParallelIterator["
        "from_items[str, 3, shards=1]]]]"
    )
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
    assert (
        repr(it) == "LocalIterator[LocalUnion[LocalIterator["
        "ParallelIterator[from_iterators[shards=1].for_each()]"
        ".gather_async()], LocalIterator[ParallelIterator["
        "from_iterators[shards=1].for_each()].gather_async()]]]"
    )
    results = list(it)
    assert all(x[0] == "slow" for x in results[-3:]), results


def test_serialization(ray_start_regular_shared):
    it = (
        from_items([1, 2, 3, 4])
        .gather_sync()
        .for_each(lambda x: x)
        .filter(lambda x: True)
        .batch(2)
        .flatten()
    )
    assert (
        repr(it) == "LocalIterator[ParallelIterator["
        "from_items[int, 4, shards=2]].gather_sync()."
        "for_each().filter().batch(2).flatten()]"
    )

    @ray.remote
    def get(it):
        return list(it)

    assert ray.get(get.remote(it)) == [1, 2, 3, 4]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
