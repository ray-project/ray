from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.experimental.iter import from_items, from_generators, from_range, \
    from_actors, _ParIteratorWorker


def test_from_items(ray_start_regular_shared):
    it = from_items([1, 2, 3, 4])
    assert list(it.sync_iterator()) == [1, 2, 3, 4]


def test_from_generators(ray_start_regular_shared):
    it = from_generators([range(2), range(2)])
    assert list(it.sync_iterator()) == [0, 0, 1, 1]


def test_from_range(ray_start_regular_shared):
    it = from_range(4)
    assert list(it.sync_iterator()) == [0, 2, 1, 3]


def test_from_actors(ray_start_regular_shared):
    worker = ray.remote(_ParIteratorWorker)
    a = worker.remote([1, 2])
    b = worker.remote([3, 4])
    it = from_actors([a, b])
    assert list(it.sync_iterator()) == [1, 3, 2, 4]


def test_for_each(ray_start_regular_shared):
    it = from_range(4).for_each(lambda x: x * 2)
    assert list(it.sync_iterator()) == [0, 4, 2, 6]


def test_chain(ray_start_regular_shared):
    it = from_range(4).for_each(lambda x: x * 2).for_each(lambda x: x * 2)
    assert list(it.sync_iterator()) == [0, 8, 4, 12]


def test_filter(ray_start_regular_shared):
    it = from_range(4).filter(lambda x: x < 3)
    assert list(it.sync_iterator()) == [0, 2]


def test_batch(ray_start_regular_shared):
    it = from_range(4, 1).batch(2)
    assert list(it.sync_iterator()) == [[0, 1], [2, 3]]


def test_flatten(ray_start_regular_shared):
    it = from_items([[1, 2], [3, 4]], 1).flatten()
    assert list(it.sync_iterator()) == [1, 2, 3, 4]


def test_async_iterator(ray_start_regular_shared):
    it = from_range(4)
    assert sorted(list(it.async_iterator())) == [0, 1, 2, 3]


def test_sync_across_shards(ray_start_regular_shared):
    it = from_generators([[0, 1], [2, 3]])
    assert sorted(list(it.sync_iterator_across_shards())) == [[0, 2], [1, 3]]


def test_remote(ray_start_regular_shared):
    it = from_generators([[0, 1], [3, 4], [5, 6, 7]])
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


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
