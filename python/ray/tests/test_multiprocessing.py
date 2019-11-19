from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest

import ray
from ray.experimental.multiprocessing import Pool, TimeoutError


@pytest.fixture
def pool():
    pool = Pool(processes=1)
    yield pool
    pool.terminate()
    ray.shutdown()


@pytest.fixture
def pool_4_procs():
    pool = Pool(processes=4)
    yield pool
    pool.terminate()
    ray.shutdown()


@pytest.mark.skip(reason="Currently fails - globals not working.")
def test_initializer():
    def init(arg1, arg2):
        global x
        x = arg1 + arg2

    pool = Pool(processes=4, initializer=init, initargs=(1, 2))

    def get(i):
        return x

    for result in pool.map(get, range(100)):
        assert result == 3

    pool.terminate()
    ray.shutdown()


def test_apply(pool):
    def f(arg1, arg2, kwarg1=None, kwarg2=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg1 is None
        assert kwarg2 == 3
        return 1

    assert pool.apply(f, (1, 2), {"kwarg2": 3}) == 1
    with pytest.raises(AssertionError):
        pool.apply(f, (
            2,
            2,
        ), {"kwarg2": 3})
    with pytest.raises(Exception):
        pool.apply(f, (1, ))
    with pytest.raises(Exception):
        pool.apply(f, (1, 2), {"kwarg1": 3})


def test_apply_async(pool):
    def f(arg1, arg2, kwarg1=None, kwarg2=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg1 is None
        assert kwarg2 == 3
        return 1

    assert pool.apply_async(f, (1, 2), {"kwarg2": 3}).get() == 1
    with pytest.raises(AssertionError):
        pool.apply_async(f, (
            2,
            2,
        ), {
            "kwarg2": 3
        }).get()
    with pytest.raises(Exception):
        pool.apply_async(f, (1, )).get()
    with pytest.raises(Exception):
        pool.apply_async(f, (1, 2), {"kwarg1": 3}).get()

    # Won't return until the input ObjectID is fulfilled.
    def ten_over(input):
        return 10 / ray.get(input[0])

    # Generate a random ObjectID that will be fulfilled later.
    object_id = ray.ObjectID.from_random()
    result = pool.apply_async(ten_over, ([object_id], ))
    result.wait(timeout=0.01)
    assert not result.ready()
    with pytest.raises(TimeoutError):
        result.get(timeout=0.1)

    # Fulfill the ObjectID.
    ray.worker.global_worker.put_object(10, object_id=object_id)
    result.wait(timeout=10)
    assert result.ready()
    assert result.successful()
    assert result.get() == 1

    # Generate a random ObjectID that will be fulfilled later.
    object_id = ray.ObjectID.from_random()
    result = pool.apply_async(ten_over, ([object_id], ))
    with pytest.raises(ValueError, match="not ready"):
        result.successful()

    # Fulfill the ObjectID with 0, causing the task to fail (divide by zero).
    ray.worker.global_worker.put_object(0, object_id=object_id)
    result.wait(timeout=10)
    assert result.ready()
    assert not result.successful()
    with pytest.raises(ZeroDivisionError):
        result.get()


def test_close():
    pass
