import pytest
import time

import ray
from ray.data._internal.block_batching.util import _make_async_gen, _calculate_ref_hits


def test_make_async_gen_fail():
    """Tests that any errors raised in async threads are propagated to the main
    thread."""

    def gen(base_iterator):
        raise ValueError("Fail")

    iterator = _make_async_gen(base_iterator=iter([1]), fn=gen)

    with pytest.raises(ValueError) as e:
        for _ in iterator:
            pass

    assert e.match("Fail")


def test_make_async_gen():
    """Tests that make_async_gen overlaps compute."""

    num_items = 10

    def gen(base_iterator):
        for i in base_iterator:
            time.sleep(2)
            yield i

    def sleep_udf(item):
        time.sleep(3)
        return item

    iterator = _make_async_gen(
        base_iterator=iter(range(num_items)), fn=gen, num_workers=1
    )

    start_time = time.time()
    outputs = []
    for item in iterator:
        print(item)
        outputs.append(sleep_udf(item))
    end_time = time.time()

    assert outputs == list(range(num_items))

    # Three second buffer.
    assert end_time - start_time < num_items * 3 + 3


def test_make_async_gen_multiple_threads():
    """Tests that using multiple threads can overlap compute even more."""

    num_items = 5

    def gen(base_iterator):
        for i in base_iterator:
            time.sleep(4)
            yield i

    def sleep_udf(item):
        time.sleep(5)
        return item

    # All 5 items should be fetched concurrently.
    iterator = _make_async_gen(
        base_iterator=iter(range(num_items)), fn=gen, num_workers=5
    )

    start_time = time.time()

    # Only sleep for first item.
    sleep_udf(next(iterator))

    # All subsequent items should already be prefetched and should be ready.
    for _ in iterator:
        pass
    end_time = time.time()

    # 4 second for first item, 5 seconds for udf, 0.5 seconds buffer
    assert end_time - start_time < 9.5


def test_calculate_ref_hits(ray_start_regular_shared):
    refs = [ray.put(0), ray.put(1)]
    hits, misses, unknowns = _calculate_ref_hits(refs)
    assert hits == 2
    assert misses == 0
    assert unknowns == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
