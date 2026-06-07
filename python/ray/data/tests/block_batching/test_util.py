import logging
import random
import sys
import threading
import time
from collections import Counter
from os import urandom
from typing import Callable, Iterator

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.block_batching.interfaces import Batch, BatchMetadata
from ray.data._internal.block_batching.util import (
    _calculate_ref_hits,
    blocks_to_batches,
    collate,
    finalize_batches,
    format_batches,
    iter_threaded,
    resolve_block_refs,
)
from ray.data._internal.util import make_async_gen

logger = logging.getLogger(__file__)


def block_generator(num_rows: int, num_blocks: int):
    for _ in range(num_blocks):
        yield pa.table({"foo": [1] * num_rows})


def test_resolve_block_refs(ray_start_regular_shared):
    block_refs = [ray.put(0), ray.put(1), ray.put(2)]

    resolved_iter = resolve_block_refs(iter(block_refs))
    assert list(resolved_iter) == [0, 1, 2]


@pytest.mark.parametrize("block_size", [1, 10])
@pytest.mark.parametrize("drop_last", [True, False])
def test_blocks_to_batches(block_size, drop_last):
    num_blocks = 5
    block_iter = block_generator(num_rows=block_size, num_blocks=num_blocks)

    batch_size = 3
    batch_iter = list(
        blocks_to_batches(block_iter, batch_size=batch_size, drop_last=drop_last)
    )

    if drop_last:
        for batch in batch_iter:
            assert len(batch.data) == batch_size
    else:
        full_batches = 0
        leftover_batches = 0

        dataset_size = block_size * num_blocks
        for batch in batch_iter:
            if len(batch.data) == batch_size:
                full_batches += 1
            if len(batch.data) == (dataset_size % batch_size):
                leftover_batches += 1

        assert leftover_batches == 1
        assert full_batches == (dataset_size // batch_size)

    assert [batch.metadata.batch_idx for batch in batch_iter] == list(
        range(len(batch_iter))
    )


@pytest.mark.parametrize("batch_format", ["pandas", "numpy", "pyarrow"])
def test_format_batches(batch_format):
    block_iter = block_generator(num_rows=2, num_blocks=2)
    batch_iter = (
        Batch(BatchMetadata(batch_idx=i), block) for i, block in enumerate(block_iter)
    )
    batch_iter = list(format_batches(batch_iter, batch_format=batch_format))

    for batch in batch_iter:
        if batch_format == "pandas":
            assert isinstance(batch.data, pd.DataFrame)
        elif batch_format == "arrow":
            assert isinstance(batch.data, pa.Table)
        elif batch_format == "numpy":
            assert isinstance(batch.data, dict)
            assert isinstance(batch.data["foo"], np.ndarray)

    assert [batch.metadata.batch_idx for batch in batch_iter] == list(
        range(len(batch_iter))
    )


def test_collate():
    def collate_fn(batch):
        return pa.table({"bar": [1] * 2})

    batches = [
        Batch(BatchMetadata(batch_idx=i), data)
        for i, data in enumerate(block_generator(num_rows=2, num_blocks=2))
    ]
    batch_iter = collate(batches, collate_fn=collate_fn)

    for i, batch in enumerate(batch_iter):
        assert batch.metadata.batch_idx == i
        assert batch.data == pa.table({"bar": [1] * 2})


def test_finalize():
    def finalize_fn(batch):
        return pa.table({"bar": [1] * 2})

    batches = [
        Batch(BatchMetadata(batch_idx=i), data)
        for i, data in enumerate(block_generator(num_rows=2, num_blocks=2))
    ]
    batch_iter = finalize_batches(batches, finalize_fn=finalize_fn)

    for i, batch in enumerate(batch_iter):
        assert batch.metadata.batch_idx == i
        assert batch.data == pa.table({"bar": [1] * 2})


@pytest.mark.parametrize("preserve_ordering", [True, False])
@pytest.mark.parametrize("buffer_size", [0, 1, 2])
def test_make_async_gen_fail(buffer_size: int, preserve_ordering):
    """Tests that any errors raised in async threads are propagated to the main
    thread."""

    def gen(base_iterator):
        raise ValueError("Fail")

    iterator = make_async_gen(
        base_iterator=iter([1]),
        fn=gen,
        preserve_ordering=preserve_ordering,
        buffer_size=buffer_size,
    )

    with pytest.raises(ValueError) as e:
        for _ in iterator:
            pass

    assert e.match("Fail")


@pytest.mark.parametrize("preserve_ordering", [True, False])
def test_make_async_gen_varying_seq_length_stress_test(preserve_ordering):
    """This test executes make_async_gen against a function generating variable
    length sequences to stress test its concurrency control.
    """

    num_workers = 4

    c = 0

    # Roll the dice 100 times
    for i in range(100):
        # Fetch 8b seed from urandom
        seed = int.from_bytes(urandom(8), byteorder=sys.byteorder)
        r = random.Random(seed)

        print(f">>> Seed: {seed}")

        # NOTE: Number of seqs >> number of workers
        #       to saturate the input queue
        num_seqs = num_workers * 10

        lens = list(range(num_seqs))

        r.shuffle(lens)

        source = [range(len_) for len_ in lens]

        print("===" * 8)
        print(source)
        print("===" * 8)

        def flatten(list_iter):
            for l in list_iter:
                print(f">>> Flattening: {l}")
                yield from l

        it = make_async_gen(
            iter(source),
            flatten,
            preserve_ordering=preserve_ordering,
            num_workers=4,
            buffer_size=1,
        )

        total = 0

        for i in it:
            total += i

        assert total == 9880
        c += 1

    assert c == 100


@pytest.mark.parametrize("preserve_ordering", [True, False])
def test_make_async_gen_non_reentrant(preserve_ordering):
    """This test is asserting that make_async_gen iterating over the
    sequence as a whole and not re-entering provided transformation,
    as this might have substantial performance impact in extreme case
    of re-entering for every element of the sequence
    """

    logs = []
    finished = False

    def _transform_inner(it):
        nonlocal finished

        assert not finished

        logs.append(">>> Entering Inner")

        for i in it:
            logs.append(f">>> Inner: {i}")
            yield i

        logs.append(">>> Leaving Inner")

        # Once this transform finishes
        finished = True

    def _transform_b(it):
        logs.append(">>> Entering Outer")

        for i in _transform_inner(it):
            logs.append(f">>> Outer: {i}")
            yield i

        logs.append(">>> Leaving Outer")

    for _ in make_async_gen(
        iter(range(3)),
        _transform_b,
        preserve_ordering=preserve_ordering,
    ):
        pass

    assert [
        ">>> Entering Outer",
        ">>> Entering Inner",
        ">>> Inner: 0",
        ">>> Outer: 0",
        ">>> Inner: 1",
        ">>> Outer: 1",
        ">>> Inner: 2",
        ">>> Outer: 2",
        ">>> Leaving Inner",
        ">>> Leaving Outer",
    ] == logs


@pytest.mark.parametrize("preserve_ordering", [True, False])
@pytest.mark.parametrize(
    "buffer_size, expected_gen_time",
    [
        (0, 5.5),  # 5 x 1s + 0.5s buffer
        (1, 7.5),  # 3 x 1s + 2 x 2s (limited buffer delay) + 0.5s buffer
        (2, 5.5),  # 5 x 1s + 0.5s buffer
    ],
)
def test_make_async_gen_x(buffer_size: int, expected_gen_time, preserve_ordering):
    """Tests that make_async_gen overlaps compute."""

    num_items = 5

    def gen(base_iterator):
        gen_start = time.perf_counter()

        for i in base_iterator:
            time.sleep(1)
            yield i
            print(f">>> ({time.time()}) Generating {i}")

        gen_finish = time.perf_counter()

        # 0.5s buffer
        assert gen_finish - gen_start < expected_gen_time

    def sleepy_udf(item):
        time.sleep(2)
        return item

    iterator = make_async_gen(
        base_iterator=iter(range(num_items)),
        fn=gen,
        preserve_ordering=preserve_ordering,
        num_workers=1,
        buffer_size=buffer_size,
    )

    outputs = []

    iter_start = time.perf_counter()
    for item in iterator:
        print(f">>> ({time.time()}) Iterating over {item}")
        print(item)
        outputs.append(sleepy_udf(item))
    iter_finish = time.perf_counter()

    dur_s = iter_finish - iter_start

    print(f">>> Took {dur_s}")

    # 1s to yield first element
    # 10s to iterate t/h all 5
    # 0.5s extra buffer
    assert dur_s < num_items * 2 + 1.5

    # Assert ordering is preserved
    assert outputs == list(range(num_items))


@pytest.mark.parametrize("preserve_ordering", [True, False])
@pytest.mark.parametrize("buffer_size", [0, 1, 2])
def test_make_async_gen_multiple_threads(buffer_size: int, preserve_ordering):
    """Tests that using multiple threads can overlap compute even more."""

    num_items = 5

    gen_sleep = 2
    iter_sleep = 3

    def gen(base_iterator):
        for i in base_iterator:
            time.sleep(gen_sleep)
            yield i

    def sleep_udf(item):
        time.sleep(iter_sleep)
        return item

    # All 5 items should be fetched concurrently.
    iterator = make_async_gen(
        base_iterator=iter(range(num_items)),
        fn=gen,
        preserve_ordering=preserve_ordering,
        num_workers=5,
        buffer_size=buffer_size,
    )

    start_time = time.time()

    # Only sleep for first item.
    elements = [sleep_udf(next(iterator))] + list(iterator)

    # All subsequent items should already be prefetched and should be ready.
    end_time = time.time()

    # Assert ordering is preserved
    if preserve_ordering:
        assert elements == list(range(num_items))

    # - 2 second for every worker to handle their single element
    # - 3 seconds for overlapping one
    # - 0.5 seconds buffer
    assert end_time - start_time < gen_sleep + iter_sleep + 0.5


@pytest.mark.parametrize("preserve_ordering", [True, False])
@pytest.mark.parametrize("buffer_size", [0, 1, 2])
def test_make_async_gen_multiple_threads_unfinished(
    buffer_size: int, preserve_ordering
):
    """Tests that using multiple threads can overlap compute even more.
    Do not finish iteration with break in the middle.
    """

    num_items = 5

    def gen(base_iterator):
        for i in base_iterator:
            time.sleep(4)
            yield i

    def sleep_udf(item):
        time.sleep(5)
        return item

    # All 5 items should be fetched concurrently.
    iterator = make_async_gen(
        base_iterator=iter(range(num_items)),
        fn=gen,
        preserve_ordering=preserve_ordering,
        num_workers=5,
        buffer_size=buffer_size,
    )

    start_time = time.time()

    # Only sleep for first item.
    sleep_udf(next(iterator))

    # All subsequent items should already be prefetched and should be ready.
    for i, _ in enumerate(iterator):
        if i > 2:
            break
    end_time = time.time()

    # 4 second for first item, 5 seconds for udf, 0.5 seconds buffer
    assert end_time - start_time < 9.5


def test_calculate_ref_hits(ray_start_regular_shared):
    refs = [ray.put(0), ray.put(1)]
    hits, misses, unknowns = _calculate_ref_hits(refs)
    # With ctx.enable_get_object_locations_for_metrics set to False
    # by default, `_calculate_ref_hits` returns -1 for all, since
    # getting object locations is disabled.
    assert hits == 0
    assert misses == 0
    assert unknowns == 0

    ctx = ray.data.DataContext.get_current()
    prev_enable_get_object_locations_for_metrics = (
        ctx.enable_get_object_locations_for_metrics
    )
    try:
        ctx.enable_get_object_locations_for_metrics = True
        hits, misses, unknowns = _calculate_ref_hits(refs)
        assert hits == 2
        assert misses == 0
        assert unknowns == 0
    finally:
        ctx.enable_get_object_locations_for_metrics = (
            prev_enable_get_object_locations_for_metrics
        )


def _identity(it: Iterator[int]) -> Iterator[int]:
    return it


def _duplicate_each(it: Iterator[int]) -> Iterator[int]:
    for item in it:
        yield item
        yield item


class TestIterThreaded:
    """Unit tests for ``iter_threaded``."""

    @pytest.mark.parametrize("num_workers", [1, 2, 4])
    @pytest.mark.parametrize("output_buffer_size", [1, 2, 4])
    @pytest.mark.parametrize(
        "fn,multiplier",
        [(_identity, 1), (_duplicate_each, 2)],
        ids=["identity", "duplicate"],
    )
    def test_processes_all_exactly_once(
        self,
        num_workers: int,
        output_buffer_size: int,
        fn: Callable[[Iterator[int]], Iterator[int]],
        multiplier: int,
    ):
        """Every input item is consumed and produced exactly the expected
        number of times across the worker pool (no losses, no duplicates).
        Output ordering is not required."""
        items = list(range(50))
        output = list(
            iter_threaded(
                iter(items),
                fn,
                num_workers=num_workers,
                output_buffer_size=output_buffer_size,
            )
        )
        assert len(output) == len(items) * multiplier
        assert Counter(output) == Counter(items * multiplier)

    def test_stateful_base_iterator_thread_safe(self):
        """Python generators are not thread-safe; concurrent ``next()``
        calls raise ``ValueError: generator already executing`` without
        a lock. This test passes only if ``iter_threaded`` serializes
        the underlying ``next()`` properly."""

        def stateful_gen():
            for i in range(200):
                # Encourage interleaving across workers.
                time.sleep(0.001)
                yield i

        output = list(iter_threaded(stateful_gen(), _identity, num_workers=4))
        assert sorted(output) == list(range(200))

    @pytest.mark.parametrize("num_workers", [1, 4])
    def test_fn_exception_propagates(self, num_workers: int):
        """An exception raised inside ``fn`` is surfaced to the consumer
        rather than silently swallowed or hanging the iterator."""

        def fn(it: Iterator[int]) -> Iterator[int]:
            for i, item in enumerate(it):
                if i >= 3:
                    raise ValueError("boom")
                yield item

        it = iter_threaded(iter(range(100)), fn, num_workers=num_workers)
        with pytest.raises(ValueError, match="boom"):
            list(it)

    @pytest.mark.parametrize("num_workers", [1, 4])
    def test_consumer_break_stops_workers(self, num_workers: int):
        """When the consumer breaks early and the iterator is no longer
        referenced, CPython GCs the generator immediately, which runs the
        ``finally: stopped.set()`` cleanup path. Worker threads should
        terminate within the ``_put`` poll interval (~100ms) rather than
        leak."""

        def slow_fn(it: Iterator[int]) -> Iterator[int]:
            for item in it:
                time.sleep(0.05)
                yield item

        # Inline so `break` drops the last reference → GC → finally.
        for i, _ in enumerate(
            iter_threaded(iter(range(10_000)), slow_fn, num_workers=num_workers)
        ):
            if i >= 5:
                break

        # Workers poll `stopped` every 100ms inside `_put`; give a generous
        # margin for CI under load.
        deadline = time.time() + 5.0
        while time.time() < deadline:
            alive = [t for t in threading.enumerate() if t.name == "iter_threaded"]
            if not alive:
                break
            time.sleep(0.05)
        else:
            pytest.fail(
                f"iter_threaded workers did not exit within 5s: "
                f"{[t.name for t in threading.enumerate() if t.name == 'iter_threaded']}"
            )

    def test_num_workers_validation(self):
        with pytest.raises(ValueError, match="at least 1"):
            list(iter_threaded(iter([1]), _identity, num_workers=0))

    def test_empty_base_iterator(self):
        output = list(iter_threaded(iter([]), _identity, num_workers=4))
        assert output == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
