import logging
import random
import sys
import time
from os import urandom

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


def test_resolve_block_refs_batches(ray_start_regular_shared, monkeypatch):
    ctx = ray.data.DataContext.get_current()
    old_batch_size = ctx.iter_get_block_batch_size
    ctx.iter_get_block_batch_size = 2

    call_sizes = []
    original_get = ray.get

    def recording_get(refs, *args, **kwargs):
        if isinstance(refs, list):
            call_sizes.append(len(refs))
        else:
            call_sizes.append(1)
        return original_get(refs, *args, **kwargs)

    monkeypatch.setattr(ray, "get", recording_get)

    block_refs = [ray.put(i) for i in range(5)]

    try:
        assert list(resolve_block_refs(iter(block_refs))) == list(range(5))
    finally:
        ctx.iter_get_block_batch_size = old_batch_size

    assert call_sizes == [2, 2, 1]


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
    assert hits == -1
    assert misses == -1
    assert unknowns == -1

    ctx = ray.data.context.DataContext.get_current()
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
