import pytest

from ray.data._internal.dynamic_work_queue import parallel_process_work_stealing


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_flat(num_workers):
    """Flat (non-recursive) processing: every seed item produces exactly one
    result with no dynamically-added work."""

    def process(item, add_work, add_result):
        add_result(item * 10)

    results = list(
        parallel_process_work_stealing(
            [1, 2, 3, 4, 5],
            process,
            num_workers=num_workers,
        )
    )

    assert sorted(results) == [10, 20, 30, 40, 50]


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_flat_ordered(num_workers):
    """Flat processing with preserve_order sorts by order_key."""

    def process(item, add_work, add_result):
        add_result(item * 10)

    results = list(
        parallel_process_work_stealing(
            [3, 1, 5, 2, 4],
            process,
            num_workers=num_workers,
            preserve_order=True,
            order_key=lambda x: x,
        )
    )

    assert results == [10, 20, 30, 40, 50]


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_recursive(num_workers):
    """Recursive work generation: processing one item spawns sub-items,
    mimicking a tree traversal."""

    tree = {
        "a": ["a/1", "a/2"],
        "b": ["b/1"],
    }

    def process(item, add_work, add_result):
        if item in tree:
            for child in tree[item]:
                add_work(child)
        else:
            add_result(item)

    results = list(
        parallel_process_work_stealing(
            ["a", "b"],
            process,
            num_workers=num_workers,
        )
    )

    assert sorted(results) == ["a/1", "a/2", "b/1"]


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_recursive_ordered(num_workers):
    """Recursive work generation with preserve_order sorts all results
    globally by order_key."""

    tree = {
        "a": ["a/2", "a/1"],
        "b": ["b/1"],
    }

    def process(item, add_work, add_result):
        if item in tree:
            for child in tree[item]:
                add_work(child)
        else:
            add_result(item)

    results = list(
        parallel_process_work_stealing(
            ["a", "b"],
            process,
            num_workers=num_workers,
            preserve_order=True,
            order_key=lambda x: x,
        )
    )

    assert results == ["a/1", "a/2", "b/1"]


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_deep_chain(num_workers):
    """A single seed item spawns a long chain of work (depth > breadth)."""

    def process(item, add_work, add_result):
        if item > 0:
            add_work(item - 1)
        add_result(item)

    results = list(
        parallel_process_work_stealing(
            [5],
            process,
            num_workers=num_workers,
        )
    )

    assert sorted(results) == [0, 1, 2, 3, 4, 5]


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_deep_chain_ordered(num_workers):
    """Deep chain with preserve_order sorts results by order_key."""

    def process(item, add_work, add_result):
        if item > 0:
            add_work(item - 1)
        add_result(item)

    results = list(
        parallel_process_work_stealing(
            [5],
            process,
            num_workers=num_workers,
            preserve_order=True,
            order_key=lambda x: x,
        )
    )

    assert results == [0, 1, 2, 3, 4, 5]


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_error_propagation(num_workers):
    """Exceptions raised in process_fn are propagated to the caller."""

    def process(item, add_work, add_result):
        if item == 3:
            raise ValueError("boom")
        add_result(item)

    with pytest.raises(ValueError, match="boom"):
        list(
            parallel_process_work_stealing(
                [1, 2, 3, 4],
                process,
                num_workers=num_workers,
            )
        )


def test_parallel_process_work_stealing_empty_seeds():
    """No seed items produces an empty generator."""
    results = list(
        parallel_process_work_stealing([], lambda item, aw, ar: None, num_workers=1)
    )
    assert results == []


def test_parallel_process_work_stealing_no_results():
    """Seed items that produce no results yield an empty sequence."""

    def process(item, add_work, add_result):
        pass  # intentionally produce nothing

    results = list(parallel_process_work_stealing([1, 2, 3], process, num_workers=2))
    assert results == []


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_preserve_order_sorts_globally(num_workers):
    """With preserve_order=True, all results are sorted globally by
    order_key regardless of which seed item produced them."""

    def process(item, add_work, add_result):
        for suffix in ["c", "a", "b"]:
            add_result(f"{item}-{suffix}")

    results = list(
        parallel_process_work_stealing(
            ["X", "Y"],
            process,
            num_workers=num_workers,
            preserve_order=True,
            order_key=lambda x: x,
        )
    )

    assert results == ["X-a", "X-b", "X-c", "Y-a", "Y-b", "Y-c"]


def test_parallel_process_work_stealing_preserve_order_requires_order_key():
    """preserve_order=True without order_key raises ValueError."""
    with pytest.raises(ValueError, match="order_key is required"):
        list(
            parallel_process_work_stealing(
                [1],
                lambda item, aw, ar: ar(item),
                preserve_order=True,
            )
        )


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_early_stop(num_workers):
    """Generator can be stopped early (via break) without hanging."""

    def process(item, add_work, add_result):
        add_result(item)
        # Spawn more work to ensure the queue isn't empty at break time.
        if item < 100:
            add_work(item + 1)

    gen = parallel_process_work_stealing(
        [0],
        process,
        num_workers=num_workers,
        preserve_order=False,
    )

    collected = []
    for result in gen:
        collected.append(result)
        if len(collected) >= 5:
            break

    assert len(collected) == 5


def test_parallel_process_work_stealing_invalid_num_workers():
    """num_workers < 1 raises ValueError."""
    with pytest.raises(ValueError, match="num_workers must be at least 1"):
        list(
            parallel_process_work_stealing(
                [1], lambda item, aw, ar: ar(item), num_workers=0
            )
        )


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_early_stop_no_thread_leak(num_workers):
    """Threads spawned by the generator must not leak after early termination."""
    import threading
    import time

    baseline = threading.active_count()

    def process(item, add_work, add_result):
        add_result(item)
        if item < 1000:
            add_work(item + 1)

    gen = parallel_process_work_stealing(
        [0],
        process,
        num_workers=num_workers,
        preserve_order=False,
    )

    collected = []
    for result in gen:
        collected.append(result)
        if len(collected) >= 3:
            break

    assert len(collected) == 3

    time.sleep(3)
    assert threading.active_count() <= baseline + 1


def test_parallel_process_work_stealing_error_clears_exception():
    """_WorkerError.exception should be cleared after re-raising to avoid
    traceback reference cycles that delay GC of worker-frame locals."""
    import gc
    import weakref

    class BigObject:
        pass

    refs: "list[weakref.ref]" = []

    def process(item, add_work, add_result):
        obj = BigObject()
        refs.append(weakref.ref(obj))
        raise ValueError("test error")

    with pytest.raises(ValueError, match="test error"):
        list(parallel_process_work_stealing([1], process, num_workers=1))

    gc.collect()
    assert refs[0]() is None, "BigObject leaked via traceback reference cycle"


@pytest.mark.parametrize("num_workers", [1, 4])
def test_parallel_process_work_stealing_error_in_dynamically_added_work(num_workers):
    """Errors raised in dynamically-added (non-seed) work items must still
    propagate to the caller."""

    def process(item, add_work, add_result):
        if item == "child":
            raise RuntimeError("child error")
        add_work("child")
        add_result(item)

    with pytest.raises(RuntimeError, match="child error"):
        list(
            parallel_process_work_stealing(
                ["root"],
                process,
                num_workers=num_workers,
            )
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
