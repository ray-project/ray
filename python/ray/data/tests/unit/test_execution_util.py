from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, get_ident
from types import GeneratorType

import pytest

from ray.data._internal.execution.util import make_callable_class_single_threaded


@pytest.fixture
def wrap_udf():
    instances = []

    def wrap(cls):
        instance = make_callable_class_single_threaded(cls)()
        instances.append(instance)
        return instance

    yield wrap

    for instance in instances:
        instance.thread_pool_executor.shutdown()


def test_generator_steps_run_on_single_thread(wrap_udf):
    class UDF:
        def __call__(self, value):
            for step in range(3):
                yield value, step, get_ident()

    udf = wrap_udf(UDF)
    executor_thread = udf.thread_pool_executor.submit(get_ident).result()
    barrier = Barrier(2, timeout=10)

    def consume(value):
        generator = udf(value)
        outputs = []
        try:
            for _ in range(3):
                # Synchronize callers, not UDF bodies, which must run serially.
                barrier.wait()
                outputs.append(next(generator))
            with pytest.raises(StopIteration):
                next(generator)
            return get_ident(), outputs
        finally:
            generator.close()

    with ThreadPoolExecutor(max_workers=2) as callers:
        futures = [callers.submit(consume, value) for value in range(2)]
        results = [future.result(timeout=15) for future in futures]

    assert len({thread for thread, _ in results}) == 2
    for value, (caller_thread, outputs) in enumerate(results):
        assert caller_thread != executor_thread
        assert outputs == [(value, step, executor_thread) for step in range(3)]


@pytest.mark.parametrize("num_outputs", [0, 3])
def test_generator_is_lazy(wrap_udf, num_outputs):
    events = []

    class UDF:
        def __call__(self):
            events.append("start")
            for value in range(num_outputs):
                events.append(value)
                yield value
            events.append("end")

    generator = wrap_udf(UDF)()
    try:
        assert isinstance(generator, GeneratorType)
        assert events == []
        for value in range(num_outputs):
            assert next(generator) == value
            assert events == ["start", *range(value + 1)]
        with pytest.raises(StopIteration):
            next(generator)
        assert events == ["start", *range(num_outputs), "end"]
    finally:
        generator.close()


def test_single_threaded_udf_returns_regular_result(wrap_udf):
    class UDF:
        def __call__(self, value, *, increment):
            return value + increment

    assert wrap_udf(UDF)(2, increment=3) == 5


@pytest.mark.parametrize("yield_before_error", [False, True])
def test_generator_exception_propagates(wrap_udf, yield_before_error):
    error = ValueError("UDF failed")
    cleanup_threads = []

    class UDF:
        def __call__(self):
            try:
                if yield_before_error:
                    yield 1
                raise error
            finally:
                cleanup_threads.append(get_ident())

    udf = wrap_udf(UDF)
    executor_thread = udf.thread_pool_executor.submit(get_ident).result()
    generator = udf()
    try:
        if yield_before_error:
            assert next(generator) == 1
        with pytest.raises(ValueError) as exc_info:
            next(generator)
        assert exc_info.value is error
        assert cleanup_threads == [executor_thread]
    finally:
        generator.close()


@pytest.mark.parametrize("exhaust", [False, True])
def test_generator_cleanup_runs_on_single_thread(wrap_udf, exhaust):
    cleanup_threads = []

    class UDF:
        def __call__(self):
            try:
                yield 1
                yield 2
            finally:
                cleanup_threads.append(get_ident())

    udf = wrap_udf(UDF)
    executor_thread = udf.thread_pool_executor.submit(get_ident).result()
    generator = udf()
    try:
        assert next(generator) == 1
        assert cleanup_threads == []
        if exhaust:
            assert list(generator) == [2]
        else:
            generator.close()
        assert cleanup_threads == [executor_thread]
        generator.close()
        assert cleanup_threads == [executor_thread]
    finally:
        generator.close()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
