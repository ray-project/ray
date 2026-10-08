from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Barrier, Event, get_ident
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
            self.buffer = value
            for step in range(3):
                yield self.buffer, step, get_ident()

    udf = wrap_udf(UDF)
    executor_thread = udf.thread_pool_executor.submit(get_ident).result()
    barrier = Barrier(2, timeout=10)

    def consume(value):
        barrier.wait()
        generator = udf(value)
        try:
            return get_ident(), list(generator)
        finally:
            generator.close()

    with ThreadPoolExecutor(max_workers=2) as callers:
        futures = [callers.submit(consume, value) for value in range(2)]
        results = [future.result(timeout=15) for future in futures]

    assert len({thread for thread, _ in results}) == 2
    for value, (caller_thread, outputs) in enumerate(results):
        assert caller_thread != executor_thread
        assert outputs == [(value, step, executor_thread) for step in range(3)]


@pytest.mark.parametrize("finish", ["exhaust", "close", "raise", "close_raises"])
def test_generator_serializes_calls_until_finished(wrap_udf, finish):
    error = ValueError("UDF failed")
    cleanup_threads = []

    class UDF:
        def __call__(self, value):
            self.buffer = value
            try:
                yield self.buffer
                if value == 0 and finish == "raise":
                    raise error
                yield self.buffer
            finally:
                cleanup_threads.append(get_ident())
                if value == 0 and finish == "close_raises":
                    raise error

    udf = wrap_udf(UDF)
    executor_thread = udf.thread_pool_executor.submit(get_ident).result()
    first = udf(0)
    second_generator = udf(1)
    contender_started = Event()

    def consume_second():
        contender_started.set()
        return list(second_generator)

    with ThreadPoolExecutor(max_workers=1) as callers:
        try:
            assert next(first) == 0
            second = callers.submit(consume_second)
            assert contender_started.wait(timeout=10)
            with pytest.raises(TimeoutError):
                second.result(timeout=0.1)

            # A waiting caller must not block the worker needed to resume the UDF.
            assert (
                udf.thread_pool_executor.submit(get_ident).result(timeout=10)
                == executor_thread
            )
            if finish == "exhaust":
                assert list(first) == [0]
            elif finish == "close":
                first.close()
            else:
                with pytest.raises(ValueError) as exc_info:
                    if finish == "raise":
                        next(first)
                    else:
                        first.close()
                assert exc_info.value is error

            assert second.result(timeout=10) == [1, 1]
            assert cleanup_threads == [executor_thread] * 2
        finally:
            first.close()


def test_unstarted_generator_does_not_block_other_calls(wrap_udf):
    class UDF:
        def __call__(self, value):
            yield value

    udf = wrap_udf(UDF)
    first = udf(0)
    try:
        assert list(udf(1)) == [1]
    finally:
        first.close()
    assert list(udf(2)) == [2]


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
