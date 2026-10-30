from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event, get_ident
from types import GeneratorType

import pytest

from ray.data._internal.execution.util import make_callable_class_single_threaded


def _get_executor_thread_id(udf):
    return udf.thread_pool_executor.submit(get_ident).result(timeout=10)


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


@pytest.mark.parametrize(
    "finish",
    ["exhaust", "close", "raise_before_yield", "raise_after_yield", "close_raises"],
)
def test_generator_serializes_calls_until_finished(wrap_udf, finish):
    error = ValueError("UDF failed")
    cleanup_calls = []

    class UDF:
        def __call__(self, value):
            self.buffer = value
            try:
                if value == 0 and finish == "raise_before_yield":
                    raise error
                yield self.buffer
                if value == 0 and finish == "raise_after_yield":
                    raise error
                yield self.buffer
            finally:
                cleanup_calls.append((value, get_ident()))
                if value == 0 and finish == "close_raises":
                    raise error

    udf = wrap_udf(UDF)
    executor_thread = _get_executor_thread_id(udf)
    first = udf(0)
    second_generator = udf(1)
    contender_started = Event()

    def consume_second():
        contender_started.set()
        return list(second_generator)

    with ThreadPoolExecutor(max_workers=1) as callers:
        try:
            if finish == "raise_before_yield":
                with pytest.raises(ValueError) as exc_info:
                    next(first)
                assert exc_info.value is error
                assert cleanup_calls == [(0, executor_thread)]
            else:
                assert next(first) == 0
                assert cleanup_calls == []

            second = callers.submit(consume_second)
            assert contender_started.wait(timeout=10)
            if finish != "raise_before_yield":
                with pytest.raises(TimeoutError):
                    second.result(timeout=0.1)

                # A waiting caller must not block the worker needed to resume the UDF.
                assert _get_executor_thread_id(udf) == executor_thread
                if finish == "exhaust":
                    assert list(first) == [0]
                elif finish == "close":
                    first.close()
                else:
                    with pytest.raises(ValueError) as exc_info:
                        if finish == "raise_after_yield":
                            next(first)
                        else:
                            first.close()
                    assert exc_info.value is error

            assert second.result(timeout=10) == [1, 1]
            assert cleanup_calls == [(0, executor_thread), (1, executor_thread)]
            first.close()
            second_generator.close()
            assert cleanup_calls == [(0, executor_thread), (1, executor_thread)]
        finally:
            first.close()


@pytest.mark.parametrize("started", [False, True])
def test_closed_generator_does_not_block_other_calls(wrap_udf, started):
    class UDF:
        def __call__(self, value):
            yield value
            yield value + 1

    udf = wrap_udf(UDF)
    first = udf(0)
    try:
        if started:
            assert next(first) == 0
        else:
            assert list(udf(1)) == [1, 2]
    finally:
        first.close()
    assert list(udf(2)) == [2, 3]


def test_generator_is_lazy(wrap_udf):
    events = []

    class UDF:
        def __call__(self):
            events.append("start")
            yield 1

    generator = wrap_udf(UDF)()
    try:
        assert isinstance(generator, GeneratorType)
        assert events == []
        assert next(generator) == 1
        assert events == ["start"]
    finally:
        generator.close()


def test_single_threaded_udf_returns_regular_result(wrap_udf):
    class UDF:
        def __call__(self, value, *, increment):
            return value + increment

    assert wrap_udf(UDF)(2, increment=3) == 5


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
