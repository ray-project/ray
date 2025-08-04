import sys
from collections.abc import AsyncIterator

import pytest

from ray._private.async_compat import has_async_methods, is_async_func, sync_to_async


def test_is_async_func():
    def f():
        return 1

    def f_gen():
        yield 1

    async def g():
        return 1

    async def g_gen():
        yield 1

    assert is_async_func(f) is False
    assert is_async_func(f_gen) is False
    assert is_async_func(g) is True
    assert is_async_func(g_gen) is True


class NoAsyncMethods:
    def sync_fn(self) -> None:
        pass


class YesAsyncMethods:
    async def async_fn(self) -> None:
        pass


class YesAsyncGenMethods:
    async def async_gen_fn(self) -> AsyncIterator[None]:
        yield None


@pytest.mark.parametrize(
    "cls, expected",
    [
        (NoAsyncMethods, False),
        (YesAsyncMethods, True),
        (YesAsyncGenMethods, True),
    ],
)
def test_has_async_methods(cls, expected: bool) -> None:
    assert has_async_methods(cls) is expected


def test_sync_to_async_is_cached() -> None:
    def sync_fn() -> None:
        pass

    async def async_fn() -> None:
        pass

    assert sync_to_async(sync_fn) is sync_to_async(sync_fn)
    assert sync_to_async(async_fn) is sync_to_async(async_fn)

    assert sync_to_async(sync_fn) is not sync_to_async(async_fn)


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
