# coding: utf-8
import logging
import ray
import os
import sys
import pytest

logger = logging.getLogger(__name__)


def test_no_accidental_dependenies(ray_start_regular):
    # Internal dependencies are not present in worker or in driver.

    def has_any_internal_deps():
        try:
            import aiohttp
        except ImportError:
            return True
        try:
            import aiosignal
        except ImportError:
            return True
        try:
            import aiohttp_cors
        except ImportError:
            return True
        return False

    assert not has_any_internal_deps()

    @ray.remote
    def f():
        return has_any_internal_deps()

    assert not ray.get(f.remote())

    @ray.remote
    class A:
        def m(self):
            return has_any_internal_deps()

    a = A.remote()
    assert not ray.get(a.m.remote())


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
