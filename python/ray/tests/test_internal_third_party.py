# coding: utf-8
import logging
import ray
import os
import sys
import pytest
import importlib

logger = logging.getLogger(__name__)


def test_no_accidental_dependenies(ray_start_regular):
    # Internal dependencies are not present in worker or in driver.

    def has_module(name):
        try:
            importlib.import_module(name)
            return True
        except ImportError:
            return False

    def expect_no_internal_deps():
        """
        Asserts no internal deps exist in this environment.
        If any, raises error with the import result.
        """
        deps = ["aiohttp", "aiohttp_cors", "aiosignal"]
        has_deps = {name: has_module(name) for name in deps}
        if any(has_deps.values()):
            raise ValueError(f"unexpectedly imported some internal deps: {has_deps}")
        return True

    assert expect_no_internal_deps()

    @ray.remote
    def f():
        return expect_no_internal_deps()

    assert ray.get(f.remote())

    @ray.remote
    class A:
        def m(self):
            return expect_no_internal_deps()

    a = A.remote()
    assert ray.get(a.m.remote())


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
