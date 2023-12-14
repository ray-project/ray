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

    def get_module(name):
        try:
            module = importlib.import_module(name)
            return module
        except ImportError:
            return None

    def expect_no_internal_deps():
        """
        Asserts no internal deps exist in this environment.
        If any, raises error with the import result.
        """
        deps = ["aiohttp", "aiohttp_cors", "aiosignal"]
        imported_deps = {name: get_module(name) for name in deps}
        if any(imported_deps.values()):
            raise ValueError(
                f"unexpectedly imported some internal deps: {imported_deps}"
            )
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


def test_cant_import_internal_dependencies():
    with pytest.raises(
        ImportError,
        match="ray._private.internal_third_party is only for Ray internal use.",
    ):
        from ray._private.internal_third_party import aiohttp  # noqa: F401


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
