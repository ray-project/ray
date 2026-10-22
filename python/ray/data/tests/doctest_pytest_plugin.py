"""This file is injected for Ray Data doctest targets."""
import doctest
import os

import pytest

import ray

# Keep the footer-reader pool tiny: doctests read small Parquet fixtures, and
# the default 32-actor pool can trip Ray's "too many worker processes" warning,
# which pollutes Sphinx ``testoutput`` expectations. Mirrored in
# python/ray/data/test.bzl for bazel doctest targets.
os.environ.setdefault("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")


# `# doctest: +SKIP_EXAMPLE` on any line of a docstring's `>>>` examples skips that
# whole docstring. Use it for render-only examples that can't run in CI, such as
# ones that need TPUs or an external service: the flag goes on one line instead of
# `+SKIP` on every statement, and Sphinx and griffe strip it from the rendered page.
# Keep in sync with bazel/default_doctest_pytest_plugin.py.
SKIP_EXAMPLE = doctest.register_optionflag("SKIP_EXAMPLE")


def pytest_collection_modifyitems(config, items):
    for item in items:
        dtest = getattr(item, "dtest", None)
        if dtest is not None and any(
            example.options.get(SKIP_EXAMPLE) for example in dtest.examples
        ):
            item.add_marker(
                pytest.mark.skip(reason="render-only example (+SKIP_EXAMPLE)")
            )


@pytest.fixture(autouse=True, scope="module")
def shutdown_ray():
    ray.shutdown()
    yield


@pytest.fixture(autouse=True)
def preserve_block_order():
    ray.data.context.DataContext.get_current().execution_options.preserve_order = True
    yield


@pytest.fixture(autouse=True)
def disable_start_message():
    context = ray.data.context.DataContext.get_current()
    original_value = context.print_on_execution_start
    context.print_on_execution_start = False
    yield
    context.print_on_execution_start = original_value
