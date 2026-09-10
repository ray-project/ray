"""This file is injected for Ray Data doctest targets."""
import os

import pytest

import ray

# Keep the footer-reader pool tiny: doctests read small Parquet fixtures, and
# the default 32-actor pool can trip Ray's "too many worker processes" warning,
# which pollutes Sphinx ``testoutput`` expectations. Mirrored in
# python/ray/data/test.bzl for bazel doctest targets.
os.environ.setdefault("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")


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
