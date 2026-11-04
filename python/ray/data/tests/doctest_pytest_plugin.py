"""This file is injected for Ray Data doctest targets."""
import doctest
import inspect
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


# Ray Train and Tune overwrite `__module__` on public re-exports in their package
# `__init__.py`, so `ray.train.report` presents as `ray.train` rather than its
# defining module. Doctest only collects an object from the module its
# `__module__` names, and the `doctest()` macro never scans package `__init__.py`
# files, so no target would collect these docstrings. Also accept a function or
# class whose code is defined in the module being scanned.
_doctest_from_module = vars(doctest.DocTestFinder)["_from_module"]


def _defines(module, obj):
    if inspect.isclass(obj):
        members = list(vars(obj).values())
    elif inspect.isfunction(obj):
        members = [obj]
    else:
        return False
    for member in members:
        if isinstance(member, (staticmethod, classmethod)):
            member = member.__func__
        elif isinstance(member, property):
            member = member.fget
        if inspect.isfunction(member):
            member = inspect.unwrap(member)
            if member.__globals__ is module.__dict__:
                return True
    return False


def _from_module(self, module, object):
    return _doctest_from_module(self, module, object) or (
        module is not None and _defines(module, object)
    )


doctest.DocTestFinder._from_module = _from_module


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
