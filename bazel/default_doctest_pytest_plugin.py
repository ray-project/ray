"""This file is injected for all doctest targets in the repo by default."""
import doctest

import pytest

import ray

# `# doctest: +SKIP_EXAMPLE` on any line of a docstring's `>>>` examples skips that
# whole docstring. Use it for render-only examples that can't run in CI, such as
# ones that need TPUs or an external service: the flag goes on one line instead of
# `+SKIP` on every statement, and Sphinx and griffe strip it from the rendered page.
# Keep in sync with python/ray/data/tests/doctest_pytest_plugin.py.
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
