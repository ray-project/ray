import logging

import pytest

import ray


@pytest.fixture(autouse=True)
def disallow_ray_init(monkeypatch):
    def raise_on_init():
        raise RuntimeError("Unit tests should not depend on Ray being initialized.")

    monkeypatch.setattr(ray, "init", raise_on_init)


@pytest.fixture
def propagate_logs():
    # Mirrors python/ray/tests/conftest.py::propagate_logs. The bazel target
    # for this directory does not pull the parent conftest, so unit tests that
    # use caplog against ray-namespaced loggers need a local copy.
    logging.getLogger("ray").propagate = True
    logging.getLogger("ray.data").propagate = True
    yield
