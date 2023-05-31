from __future__ import annotations

import pytest

import ray


@pytest.fixture(autouse=True, scope="module")
def shutdown_ray():
    ray.shutdown()
    yield

@pytest.fixture(autouse=True, scope="session")
def import_ray(doctest_namespace):
    doctest_namespace["ray"] = ray
