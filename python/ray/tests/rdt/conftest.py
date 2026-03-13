"""Pytest fixtures for RDT tests on the Rust Ray backend."""

import pytest
import ray


@pytest.fixture
def ray_start_regular(request):
    """Start a local Ray cluster with the given parameters.

    Matches the C++ backend's ray_start_regular fixture:
    @pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
    """
    param = getattr(request, "param", {})
    ray.init(**param)
    yield {}
    ray.shutdown()
