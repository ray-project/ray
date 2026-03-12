"""Test backend registry API.

This test file demonstrates how to register and use custom collective backends
with ray.util.collective module. Custom backends can be registered
to support alternative collective communication libraries (e.g., HCCL, XCCL, etc.)
"""
import pytest

from ray.util.collective import (
    is_backend_available,
    is_registered_collective_backend,
    register_collective_backend,
)
from ray.util.collective.collective_group.base_collective_group import BaseGroup


class MockGroup(BaseGroup):
    """Mock collective group for testing.

    This example shows the minimal interface required for a custom backend:
    1. Inherit from BaseGroup
    2. Implement __init__ with (world_size, rank, group_name) parameters
    3. Implement backend() classmethod returning backend name
    4. Implement check_backend_availability() classmethod returning bool
    5. Implement required collective operations (allreduce, barrier, etc.)
    """

    def __init__(self, world_size, rank, group_name):
        super().__init__(world_size, rank, group_name)

    @classmethod
    def backend(cls):
        return "MOCK"

    @classmethod
    def check_backend_availability(cls) -> bool:
        return True


class CustomBackendWithTimeout(BaseGroup):
    """Example of a custom backend that accepts additional parameters.

    This demonstrates how to create backends with custom __init__ signatures.
    The registry system will dynamically inspect the signature and only
    pass parameters that the backend accepts.
    """

    def __init__(self, world_size, rank, group_name, custom_timeout=None):
        super().__init__(world_size, rank, group_name)
        self.custom_timeout = custom_timeout

    @classmethod
    def backend(cls):
        return "CUSTOM_WITH_TIMEOUT"

    @classmethod
    def check_backend_availability(cls) -> bool:
        return True


@pytest.fixture
def clean_registry():
    """Fixture to clean the backend registry before and after each test."""
    from ray.util.collective import backend_registry

    registry = backend_registry._global_registry
    original_backends = registry._map.copy()
    registry._map.clear()
    yield
    registry._map.clear()
    registry._map.update(original_backends)


def test_register_collective_backend(clean_registry):
    """Test registering a custom collective backend.

    This is the basic usage pattern for registering custom backends.
    After registration, the backend can be used with init_collective_group
    and create_collective_group functions.
    """
    register_collective_backend("mock", MockGroup)

    assert is_registered_collective_backend("mock") is True
    assert is_backend_available("mock") is True


def test_register_duplicate_backend(clean_registry):
    """Test that registering duplicate backend raises error."""
    register_collective_backend("mock", MockGroup)

    with pytest.raises(ValueError, match="Backend MOCK already registered"):
        register_collective_backend("mock", MockGroup)


def test_register_invalid_backend(clean_registry):
    """Test that registering non-BaseGroup subclass raises error."""

    class NotABaseGroup:
        pass

    with pytest.raises(TypeError, match="is not a subclass of BaseGroup"):
        register_collective_backend("invalid", NotABaseGroup)


def test_is_backend_available(clean_registry):
    """Test high-level is_backend_available API.

    This function checks both registration and runtime availability.
    """
    register_collective_backend("mock", MockGroup)

    assert is_backend_available("mock") is True
    assert is_backend_available("nonexistent") is False


def test_is_registered_collective_backend(clean_registry):
    """Test checking if a backend is registered.

    This function only checks registration status, not runtime availability.
    It's useful for users who want to know if a backend has been
    registered without attempting to load the backend library.
    """
    assert is_registered_collective_backend("mock") is False

    register_collective_backend("mock", MockGroup)
    assert is_registered_collective_backend("mock") is True
    assert is_registered_collective_backend("MOCK") is True


def test_backend_with_custom_parameters(clean_registry):
    """Test backend with custom __init__ parameters.

    The registry system dynamically inspects the backend's __init__
    signature and only passes parameters that are accepted. This allows
    backends to define their own custom parameters without breaking
    the core collective API.
    """
    register_collective_backend("custom_timeout", CustomBackendWithTimeout)

    assert is_registered_collective_backend("custom_timeout") is True


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
