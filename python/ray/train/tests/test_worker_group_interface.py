"""Tests for WorkerGroupInterface implementation and usage."""

import pytest

from ray.train._internal.worker_group import WorkerGroup as V1WorkerGroup
from ray.train._internal.worker_group_interface import WorkerGroupInterface
from ray.train.v2._internal.execution.worker_group.worker_group import (
    WorkerGroup as V2WorkerGroup,
)


def test_interface_abstract_methods():
    """Test that WorkerGroupInterface enforces its abstract methods."""
    # Should not be able to instantiate interface directly
    with pytest.raises(TypeError):
        WorkerGroupInterface()

    # Should not be able to create incomplete implementation
    class IncompleteWorkerGroup(WorkerGroupInterface):
        def execute(self, func, *args, **kwargs):
            pass

        # Missing other abstract methods

    with pytest.raises(TypeError):
        IncompleteWorkerGroup()


def test_real_implementations_inherit_interface():
    """Smoke test that real WorkerGroup implementations inherit from interface."""
    # Test inheritance
    assert issubclass(V1WorkerGroup, WorkerGroupInterface)
    assert issubclass(V2WorkerGroup, WorkerGroupInterface)

    # Test that all abstract methods are implemented
    # If any abstract methods are missing, __abstractmethods__ will be non-empty
    assert (
        len(V1WorkerGroup.__abstractmethods__) == 0
    ), f"V1 WorkerGroup missing abstract methods: {V1WorkerGroup.__abstractmethods__}"
    assert (
        len(V2WorkerGroup.__abstractmethods__) == 0
    ), f"V2 WorkerGroup missing abstract methods: {V2WorkerGroup.__abstractmethods__}"


if __name__ == "__main__":
    pytest.main([__file__])
