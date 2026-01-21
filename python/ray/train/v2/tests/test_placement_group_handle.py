from unittest.mock import MagicMock

import pytest

import ray
from ray.train.v2._internal.execution.worker_group.placement_group_handle import (
    DefaultPlacementGroupHandle,
    PlacementGroupHandle,
    SlicePlacementGroupHandle,
)
from ray.util.placement_group import placement_group


@pytest.fixture(autouse=True)
def ray_start():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


# DefaultPlacementGroupHandle tests


def test_default_handle_is_placement_group_handle():
    """DefaultPlacementGroupHandle should be a PlacementGroupHandle."""
    pg = placement_group([{"CPU": 1}])
    handle = DefaultPlacementGroupHandle(pg)
    assert isinstance(handle, PlacementGroupHandle)
    handle.shutdown()


def test_default_handle_placement_group_property():
    """placement_group property should return the underlying PlacementGroup."""
    pg = placement_group([{"CPU": 1}])
    handle = DefaultPlacementGroupHandle(pg)
    assert handle.placement_group is pg
    handle.shutdown()


def test_default_handle_ready():
    """ready() should return an ObjectRef that can be waited on."""
    pg = placement_group([{"CPU": 1}])
    handle = DefaultPlacementGroupHandle(pg)

    ready_ref = handle.ready()
    # Should be able to ray.get the ready ref
    ray.get(ready_ref, timeout=10)
    handle.shutdown()


def test_default_handle_shutdown():
    """shutdown() should remove the placement group."""
    pg = placement_group([{"CPU": 1}])
    handle = DefaultPlacementGroupHandle(pg)

    # Wait for PG to be ready
    ray.get(handle.ready(), timeout=10)

    handle.shutdown()


# SlicePlacementGroupHandle tests


def test_slice_handle_is_placement_group_handle():
    """SlicePlacementGroupHandle should be a PlacementGroupHandle."""
    mock_spg = MagicMock()
    mock_pg = MagicMock()
    mock_spg.placement_group = mock_pg

    handle = SlicePlacementGroupHandle(mock_spg)
    assert isinstance(handle, PlacementGroupHandle)


def test_slice_handle_placement_group_property():
    """placement_group property should return the underlying PlacementGroup."""
    mock_spg = MagicMock()
    mock_pg = MagicMock()
    mock_spg.placement_group = mock_pg

    handle = SlicePlacementGroupHandle(mock_spg)
    assert handle.placement_group is mock_pg


def test_slice_handle_ready():
    """ready() should delegate to the underlying PlacementGroup."""
    mock_spg = MagicMock()
    mock_pg = MagicMock()
    mock_ready_ref = MagicMock()
    mock_pg.ready.return_value = mock_ready_ref
    mock_spg.placement_group = mock_pg

    handle = SlicePlacementGroupHandle(mock_spg)
    result = handle.ready()

    mock_pg.ready.assert_called_once()
    assert result is mock_ready_ref


def test_slice_handle_shutdown():
    """shutdown() should call shutdown on the SlicePlacementGroup."""
    mock_spg = MagicMock()
    mock_pg = MagicMock()
    mock_spg.placement_group = mock_pg

    handle = SlicePlacementGroupHandle(mock_spg)
    handle.shutdown()

    mock_spg.shutdown.assert_called_once()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
