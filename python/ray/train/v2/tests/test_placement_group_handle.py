from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.train.v2._internal.execution.worker_group.placement_group_handle import (
    DefaultPlacementGroupHandle,
    PlacementGroupHandle,
    SlicePlacementGroupHandle,
)
from ray.util.placement_group import PlacementGroup, placement_group


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


def test_default_handle_wait():
    """wait() should return True if the placement group is ready."""
    pg = placement_group([{"CPU": 1}])
    handle = DefaultPlacementGroupHandle(pg)
    assert handle.wait(timeout_seconds=10)
    handle.shutdown()


def test_default_handle_wait_not_found_returns_false():
    """wait() should return False if the placement group no longer exists."""
    mock_pg = MagicMock()
    mock_pg.wait.side_effect = Exception(
        "Placement group PlacementGroupID(abc) does not exist."
    )
    handle = DefaultPlacementGroupHandle(mock_pg)
    assert handle.wait(timeout_seconds=0) is False


def test_default_handle_shutdown():
    """shutdown() should remove the placement group."""
    pg = placement_group([{"CPU": 1}])
    handle = DefaultPlacementGroupHandle(pg)

    assert handle.wait(timeout_seconds=10)

    handle.shutdown()


# SlicePlacementGroupHandle tests


def test_slice_handle_is_placement_group_handle():
    """SlicePlacementGroupHandle should be a PlacementGroupHandle."""
    mock_spg = MagicMock()

    handle = SlicePlacementGroupHandle(mock_spg)
    assert isinstance(handle, PlacementGroupHandle)


def test_slice_handle_placement_group_property():
    """placement_group property should return the underlying PlacementGroup."""
    mock_spg = MagicMock()
    mock_pg = MagicMock()
    mock_spg.placement_group = mock_pg

    handle = SlicePlacementGroupHandle(mock_spg)
    assert handle.placement_group is mock_pg


def test_slice_handle_wait():
    """wait() should delegate to the underlying PlacementGroup(s)."""
    mock_spg = MagicMock()
    mock_pg = MagicMock(spec=PlacementGroup)
    mock_pg.wait.return_value = True
    mock_spg.placement_groups = [mock_pg]

    handle = SlicePlacementGroupHandle(mock_spg)
    assert handle.wait(timeout_seconds=10)
    mock_pg.wait.assert_called_once()

    # Sub-second positive timeout is rounded up to 1s via math.ceil so Cython
    # int64_t conversion in CoreWorker.wait_placement_group_ready does not truncate to 0.
    mock_pg.reset_mock()
    assert handle.wait(timeout_seconds=0.5)
    mock_pg.wait.assert_called_once_with(1)

    # Infinite timeout clamps cleanly to a positive finite integer instead of raising OverflowError.
    mock_pg.reset_mock()
    assert handle.wait(timeout_seconds=float("inf")) is True
    mock_pg.wait.assert_called_once_with(2**31 - 1)

    # NaN timeout returns False immediately without calling pg.wait.
    mock_pg.reset_mock()
    assert handle.wait(timeout_seconds=float("nan")) is False
    mock_pg.wait.assert_not_called()


def test_slice_handle_multi_slice_wait():
    """wait() should wait on all underlying PlacementGroups for multi-slice TPUs."""
    mock_spg = MagicMock()
    mock_pg1 = MagicMock(spec=PlacementGroup)
    mock_pg2 = MagicMock(spec=PlacementGroup)
    mock_pg1.wait.return_value = True
    mock_pg2.wait.return_value = True
    mock_spg.placement_groups = [mock_pg1, mock_pg2]

    handle = SlicePlacementGroupHandle(mock_spg)
    assert handle.placement_groups == [mock_pg1, mock_pg2]
    assert handle.wait(timeout_seconds=10)
    mock_pg1.wait.assert_called_once()
    mock_pg2.wait.assert_called_once()

    # Short-circuit on first PG timeout
    mock_pg1.reset_mock()
    mock_pg2.reset_mock()
    mock_pg1.wait.return_value = False
    assert not handle.wait(timeout_seconds=10)
    mock_pg1.wait.assert_called_once()
    mock_pg2.wait.assert_not_called()

    # Second PG timeout after first PG succeeds
    mock_pg1.reset_mock()
    mock_pg2.reset_mock()
    mock_pg1.wait.return_value = True
    mock_pg2.wait.return_value = False
    assert not handle.wait(timeout_seconds=10)
    mock_pg1.wait.assert_called_once()
    mock_pg2.wait.assert_called_once()

    # Empty placement_groups returns False
    mock_spg.placement_groups = []
    assert not handle.wait(timeout_seconds=10)

    # Live Ray PGs with 0s remaining budget check GCS table state rather than pg.wait(0),
    # including when the shared deadline expires mid-loop after an earlier slice becomes ready.
    pg1 = placement_group([{"CPU": 1}])
    pg2 = placement_group([{"CPU": 1}])
    pg_pending = placement_group([{"CPU": 99}])
    try:
        ray.get([pg1.ready(), pg2.ready()], timeout=10)
        assert SlicePlacementGroupHandle(MagicMock(placement_groups=[pg1, pg2])).wait(
            timeout_seconds=0
        )
        assert not SlicePlacementGroupHandle(
            MagicMock(placement_groups=[pg1, pg_pending])
        ).wait(timeout_seconds=0)

        ready_clock = iter([0.0, 0.5, 1.5])
        with patch(
            "ray.train.v2._internal.execution.worker_group.placement_group_handle.time.monotonic",
            side_effect=lambda: next(ready_clock, 1.5),
        ):
            assert SlicePlacementGroupHandle(
                MagicMock(placement_groups=[pg1, pg2])
            ).wait(timeout_seconds=1.0)
        pending_clock = iter([0.0, 0.5, 1.5])
        with patch(
            "ray.train.v2._internal.execution.worker_group.placement_group_handle.time.monotonic",
            side_effect=lambda: next(pending_clock, 1.5),
        ):
            assert not SlicePlacementGroupHandle(
                MagicMock(placement_groups=[pg1, pg_pending])
            ).wait(timeout_seconds=1.0)
    finally:
        for pg in (pg1, pg2, pg_pending):
            ray.util.remove_placement_group(pg)


def test_slice_handle_shutdown():
    """shutdown() should call shutdown on the SlicePlacementGroup."""
    mock_spg = MagicMock()

    handle = SlicePlacementGroupHandle(mock_spg)
    handle.shutdown()

    mock_spg.shutdown.assert_called_once()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
