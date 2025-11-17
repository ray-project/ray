from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.controller.pg_cleaner_callback import (
    PlacementGroupCleanerCallback,
)
from ray.train.v2._internal.execution.failure_handling import FailureConfig, FailurePolicy
from ray.train.v2._internal.execution.scaling_policy import ScalingPolicy
from ray.train.v2.api.config import ScalingConfig
from ray.train.v2.tests.util import (
    DummyObjectRefWrapper,
    DummyWorkerGroup,
    MockScalingPolicy,
    create_dummy_run_context,
)

pytestmark = pytest.mark.usefixtures("mock_runtime_context")


@pytest.fixture(autouse=True)
def ray_start():
    ray.init()
    yield
    ray.shutdown()


@pytest.fixture(autouse=True)
def patch_worker_group(monkeypatch):
    monkeypatch.setattr(TrainController, "worker_group_cls", DummyWorkerGroup)


@pytest.mark.asyncio
async def test_pg_cleaner_callback_lifecycle():
    """Test that the PG cleaner callback is properly integrated into controller lifecycle."""
    
    # Create a mock PlacementGroupCleaner actor
    mock_cleaner = MagicMock()
    mock_cleaner.register_controller.remote.return_value = ray.put(None)
    mock_cleaner.register_placement_group.remote.return_value = ray.put(None)
    mock_cleaner.start_monitoring.remote.return_value = ray.put(True)
    mock_cleaner.stop.remote.return_value = ray.put(None)
    
    # Create the callback
    callback = PlacementGroupCleanerCallback(check_interval_s=0.5)
    
    # Patch the PlacementGroupCleaner.options().remote() call to return our mock
    with patch(
        "ray.train.v2._internal.execution.controller.pg_cleaner_callback."
        "PlacementGroupCleaner.options"
    ) as mock_options:
        mock_options.return_value.remote.return_value = mock_cleaner
        
        # Mock the runtime context call
        mock_runtime_context = MagicMock()
        mock_runtime_context.get_actor_id.return_value = "controller-actor-id"
        with patch("ray.get_runtime_context", return_value=mock_runtime_context):
            
            # Create train context
            train_run_context = create_dummy_run_context()
            
            # Test after_controller_start
            callback.after_controller_start(train_run_context)
            
            # Verify cleaner was launched with correct options
            mock_options.assert_called_once()
            call_kwargs = mock_options.call_args.kwargs
            assert call_kwargs["name"] == f"pg_cleaner_{train_run_context.run_id}"
            assert call_kwargs["namespace"] == "train"
            assert call_kwargs["lifetime"] == "detached"
            assert call_kwargs["get_if_exists"] is False
            
            # Verify controller was registered
            mock_cleaner.register_controller.remote.assert_called_once_with(
                "controller-actor-id"
            )
            
            # Create a mock worker group with placement group
            mock_worker_group = MagicMock()
            mock_pg = MagicMock()
            mock_pg.id = "test-pg-123"
            mock_worker_group_state = MagicMock()
            mock_worker_group_state.placement_group = mock_pg
            mock_worker_group.get_worker_group_state.return_value = mock_worker_group_state
            
            # Test after_worker_group_start
            callback.after_worker_group_start(mock_worker_group)
            
            # Verify PG was registered
            mock_cleaner.register_placement_group.remote.assert_called_once_with(mock_pg)
            
            # Verify monitoring was started
            mock_cleaner.start_monitoring.remote.assert_called_once()
            
            # Test before_controller_shutdown
            callback.before_controller_shutdown()
            
            # Verify cleaner was stopped
            mock_cleaner.stop.remote.assert_called_once()


@pytest.mark.asyncio
async def test_pg_cleaner_callback_handles_errors_gracefully():
    """Test that callback handles errors gracefully and doesn't crash the controller."""
    
    callback = PlacementGroupCleanerCallback(check_interval_s=0.5)
    
    # Test that failure to launch cleaner doesn't crash
    with patch(
        "ray.train.v2._internal.execution.controller.pg_cleaner_callback."
        "PlacementGroupCleaner.options"
    ) as mock_options:
        mock_options.side_effect = Exception("Launch failed")
        
        train_run_context = create_dummy_run_context()
        
        # Should not raise an exception
        callback.after_controller_start(train_run_context)
        
        # Cleaner should be None
        assert callback._cleaner is None
    
    # Test that failure to register PG doesn't crash
    callback._cleaner = MagicMock()
    callback._cleaner.register_placement_group.remote.side_effect = Exception("Register failed")
    
    mock_worker_group = MagicMock()
    mock_pg = MagicMock()
    mock_worker_group_state = MagicMock()
    mock_worker_group_state.placement_group = mock_pg
    mock_worker_group.get_worker_group_state.return_value = mock_worker_group_state
    
    # Should not raise an exception
    callback.after_worker_group_start(mock_worker_group)
    
    # Test that failure to stop cleaner doesn't crash
    callback._cleaner = MagicMock()
    callback._cleaner.stop.remote.side_effect = Exception("Stop failed")
    
    # Should not raise an exception
    callback.before_controller_shutdown()


@pytest.mark.asyncio
async def test_pg_cleaner_callback_no_op_when_cleaner_not_available():
    """Test that callback is a no-op when cleaner is not available."""
    
    callback = PlacementGroupCleanerCallback()
    callback._cleaner = None
    
    # Test after_worker_group_start does nothing
    mock_worker_group = MagicMock()
    callback.after_worker_group_start(mock_worker_group)
    mock_worker_group.get_worker_group_state.assert_not_called()
    
    # Test before_controller_shutdown does nothing
    callback.before_controller_shutdown()  # Should not raise

