from unittest.mock import MagicMock, patch

import pytest

from ray.train.v2._internal.execution.context import (
    LocalTrainContext,
    get_train_context,
    set_train_context,
)
from ray.train.v2._internal.execution.storage import StorageContext


class TestLocalTrainContext:
    """Test LocalTrainContext functionality without Ray running."""

    def setup_method(self):
        """Reset the global train context before each test."""
        global _train_context
        _train_context = None

    def teardown_method(self):
        """Clean up after each test."""
        global _train_context
        _train_context = None

    def test_local_train_context_creation(self):
        """Test that LocalTrainContext can be created without Ray."""
        # Create a mock storage context
        storage_context = MagicMock(spec=StorageContext)

        # Create LocalTrainContext without Ray running
        context = LocalTrainContext(
            local_world_size=2,
            local_rank=0,
            experiment_name="test_experiment",
            dataset_shards={},
            storage_context=storage_context,
        )

        # Verify basic attributes
        assert context.get_experiment_name() == "test_experiment"
        assert context.get_world_size() == 2
        assert context.get_world_rank() == 0
        assert context.get_local_rank() == 0
        assert context.get_local_world_size() == 2
        assert context.get_node_rank() == 0
        assert context.get_storage() == storage_context

    def test_set_train_context_with_local_context(self):
        """Test that set_train_context works with LocalTrainContext without Ray."""
        storage_context = MagicMock(spec=StorageContext)

        # Create LocalTrainContext
        local_context = LocalTrainContext(
            local_world_size=1,
            local_rank=0,
            experiment_name="local_test",
            dataset_shards={},
            storage_context=storage_context,
        )

        # Set the context without Ray running
        set_train_context(local_context)

        # Verify we can retrieve it
        retrieved_context = get_train_context()
        assert retrieved_context is local_context
        assert isinstance(retrieved_context, LocalTrainContext)

    def test_is_running_locally_returns_true(self):
        """Test that is_running_locally returns True for LocalTrainContext."""
        storage_context = MagicMock(spec=StorageContext)

        # Create and set LocalTrainContext
        local_context = LocalTrainContext(
            local_world_size=1,
            local_rank=0,
            experiment_name="local_test",
            dataset_shards={},
            storage_context=storage_context,
        )
        set_train_context(local_context)

        # Test is_running_locally returns True
        context = get_train_context()
        assert context.is_running_locally() is True

    def test_local_context_dataset_shards(self):
        """Test dataset shard access in LocalTrainContext."""
        storage_context = MagicMock(spec=StorageContext)
        mock_iterator = MagicMock()
        dataset_shards = {"train": mock_iterator, "val": mock_iterator}

        local_context = LocalTrainContext(
            local_world_size=1,
            local_rank=0,
            experiment_name="dataset_test",
            dataset_shards=dataset_shards,
            storage_context=storage_context,
        )

        # Test getting dataset shards
        assert local_context.get_dataset_shard("train") is mock_iterator
        assert local_context.get_dataset_shard("val") is mock_iterator

    def test_local_context_report_functionality(self):
        """Test the report functionality of LocalTrainContext."""
        storage_context = MagicMock(spec=StorageContext)

        local_context = LocalTrainContext(
            local_world_size=1,
            local_rank=0,
            experiment_name="report_test",
            dataset_shards={},
            storage_context=storage_context,
        )

        # Test that report doesn't raise an error
        # For LocalTrainContext, report just logs the information
        with patch("ray.train.v2._internal.execution.context.logger") as mock_logger:
            metrics = {"loss": 0.5, "accuracy": 0.95}
            local_context.report(metrics)

            # Verify that the logger was called
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0][0]
            assert "metrics: {'loss': 0.5, 'accuracy': 0.95}" in call_args
            assert "checkpoint: None" in call_args
            assert "checkpoint_dir_name: None" in call_args

    def test_context_isolation_without_ray(self):
        """Test that LocalTrainContext works in isolation without any Ray infrastructure."""
        # This test specifically ensures we don't import or require Ray to be running
        storage_context = MagicMock(spec=StorageContext)

        # Create LocalTrainContext
        local_context = LocalTrainContext(
            local_world_size=4,
            local_rank=2,
            experiment_name="isolation_test",
            dataset_shards={"data": MagicMock()},
            storage_context=storage_context,
        )

        # Set and use context without Ray
        set_train_context(local_context)
        context = get_train_context()

        # Verify all functionality works
        assert context.is_running_locally() is True
        assert context.get_experiment_name() == "isolation_test"
        assert context.get_world_size() == 4
        assert context.get_world_rank() == 2
        assert context.get_local_rank() == 2
        assert context.get_local_world_size() == 4
        assert context.get_node_rank() == 0
        assert context.get_storage() is storage_context

        # Test report works
        context.report({"step": 1})

        # Test dataset access works
        assert context.get_dataset_shard("data") is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
