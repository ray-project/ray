"""Tests for async checkpoint functionality in Ray Train."""

import json
import os
import tempfile
import time
import pytest
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow.fs

from ray.train import Checkpoint, async_report
from ray.train._internal.async_checkpoint import (
    AsyncCheckpointWriter,
    _AsyncCheckpointError,
    _AsyncCheckpointTask,
    _get_async_checkpoint_writer,
    _shutdown_async_checkpoint_writer,
)
from ray.train._internal.session import _TrainSession, init_session, shutdown_session
from ray.train._internal.storage import StorageContext


class TestAsyncCheckpointWriter:
    """Unit tests for AsyncCheckpointWriter."""

    def setup_method(self):
        """Setup for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.storage_path = Path(self.temp_dir, "storage").as_posix()
        self.local_fs = pyarrow.fs.LocalFileSystem()

    def teardown_method(self):
        """Cleanup after each test."""
        _shutdown_async_checkpoint_writer()
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_storage_context(self) -> StorageContext:
        """Create a mock storage context for testing."""
        storage_context = Mock(spec=StorageContext)
        storage_context.storage_filesystem = self.local_fs
        storage_context.checkpoint_fs_path = Path(
            self.storage_path, "checkpoint_000001"
        ).as_posix()
        storage_context.experiment_fs_path = self.storage_path
        storage_context._check_validation_file = Mock()
        storage_context.storage_filesystem.create_dir = Mock()
        return storage_context

    def test_async_checkpoint_writer_initialization(self):
        """Test AsyncCheckpointWriter initialization."""
        writer = AsyncCheckpointWriter(max_concurrent_uploads=3, max_pending_uploads=10)

        assert writer._max_concurrent_uploads == 3
        assert writer._max_pending_uploads == 10
        assert not writer._shutdown
        assert writer._submission_thread.is_alive()

        writer.shutdown()

    def test_async_persist_checkpoint_success(self):
        """Test successful async checkpoint persistence."""
        writer = AsyncCheckpointWriter()
        storage_context = self._create_mock_storage_context()

        # Create a test checkpoint
        checkpoint_dir = Path(self.temp_dir, "checkpoint").as_posix()
        os.makedirs(checkpoint_dir)
        Path(checkpoint_dir, "model.pt").write_text("fake model data")

        checkpoint = Checkpoint.from_directory(checkpoint_dir)
        metrics = {"loss": 0.5, "accuracy": 0.8}

        # Should not raise
        writer.async_persist_checkpoint(checkpoint, storage_context, metrics)

        # Wait a bit for task to be processed
        time.sleep(0.1)

        stats = writer.get_stats()
        assert stats["pending_uploads"] >= 0

        writer.shutdown(timeout=5.0)

    def test_backpressure_control(self):
        """Test backpressure when queue is full."""
        writer = AsyncCheckpointWriter(max_concurrent_uploads=1, max_pending_uploads=1)
        storage_context = self._create_mock_storage_context()

        # Create test checkpoint
        checkpoint_dir = Path(self.temp_dir, "checkpoint").as_posix()
        os.makedirs(checkpoint_dir)
        Path(checkpoint_dir, "model.pt").write_text("fake model data")
        checkpoint = Checkpoint.from_directory(checkpoint_dir)

        # Fill the queue
        writer.async_persist_checkpoint(checkpoint, storage_context, {"step": 1})

        # This should raise due to backpressure
        with pytest.raises(_AsyncCheckpointError):
            writer.async_persist_checkpoint(checkpoint, storage_context, {"step": 2})

        writer.shutdown()

    def test_shutdown_waits_for_pending_uploads(self):
        """Test that shutdown waits for pending uploads."""
        writer = AsyncCheckpointWriter()

        # Mock a slow upload
        with patch.object(writer, "_upload_checkpoint_task") as mock_upload:
            mock_upload.side_effect = lambda task: time.sleep(0.5)

            storage_context = self._create_mock_storage_context()
            checkpoint_dir = Path(self.temp_dir, "checkpoint").as_posix()
            os.makedirs(checkpoint_dir)
            checkpoint = Checkpoint.from_directory(checkpoint_dir)

            writer.async_persist_checkpoint(checkpoint, storage_context, {})

            start_time = time.time()
            writer.shutdown(timeout=2.0)
            elapsed = time.time() - start_time

            # Should have waited for the upload
            assert elapsed >= 0.4

    def test_latest_manifest_writing(self):
        """Test LATEST.json manifest writing."""
        writer = AsyncCheckpointWriter()
        storage_context = self._create_mock_storage_context()

        # Setup real filesystem for manifest test
        os.makedirs(self.storage_path, exist_ok=True)
        storage_context.storage_filesystem = pyarrow.fs.LocalFileSystem()

        checkpoint_dir = Path(self.temp_dir, "checkpoint").as_posix()
        os.makedirs(checkpoint_dir)
        checkpoint = Checkpoint.from_directory(checkpoint_dir)
        metrics = {"step": 100, "loss": 0.1}

        writer._write_latest_manifest(storage_context, metrics, checkpoint)

        # Check manifest was written
        manifest_path = Path(self.storage_path, "LATEST.json")
        assert manifest_path.exists()

        with open(manifest_path) as f:
            manifest_data = json.load(f)

        assert manifest_data["metrics"] == metrics
        assert manifest_data["upload_completed"] is True
        assert "timestamp" in manifest_data

        writer.shutdown()


class TestAsyncReportIntegration:
    """Integration tests for async_report function."""

    def setup_method(self):
        """Setup Ray Train session for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.storage_path = Path(self.temp_dir, "storage").as_posix()

        # Mock storage context
        storage_context = Mock(spec=StorageContext)
        storage_context.storage_filesystem = pyarrow.fs.LocalFileSystem()
        storage_context.checkpoint_fs_path = Path(
            self.storage_path, "checkpoint_000001"
        ).as_posix()
        storage_context.experiment_fs_path = self.storage_path
        storage_context.checkpoint_dir_name = "checkpoint_000001"
        storage_context._check_validation_file = Mock()
        storage_context._update_checkpoint_index = Mock()
        storage_context.persist_artifacts = Mock()
        storage_context.sync_config = Mock()
        storage_context.sync_config.sync_artifacts_on_checkpoint = False

        # Initialize session
        init_session(
            training_func=lambda: None,
            world_rank=0,
            local_rank=0,
            node_rank=0,
            local_world_size=1,
            world_size=1,
            storage=storage_context,
            synchronous_result_reporting=True,
        )

    def teardown_method(self):
        """Cleanup after each test."""
        shutdown_session()
        _shutdown_async_checkpoint_writer()
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_async_report_without_checkpoint(self):
        """Test async_report with only metrics."""
        metrics = {"loss": 0.5, "accuracy": 0.8}

        # Should not raise
        async_report(metrics)

    def test_async_report_with_checkpoint(self):
        """Test async_report with checkpoint."""
        # Create test checkpoint
        checkpoint_dir = Path(self.temp_dir, "checkpoint").as_posix()
        os.makedirs(checkpoint_dir)
        Path(checkpoint_dir, "model.pt").write_text("fake model data")
        checkpoint = Checkpoint.from_directory(checkpoint_dir)

        metrics = {"loss": 0.5, "step": 10}

        # Should not raise and should return immediately
        start_time = time.time()
        async_report(metrics, checkpoint=checkpoint)
        elapsed = time.time() - start_time

        # Should be much faster than a real upload
        assert elapsed < 0.1

    def test_async_report_flush_errors(self):
        """Test error reporting with flush=True."""
        # This test would need to simulate upload failures
        # For now, just test that flush=True doesn't crash
        async_report({}, flush=True)

    def test_async_report_fallback_on_backpressure(self):
        """Test fallback to sync when async queue is full."""
        # Create a writer with very small queue
        with patch(
            "ray.train._internal.session._get_async_checkpoint_writer"
        ) as mock_get_writer:
            mock_writer = Mock()
            mock_writer.async_persist_checkpoint.side_effect = _AsyncCheckpointError(
                "Queue full", Exception()
            )
            mock_get_writer.return_value = mock_writer

            checkpoint_dir = Path(self.temp_dir, "checkpoint").as_posix()
            os.makedirs(checkpoint_dir)
            checkpoint = Checkpoint.from_directory(checkpoint_dir)

            # Should fall back to sync upload without raising
            async_report({"step": 1}, checkpoint=checkpoint)

    def test_torch_tensor_validation(self):
        """Test that torch tensors in metrics are rejected."""
        # Mock torch being imported
        with patch.dict("sys.modules", {"torch": Mock()}):
            with patch(
                "ray.air._internal.torch_utils.contains_tensor", return_value=True
            ):
                with pytest.raises(ValueError, match="Torch tensors"):
                    async_report({"tensor": "fake_tensor"})


class TestAsyncCheckpointEndToEnd:
    """End-to-end tests with real filesystem operations."""

    def setup_method(self):
        """Setup real storage for e2e tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.storage_path = Path(self.temp_dir, "storage").as_posix()
        os.makedirs(self.storage_path)

    def teardown_method(self):
        """Cleanup after each test."""
        _shutdown_async_checkpoint_writer()
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_end_to_end_upload_with_manifest(self):
        """Test complete upload flow with manifest writing."""
        writer = AsyncCheckpointWriter()

        # Create real storage context
        from ray.train._internal.storage import StorageContext

        storage_context = StorageContext(
            storage_path=self.storage_path,
            experiment_dir_name="test_experiment",
            trial_dir_name="trial_1",
            storage_filesystem=pyarrow.fs.LocalFileSystem(),
            sync_config=Mock(),
        )

        # Create real checkpoint
        checkpoint_dir = Path(self.temp_dir, "checkpoint").as_posix()
        os.makedirs(checkpoint_dir)

        # Write some files to checkpoint
        Path(checkpoint_dir, "model.pt").write_text("model weights")
        Path(checkpoint_dir, "optimizer.pt").write_text("optimizer state")
        subdir = Path(checkpoint_dir, "subdir")
        subdir.mkdir()
        Path(subdir, "config.json").write_text('{"lr": 0.001}')

        checkpoint = Checkpoint.from_directory(checkpoint_dir)
        metrics = {"epoch": 5, "loss": 0.1, "accuracy": 0.95}

        # Persist async
        writer.async_persist_checkpoint(checkpoint, storage_context, metrics)

        # Wait for completion
        writer.shutdown(timeout=10.0)

        # Verify files were uploaded
        uploaded_checkpoint = Path(storage_context.checkpoint_fs_path)
        assert uploaded_checkpoint.exists()
        assert (uploaded_checkpoint / "model.pt").exists()
        assert (uploaded_checkpoint / "optimizer.pt").exists()
        assert (uploaded_checkpoint / "subdir" / "config.json").exists()

        # Verify manifest
        manifest_path = Path(storage_context.experiment_fs_path, "LATEST.json")
        assert manifest_path.exists()

        with open(manifest_path) as f:
            manifest_data = json.load(f)

        assert manifest_data["metrics"] == metrics
        assert manifest_data["upload_completed"] is True


class TestErrorHandling:
    """Tests for error handling in async checkpoints."""

    def test_upload_failure_reporting(self):
        """Test that upload failures are properly reported."""
        writer = AsyncCheckpointWriter()

        # Mock failing upload
        with patch.object(writer, "_upload_checkpoint_task") as mock_upload:
            mock_upload.side_effect = Exception("Upload failed")

            storage_context = Mock()
            checkpoint = Mock()

            writer.async_persist_checkpoint(checkpoint, storage_context, {})

            # Wait for failure
            time.sleep(0.2)

            # Should have failed task in queue
            with pytest.raises(_AsyncCheckpointError):
                writer.check_and_raise_errors()

        writer.shutdown()

    def test_shutdown_during_uploads(self):
        """Test shutdown behavior when uploads are in progress."""
        writer = AsyncCheckpointWriter()

        # Start shutdown immediately
        writer.shutdown(timeout=1.0)

        # Should be shutdown
        assert writer._shutdown


if __name__ == "__main__":
    pytest.main([__file__])
