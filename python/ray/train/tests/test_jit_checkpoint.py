"""Tests for Just-In-Time (JIT) Checkpointing in Ray Train."""

import os
import signal
import time
from unittest.mock import MagicMock, patch

import pytest

from ray.train import Checkpoint
from ray.train._internal.jit_checkpoint import JITCheckpointHandler
from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig
from ray.train._internal.session import _TrainSession
from ray.train._internal.storage import StorageContext


class TestJITCheckpointHandler:
    """Unit tests for JITCheckpointHandler class."""

    def test_signal_handler_registration(self):
        """Test that SIGTERM handler is registered when enabled."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Create handler
        handler = JITCheckpointHandler(mock_session, kill_wait=1.0)

        # Get the original SIGTERM handler
        original_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)

        try:
            # Register the handler
            handler.register_signal_handler()

            # Verify that SIGTERM handler was changed
            current_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            assert current_handler == handler._sigterm_handler

            # Cleanup
            handler.cleanup()

            # Verify that original handler was restored
            restored_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            assert restored_handler == original_handler

        finally:
            # Ensure we restore the original handler
            signal.signal(signal.SIGTERM, original_handler)

    def test_sigterm_triggers_checkpoint_request(self):
        """Test that SIGTERM sets checkpoint_requested flag."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Create handler with very short kill_wait for testing
        handler = JITCheckpointHandler(mock_session, kill_wait=0.1)
        handler.register_signal_handler()

        try:
            # Initially, checkpoint should not be requested
            assert not handler.checkpoint_requested

            # Send SIGTERM to self
            os.kill(os.getpid(), signal.SIGTERM)

            # Give signal handler time to execute
            time.sleep(0.05)

            # Verify that checkpoint was requested
            assert handler.checkpoint_requested

        finally:
            handler.cleanup()

    def test_duplicate_sigterm_ignored(self):
        """Test that duplicate SIGTERM signals are ignored."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Create handler
        handler = JITCheckpointHandler(mock_session, kill_wait=1.0)
        handler.register_signal_handler()

        try:
            # Send first SIGTERM
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.05)

            # Verify timer was started
            first_timer = handler._checkpoint_timer
            assert first_timer is not None

            # Send second SIGTERM
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.05)

            # Verify timer wasn't replaced (same timer object)
            assert handler._checkpoint_timer is first_timer

        finally:
            handler.cleanup()

    def test_kill_wait_delay(self):
        """Test that checkpoint is delayed by kill_wait period."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Create handler with 0.2 second kill_wait
        handler = JITCheckpointHandler(mock_session, kill_wait=0.2)
        handler.register_signal_handler()

        try:
            # Send SIGTERM
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.05)

            # Checkpoint should be requested but thread not started yet
            assert handler.checkpoint_requested
            assert handler._checkpoint_thread is None

            # Wait for kill_wait to expire
            time.sleep(0.25)

            # Now thread should be started
            assert handler._checkpoint_thread is not None

        finally:
            handler.cleanup()

    def test_cleanup_cancels_pending_timer(self):
        """Test that cleanup cancels a pending timer."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Create handler with long kill_wait
        handler = JITCheckpointHandler(mock_session, kill_wait=10.0)
        handler.register_signal_handler()

        try:
            # Send SIGTERM (timer will be created but not yet fired)
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.05)

            # Verify timer exists
            assert handler._checkpoint_timer is not None
            assert handler._checkpoint_timer.is_alive()

            # Cleanup should cancel the timer
            handler.cleanup()

            # Timer should no longer be alive
            assert not handler._checkpoint_timer.is_alive()

        finally:
            # Extra cleanup in case test fails
            try:
                handler.cleanup()
            except:
                pass

    def test_checkpoint_thread_completion(self):
        """Test that checkpoint thread completes successfully."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Mock the persist_current_checkpoint method
        mock_checkpoint = MagicMock(spec=Checkpoint)
        mock_session.storage.persist_current_checkpoint.return_value = mock_checkpoint

        # Create handler with very short kill_wait for testing
        handler = JITCheckpointHandler(mock_session, kill_wait=0.1)
        handler.register_signal_handler()

        try:
            # Trigger SIGTERM
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.05)

            # Verify checkpoint was requested
            assert handler.checkpoint_requested

            # Wait for checkpoint thread to complete
            time.sleep(0.5)

            # Verify checkpoint completed
            assert handler.checkpoint_completed

            # Verify persist was called
            mock_session.storage.persist_current_checkpoint.assert_called_once()

        finally:
            handler.cleanup()

    def test_checkpoint_failure_handling(self):
        """Test that checkpoint failure is handled gracefully."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Mock storage to raise exception during persist
        mock_session.storage.persist_current_checkpoint.side_effect = Exception(
            "Storage error"
        )

        # Create handler
        handler = JITCheckpointHandler(mock_session, kill_wait=0.1)
        handler.register_signal_handler()

        try:
            # Trigger SIGTERM
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.05)

            # Verify checkpoint was requested
            assert handler.checkpoint_requested

            # Wait for checkpoint thread to complete
            time.sleep(0.5)

            # Verify handler doesn't crash (checkpoint_completed should still be False)
            assert not handler.checkpoint_completed

            # Verify persist was called (and failed)
            mock_session.storage.persist_current_checkpoint.assert_called_once()

        finally:
            handler.cleanup()

    def test_multiple_rapid_sigterm_signals(self):
        """Test that multiple rapid SIGTERM signals are handled correctly."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Create handler
        handler = JITCheckpointHandler(mock_session, kill_wait=1.0)
        handler.register_signal_handler()

        try:
            # Send multiple SIGTERM signals rapidly
            for _ in range(3):
                os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(0.01)  # Very short delay

            time.sleep(0.1)

            # Verify checkpoint was requested only once
            assert handler.checkpoint_requested

            # Verify only one timer was created
            assert handler._checkpoint_timer is not None

            # Store the first timer
            first_timer = handler._checkpoint_timer

            # Send more SIGTERM signals
            for _ in range(2):
                os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(0.01)

            time.sleep(0.1)

            # Verify timer wasn't replaced (same timer object)
            assert handler._checkpoint_timer is first_timer

        finally:
            handler.cleanup()

    def test_is_checkpoint_in_progress(self):
        """Test the is_checkpoint_in_progress method."""
        # Create a mock train session
        mock_session = MagicMock(spec=_TrainSession)
        mock_session.world_rank = 0
        mock_session.storage = MagicMock(spec=StorageContext)

        # Create handler
        handler = JITCheckpointHandler(mock_session, kill_wait=1.0)

        # Initially no checkpoint in progress
        assert not handler.is_checkpoint_in_progress()

        # Set checkpoint requested but not completed
        handler.checkpoint_requested = True
        handler.checkpoint_completed = False
        assert handler.is_checkpoint_in_progress()

        # Set checkpoint completed
        handler.checkpoint_completed = True
        assert not handler.is_checkpoint_in_progress()

        # Reset checkpoint requested
        handler.checkpoint_requested = False
        handler.checkpoint_completed = False
        assert not handler.is_checkpoint_in_progress()


class TestJITCheckpointConfig:
    """Unit tests for JITCheckpointConfig class."""

    def test_default_config(self):
        """Test that default configuration is correct."""
        config = JITCheckpointConfig()
        assert config.enabled is False
        assert config.kill_wait == 3.0

    def test_custom_config(self):
        """Test that custom configuration works."""
        config = JITCheckpointConfig(enabled=True, kill_wait=5.0)
        assert config.enabled is True
        assert config.kill_wait == 5.0

    def test_negative_kill_wait_validation(self):
        """Test that negative kill_wait raises ValueError."""
        with pytest.raises(ValueError, match="kill_wait must be non-negative"):
            JITCheckpointConfig(kill_wait=-1.0)

    def test_very_long_kill_wait_warning(self):
        """Test that very long kill_wait generates a warning."""
        with patch("logging.getLogger") as mock_logger:
            logger_instance = MagicMock()
            mock_logger.return_value = logger_instance

            # Create config with very long kill_wait
            config = JITCheckpointConfig(kill_wait=100.0)

            # Should have logged a warning
            logger_instance.warning.assert_called_once()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
