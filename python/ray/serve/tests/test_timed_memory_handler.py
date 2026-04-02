"""Tests for TimedMemoryHandler idle timeout flush functionality."""

import io
import logging
import os
import sys
import tempfile
import threading
import time

import pytest

# Import TimedMemoryHandler directly to avoid Ray initialization issues
from ray.serve._private.logging_utils import TimedMemoryHandler


class TestTimedMemoryHandler:
    """Test suite for TimedMemoryHandler."""

    def test_basic_functionality(self):
        """Test basic MemoryHandler functionality is preserved."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            temp_file = f.name

        try:
            file_handler = logging.FileHandler(temp_file)
            handler = TimedMemoryHandler(
                capacity=10,
                target=file_handler,
                flushLevel=logging.ERROR,
                flush_timeout_s=None,  # Disable timeout
            )

            logger = logging.getLogger("test")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            logger.info("Test message 1")
            logger.info("Test message 2")

            # Without flush, messages should be buffered
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Test message 1" not in content
                assert "Test message 2" not in content

            # After flush, messages should be written
            handler.flush()
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Test message 1" in content
                assert "Test message 2" in content

            handler.close()
            file_handler.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_idle_timeout_flush(self):
        """Test that idle timeout triggers automatic flush."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            temp_file = f.name

        try:
            file_handler = logging.FileHandler(temp_file)
            handler = TimedMemoryHandler(
                capacity=10,
                target=file_handler,
                flushLevel=logging.ERROR,
                flush_timeout_s=0.5,  # 0.5 second timeout
            )

            logger = logging.getLogger("test_timeout")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            # Emit a log record
            logger.info("Test message before timeout")

            # Verify message is buffered initially
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Test message before timeout" not in content

            # Wait for timeout to trigger flush
            time.sleep(0.7)

            # Verify message was flushed after timeout
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Test message before timeout" in content

            handler.close()
            file_handler.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_timeout_reset_on_emit(self):
        """Test that timeout resets when a new record is emitted."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            temp_file = f.name

        try:
            file_handler = logging.FileHandler(temp_file)
            handler = TimedMemoryHandler(
                capacity=10,
                target=file_handler,
                flushLevel=logging.ERROR,
                flush_timeout_s=0.5,  # 0.5 second timeout
            )

            logger = logging.getLogger("test_reset")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            # Emit first message
            logger.info("Message 1")
            time.sleep(0.3)  # Wait less than timeout

            # Emit second message (should reset timeout)
            logger.info("Message 2")
            time.sleep(0.3)  # Wait less than timeout again

            # Emit third message (should reset timeout again)
            logger.info("Message 3")

            # Verify messages are still buffered
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Message 1" not in content
                assert "Message 2" not in content
                assert "Message 3" not in content

            # Wait for timeout after last emit
            time.sleep(0.6)

            # All messages should be flushed after final timeout
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Message 1" in content
                assert "Message 2" in content
                assert "Message 3" in content

            handler.close()
            file_handler.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_manual_flush(self):
        """Test that manual flush works correctly."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            temp_file = f.name

        try:
            file_handler = logging.FileHandler(temp_file)
            handler = TimedMemoryHandler(
                capacity=10,
                target=file_handler,
                flushLevel=logging.ERROR,
                flush_timeout_s=1.0,  # Long timeout
            )

            logger = logging.getLogger("test_manual")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            # Emit and manually flush
            logger.info("Message before manual flush")
            handler.flush()

            # Verify message was flushed
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Message before manual flush" in content

            # Emit another message
            logger.info("Message after manual flush")

            # Wait for timeout
            time.sleep(1.2)

            # Second message should be flushed
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Message after manual flush" in content

            handler.close()
            file_handler.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_error_level_immediate_flush(self):
        """Test that ERROR level messages trigger immediate flush."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            temp_file = f.name

        try:
            file_handler = logging.FileHandler(temp_file)
            handler = TimedMemoryHandler(
                capacity=10,
                target=file_handler,
                flushLevel=logging.ERROR,
                flush_timeout_s=1.0,  # Long timeout
            )

            logger = logging.getLogger("test_error")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            # Emit INFO message (should be buffered)
            logger.info("Info message")
            time.sleep(0.1)

            # Verify INFO message is buffered
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Info message" not in content

            # Emit ERROR message (should trigger immediate flush)
            logger.error("Error message")

            # Both messages should be flushed immediately
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Info message" in content
                assert "Error message" in content

            handler.close()
            file_handler.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_close_flushes_remaining_logs(self):
        """Test that close() flushes remaining buffered logs."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            temp_file = f.name

        try:
            file_handler = logging.FileHandler(temp_file)
            handler = TimedMemoryHandler(
                capacity=10,
                target=file_handler,
                flushLevel=logging.ERROR,
                flush_timeout_s=1.0,  # Long timeout
            )

            logger = logging.getLogger("test_close")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            # Emit messages
            logger.info("Message 1")
            logger.info("Message 2")

            # Verify messages are buffered
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Message 1" not in content
                assert "Message 2" not in content

            # Close handler (should flush)
            handler.close()

            # Verify messages were flushed
            with open(temp_file, "r") as f:
                content = f.read()
                assert "Message 1" in content
                assert "Message 2" in content

            file_handler.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_close_stops_background_thread(self):
        """Test that close() properly stops the background thread."""
        file_handler = logging.StreamHandler(io.StringIO())
        handler = TimedMemoryHandler(
            capacity=10,
            target=file_handler,
            flushLevel=logging.ERROR,
            flush_timeout_s=0.1,
        )

        # Verify thread is running
        assert handler._check_thread is not None
        assert handler._check_thread.is_alive()

        # Close handler
        handler.close()

        # Wait a bit for thread to stop
        time.sleep(0.2)

        # Verify thread is stopped
        assert handler._shutdown is True
        assert not handler._check_thread.is_alive()

        file_handler.close()

    def test_no_timeout_when_disabled(self):
        """Test that no background thread is created when timeout is disabled."""
        file_handler = logging.StreamHandler(io.StringIO())
        handler = TimedMemoryHandler(
            capacity=10,
            target=file_handler,
            flushLevel=logging.ERROR,
            flush_timeout_s=None,  # Disabled
        )

        # Verify no thread is created
        assert handler._check_thread is None

        handler.close()
        file_handler.close()

    def test_zero_timeout_disabled(self):
        """Test that zero timeout disables the feature."""
        file_handler = logging.StreamHandler(io.StringIO())
        handler = TimedMemoryHandler(
            capacity=10,
            target=file_handler,
            flushLevel=logging.ERROR,
            flush_timeout_s=0,  # Zero timeout
        )

        # Verify no thread is created
        assert handler._check_thread is None

        handler.close()
        file_handler.close()

    def test_concurrent_emits(self):
        """Test that concurrent emits work correctly."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            temp_file = f.name

        try:
            file_handler = logging.FileHandler(temp_file)
            handler = TimedMemoryHandler(
                capacity=100,
                target=file_handler,
                flushLevel=logging.ERROR,
                flush_timeout_s=0.5,
            )

            logger = logging.getLogger("test_concurrent")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            # Emit messages from multiple threads
            def emit_logs(thread_id):
                for i in range(10):
                    logger.info(f"Thread {thread_id} message {i}")
                    time.sleep(0.01)

            threads = []
            for i in range(5):
                t = threading.Thread(target=emit_logs, args=(i,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # Wait for timeout
            time.sleep(0.6)

            # Verify all messages were flushed
            with open(temp_file, "r") as f:
                content = f.read()
                for i in range(5):
                    for j in range(10):
                        assert f"Thread {i} message {j}" in content

            handler.close()
            file_handler.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_buffer_capacity_limit(self):
        """Test that buffer capacity limit still works."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            temp_file = f.name

        try:
            file_handler = logging.FileHandler(temp_file)
            handler = TimedMemoryHandler(
                capacity=3,  # Small capacity
                target=file_handler,
                flushLevel=logging.ERROR,
                flush_timeout_s=1.0,  # Long timeout
            )

            logger = logging.getLogger("test_capacity")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            # Emit more messages than capacity
            for i in range(5):
                logger.info(f"Message {i}")

            # Buffer should flush when capacity is exceeded
            # Wait a bit for flush to complete
            time.sleep(0.1)

            # Verify messages were flushed
            with open(temp_file, "r") as f:
                content = f.read()
                # At least some messages should be flushed
                assert "Message" in content

            handler.close()
            file_handler.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
