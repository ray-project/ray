"""Just-In-Time (JIT) Checkpoint Handler for Ray Train.

This module provides signal-based checkpoint triggering for Ray Train workers,
mirroring the implementation from HuggingFace Transformers.

The JIT checkpoint handler:
- Listens for SIGTERM signals (graceful termination)
- Waits a configurable period before checkpointing (kill_wait)
- Executes checkpoint asynchronously in a background thread
- Integrates with Ray Train's existing checkpoint infrastructure
"""

import logging
import signal
import threading
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray.train._internal.session import _TrainSession

logger = logging.getLogger(__name__)


class JITCheckpointHandler:
    """Handles SIGTERM signals and triggers asynchronous checkpoint saving.

    Mirrors the Transformers JITCheckpointCallback implementation but adapted
    for Ray's actor-based distributed training architecture.

    This handler:
    1. Registers a SIGTERM signal handler
    2. Waits `kill_wait` seconds after SIGTERM before checkpointing
       (avoids wasting time if SIGKILL follows immediately)
    3. Executes checkpoint in a background thread (non-blocking)
    4. Uses Ray Train's existing checkpoint infrastructure

    Example:
        >>> handler = JITCheckpointHandler(train_session, kill_wait=3.0)
        >>> handler.register_signal_handler()
        >>> # Training continues...
        >>> # On SIGTERM, checkpoint is triggered automatically
        >>> handler.cleanup()  # Restore original handler
    """

    def __init__(self, train_session: "_TrainSession", kill_wait: float = 3.0):
        """Initialize the JIT checkpoint handler.

        Args:
            train_session: The _TrainSession instance for checkpoint reporting.
                This provides access to the storage context and checkpoint
                infrastructure.
            kill_wait: Seconds to wait after SIGTERM before checkpointing.
                This optimization avoids wasting time if SIGKILL follows
                immediately after SIGTERM. Defaults to 3.0 seconds.
        """
        self.train_session = train_session
        self.kill_wait = kill_wait

        # State tracking
        self.checkpoint_requested = False
        self.checkpoint_completed = False

        # Threading components
        self._checkpoint_timer: Optional[threading.Timer] = None
        self._checkpoint_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # Store original signal handler for cleanup
        self._original_sigterm_handler = None

        logger.info(f"JITCheckpointHandler initialized with kill_wait={kill_wait}s")

    def _sigterm_handler(self, signum: int, frame) -> None:
        """Handle SIGTERM signal by initiating delayed checkpoint.

        This method is called when SIGTERM is received. It:
        1. Ignores duplicate signals
        2. Starts a timer that will trigger checkpoint after `kill_wait`
        3. Logs the signal reception

        Args:
            signum: The signal number (should be signal.SIGTERM)
            frame: The current stack frame (unused)
        """
        with self._lock:
            if self.checkpoint_requested:
                logger.debug(
                    "SIGTERM received again, ignoring (checkpoint already requested)"
                )
                return

            logger.info(
                f"SIGTERM received, will initiate JIT checkpoint in {self.kill_wait}s"
            )
            self.checkpoint_requested = True

            # Start timer to delay checkpoint
            # This avoids wasting time if SIGKILL follows immediately
            self._checkpoint_timer = threading.Timer(
                self.kill_wait, self._start_checkpoint_thread
            )
            self._checkpoint_timer.daemon = True
            self._checkpoint_timer.start()

    def _start_checkpoint_thread(self) -> None:
        """Start the async checkpoint thread after kill_wait expires.

        This method is called by the timer after `kill_wait` seconds.
        It launches a background thread to execute the checkpoint without
        blocking the main training thread.
        """
        logger.info("Kill wait period expired, starting async checkpoint thread")

        self._checkpoint_thread = threading.Thread(
            target=self._execute_jit_checkpoint, name="JITCheckpointThread", daemon=False  # Non-daemon to prevent early termination
        )
        self._checkpoint_thread.start()

    def _execute_jit_checkpoint(self) -> None:
        """Execute the actual checkpoint save operation.

        This method runs in a background thread and:
        1. Calls registered checkpoint function if available (from auto-registration)
        2. Falls back to marker-only checkpoint if no function registered
        3. Persists via Ray Train's storage infrastructure
        4. Handles errors gracefully
        5. Marks checkpoint as completed
        """
        try:
            logger.info("Executing JIT checkpoint...")
            start_time = time.time()

            # Try to get user-registered checkpoint function (from auto-registration)
            checkpoint_fn = self.train_session.get_state("jit_checkpoint_fn")


            if checkpoint_fn is not None:
                logger.info("Using auto-registered checkpoint function")
                try:
                    # Call the checkpoint function (saves model/optimizer)
                    checkpoint = checkpoint_fn()

                    if checkpoint is None:
                        logger.warning("Checkpoint function returned None, creating marker-only checkpoint")
                        checkpoint = self._create_marker_checkpoint()

                    # Persist the checkpoint
                    metrics = {"jit_checkpoint": True, "timestamp": time.time()}
                    self.train_session.storage._update_checkpoint_index(metrics)
                    persisted_checkpoint = (
                        self.train_session.storage.persist_current_checkpoint(checkpoint)
                    )

                    elapsed = time.time() - start_time
                    logger.info(
                        f"JIT checkpoint completed in {elapsed:.2f}s: {persisted_checkpoint}"
                    )

                    with self._lock:
                        self.checkpoint_completed = True
                    return

                except Exception as e:
                    logger.exception(f"Auto-checkpoint function failed: {e}")
                    logger.info("Falling back to marker-only checkpoint")

            # Fallback: Create marker-only checkpoint
            logger.warning("No checkpoint function registered, creating marker-only checkpoint")
            checkpoint = self._create_marker_checkpoint()

            # Persist the checkpoint
            metrics = {"jit_checkpoint": True, "timestamp": time.time()}
            self.train_session.storage._update_checkpoint_index(metrics)
            persisted_checkpoint = (
                self.train_session.storage.persist_current_checkpoint(checkpoint)
            )

            elapsed = time.time() - start_time
            logger.info(
                f"JIT checkpoint (marker only) completed in {elapsed:.2f}s: {persisted_checkpoint}"
            )

            with self._lock:
                self.checkpoint_completed = True

        except Exception as e:
            logger.exception(f"JIT checkpoint failed: {e}")
            # Don't re-raise - we want to fail gracefully
            # The worker will still be terminated, but we tried to save state

    def _create_marker_checkpoint(self):
        """Create a marker-only checkpoint when no checkpoint function available."""
        import os
        import tempfile

        from ray.train import Checkpoint

        with tempfile.TemporaryDirectory() as tmpdir:
            marker_path = os.path.join(tmpdir, "jit_checkpoint_marker.txt")
            with open(marker_path, "w") as f:
                f.write(f"JIT checkpoint created at {time.time()}\n")
                f.write(f"Worker rank: {self.train_session.world_rank}\n")

            return Checkpoint.from_directory(tmpdir)

    def register_signal_handler(self) -> None:
        """Register the SIGTERM signal handler.

        This should be called during training session initialization to
        enable JIT checkpointing. The original SIGTERM handler is stored
        so it can be restored during cleanup.
        """
        # Store the original handler
        self._original_sigterm_handler = signal.signal(
            signal.SIGTERM, self._sigterm_handler
        )
        logger.info("SIGTERM signal handler registered for JIT checkpointing")

    def cleanup(self) -> None:
        """Clean up resources and restore original signal handler.

        This should be called during training session shutdown to:
        1. Cancel any pending timers
        2. Wait for checkpoint thread to complete (if running)
        3. Restore the original SIGTERM handler
        """
        logger.info("Cleaning up JIT checkpoint handler")

        # Cancel timer if still pending
        if self._checkpoint_timer is not None:
            self._checkpoint_timer.cancel()
            self._checkpoint_timer = None

        # Wait for checkpoint thread to complete (with timeout)
        if self._checkpoint_thread is not None and self._checkpoint_thread.is_alive():
            logger.info("Waiting for checkpoint thread to complete...")
            self._checkpoint_thread.join(timeout=10.0)
            if self._checkpoint_thread.is_alive():
                logger.warning("Checkpoint thread did not complete within timeout")

        # Restore original signal handler
        if self._original_sigterm_handler is not None:
            signal.signal(signal.SIGTERM, self._original_sigterm_handler)
            logger.info("Original SIGTERM handler restored")

    def is_checkpoint_in_progress(self) -> bool:
        """Check if a checkpoint is currently in progress.

        Returns:
            True if checkpoint was requested but not yet completed.
        """
        with self._lock:
            return self.checkpoint_requested and not self.checkpoint_completed
