"""Async checkpoint writer for Ray Train.

This module provides non-blocking checkpoint uploading capabilities for Ray Train,
specifically designed for RL on reasoning models that checkpoint frequently.
"""

import concurrent.futures
import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ray.train import Checkpoint
from ray.train._internal.storage import StorageContext, _pyarrow_fs_copy_files
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@dataclass
class _AsyncCheckpointTask:
    """Represents an async checkpoint upload task."""
    checkpoint: Checkpoint
    storage_context: StorageContext
    metrics: Dict[str, Any]
    checkpoint_fs_path: str
    future: Optional[concurrent.futures.Future] = None
    submit_time: float = 0.0


class _AsyncCheckpointError(Exception):
    """Raised when an async checkpoint operation fails."""
    def __init__(self, message: str, original_exception: Exception):
        super().__init__(message)
        self.original_exception = original_exception


@DeveloperAPI
class AsyncCheckpointWriter:
    """Manages asynchronous checkpoint uploads with backpressure control.
    
    Features:
    - Non-blocking checkpoint uploads via background thread pool
    - Backpressure control to prevent unlimited queuing
    - Atomic manifest writing (LATEST.json) on upload completion
    - Error reporting on next async_report call with flush=True
    - Graceful shutdown with pending uploads completion
    
    This is designed for RL training where frequent checkpointing to S3/GCS
    should not block the training step.
    """

    def __init__(self, max_concurrent_uploads: int = 2, max_pending_uploads: int = 5):
        """Initialize the async checkpoint writer.
        
        Args:
            max_concurrent_uploads: Maximum number of concurrent upload threads.
            max_pending_uploads: Maximum number of pending uploads before blocking.
        """
        self._max_concurrent_uploads = max_concurrent_uploads
        self._max_pending_uploads = max_pending_uploads
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent_uploads,
            thread_name_prefix="async_checkpoint"
        )
        self._pending_tasks: queue.Queue[_AsyncCheckpointTask] = queue.Queue(
            maxsize=max_pending_uploads
        )
        self._completed_tasks: queue.Queue[_AsyncCheckpointTask] = queue.Queue()
        self._failed_tasks: queue.Queue[Tuple[_AsyncCheckpointTask, Exception]] = queue.Queue()
        self._shutdown = False
        self._lock = threading.Lock()
        
        # Start the task submission thread
        self._submission_thread = threading.Thread(
            target=self._submission_worker,
            daemon=True,
            name="async_checkpoint_submitter"
        )
        self._submission_thread.start()

    def async_persist_checkpoint(
        self,
        checkpoint: Checkpoint,
        storage_context: StorageContext,
        metrics: Dict[str, Any],
    ) -> None:
        """Submit a checkpoint for asynchronous upload.
        
        Args:
            checkpoint: The checkpoint to persist.
            storage_context: Storage context for upload destination.
            metrics: Metrics associated with this checkpoint.
            
        Raises:
            _AsyncCheckpointError: If the queue is full (backpressure).
        """
        if self._shutdown:
            raise RuntimeError("AsyncCheckpointWriter has been shut down")

        # Create checkpoint fs path (same logic as StorageContext)
        checkpoint_fs_path = storage_context.checkpoint_fs_path
        
        task = _AsyncCheckpointTask(
            checkpoint=checkpoint,
            storage_context=storage_context,
            metrics=metrics,
            checkpoint_fs_path=checkpoint_fs_path,
            submit_time=time.time()
        )
        
        try:
            # Non-blocking put with timeout for backpressure
            self._pending_tasks.put(task, block=True, timeout=0.1)
            logger.debug(f"Queued async checkpoint upload to {checkpoint_fs_path}")
        except queue.Full:
            raise _AsyncCheckpointError(
                "Too many pending checkpoint uploads. Consider reducing checkpoint frequency "
                "or increasing max_pending_uploads.",
                queue.Full()
            )

    def _submission_worker(self):
        """Background thread that submits tasks to the thread pool executor."""
        while not self._shutdown:
            try:
                task = self._pending_tasks.get(timeout=1.0)
                if task is None:  # Shutdown signal
                    break
                    
                # Submit the actual upload task
                future = self._executor.submit(self._upload_checkpoint_task, task)
                task.future = future
                
                # Add callback to handle completion
                future.add_done_callback(lambda f, t=task: self._on_task_complete(f, t))
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.exception("Error in async checkpoint submission worker")

    def _upload_checkpoint_task(self, task: _AsyncCheckpointTask) -> Checkpoint:
        """Upload a single checkpoint (runs in background thread)."""
        try:
            # Validate storage path access (same as sync version)
            task.storage_context._check_validation_file()
            
            # Create destination directory
            task.storage_context.storage_filesystem.create_dir(task.checkpoint_fs_path)
            
            # Copy files to storage
            logger.debug(
                f"Starting async upload: {task.checkpoint.path} -> {task.checkpoint_fs_path}"
            )
            
            _pyarrow_fs_copy_files(
                source=task.checkpoint.path,
                destination=task.checkpoint_fs_path,
                source_filesystem=task.checkpoint.filesystem,
                destination_filesystem=task.storage_context.storage_filesystem,
            )
            
            # Create persisted checkpoint object
            persisted_checkpoint = task.checkpoint.__class__(
                filesystem=task.storage_context.storage_filesystem,
                path=task.checkpoint_fs_path,
            )
            
            # Write atomic manifest (LATEST.json)
            self._write_latest_manifest(task.storage_context, task.metrics, persisted_checkpoint)
            
            logger.info(f"Async checkpoint upload completed: {persisted_checkpoint}")
            return persisted_checkpoint
            
        except Exception as e:
            logger.error(f"Async checkpoint upload failed: {e}")
            raise

    def _write_latest_manifest(
        self,
        storage_context: StorageContext,
        metrics: Dict[str, Any],
        checkpoint: Checkpoint
    ):
        """Write atomic LATEST.json manifest to indicate completed upload."""
        try:
            manifest_data = {
                "checkpoint_path": checkpoint.path,
                "metrics": metrics,
                "timestamp": time.time(),
                "upload_completed": True
            }
            
            # Write to experiment directory  
            manifest_path = Path(storage_context.experiment_fs_path, "LATEST.json").as_posix()
            
            # Write atomically by writing to temp file then renaming
            temp_manifest_path = f"{manifest_path}.tmp"
            
            with storage_context.storage_filesystem.open_output_stream(temp_manifest_path) as f:
                f.write(json.dumps(manifest_data, indent=2).encode())
            
            # Atomic rename (most filesystems support this atomically)
            storage_context.storage_filesystem.move(temp_manifest_path, manifest_path)
            
            logger.debug(f"Wrote LATEST.json manifest to {manifest_path}")
            
        except Exception as e:
            logger.warning(f"Failed to write LATEST.json manifest: {e}")
            # Don't fail the whole upload for manifest issues

    def _on_task_complete(self, future: concurrent.futures.Future, task: _AsyncCheckpointTask):
        """Handle completion of an upload task."""
        try:
            if future.exception():
                self._failed_tasks.put((task, future.exception()))
            else:
                result = future.result()
                self._completed_tasks.put(task)
        except Exception as e:
            logger.exception("Error handling async checkpoint task completion")
            self._failed_tasks.put((task, e))

    def check_and_raise_errors(self):
        """Check for failed uploads and raise the first error found.
        
        This should be called with flush=True to report errors from
        previous async uploads.
        """
        try:
            failed_task, exception = self._failed_tasks.get_nowait()
            raise _AsyncCheckpointError(
                f"Async checkpoint upload failed for {failed_task.checkpoint_fs_path}",
                exception
            )
        except queue.Empty:
            pass

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about async checkpoint operations."""
        return {
            "pending_uploads": self._pending_tasks.qsize(),
            "completed_uploads": self._completed_tasks.qsize(),
            "failed_uploads": self._failed_tasks.qsize(),
        }

    def shutdown(self, timeout: float = 30.0):
        """Gracefully shutdown the async checkpoint writer.
        
        Args:
            timeout: Maximum time to wait for pending uploads to complete.
        """
        with self._lock:
            if self._shutdown:
                return
            
            self._shutdown = True
            
        logger.info("Shutting down AsyncCheckpointWriter...")
        
        # Signal submission worker to stop
        try:
            self._pending_tasks.put(None, timeout=1.0)
        except queue.Full:
            pass
            
        # Wait for submission thread
        self._submission_thread.join(timeout=5.0)
        
        # Shutdown executor and wait for running tasks
        self._executor.shutdown(wait=True)
        
        stats = self.get_stats()
        logger.info(f"AsyncCheckpointWriter shutdown complete. Final stats: {stats}")


# Global instance for the session
_async_checkpoint_writer: Optional[AsyncCheckpointWriter] = None


def _get_async_checkpoint_writer() -> AsyncCheckpointWriter:
    """Get or create the global async checkpoint writer."""
    global _async_checkpoint_writer
    if _async_checkpoint_writer is None:
        _async_checkpoint_writer = AsyncCheckpointWriter()
    return _async_checkpoint_writer


def _shutdown_async_checkpoint_writer():
    """Shutdown the global async checkpoint writer."""
    global _async_checkpoint_writer
    if _async_checkpoint_writer is not None:
        _async_checkpoint_writer.shutdown()
        _async_checkpoint_writer = None
