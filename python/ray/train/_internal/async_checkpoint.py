"""Async checkpoint writer for Ray Train.

This module provides non-blocking checkpoint uploading capabilities for Ray Train,
specifically designed for training that checkpoint frequently and/or asynchronously.
"""

import concurrent.futures
import json
import logging
import os
import queue
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
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
    retry_count: int = 0
    checkpoint_size_bytes: int = 0
    upload_start_time: Optional[float] = None


class _AsyncCheckpointError(Exception):
    """Raised when an async checkpoint operation fails."""

    def __init__(self, message: str, original_exception: Exception):
        super().__init__(message)
        self.original_exception = original_exception


@dataclass
class UploadProgress:
    """Progress information for an upload."""

    checkpoint_path: str
    bytes_uploaded: int = 0
    total_bytes: int = 0
    start_time: float = 0.0
    current_file: str = ""

    @property
    def progress_percent(self) -> float:
        """Get upload progress as percentage."""
        if self.total_bytes == 0:
            return 0.0
        return min(100.0, (self.bytes_uploaded / self.total_bytes) * 100.0)

    @property
    def elapsed_time(self) -> float:
        """Get elapsed upload time in seconds."""
        return time.time() - self.start_time if self.start_time > 0 else 0.0

    @property
    def upload_speed_mbps(self) -> float:
        """Get current upload speed in MB/s."""
        elapsed = self.elapsed_time
        if elapsed == 0 or self.bytes_uploaded == 0:
            return 0.0
        return (self.bytes_uploaded / (1024 * 1024)) / elapsed


@dataclass
class UploadMetrics:
    """Aggregate metrics for all upload operations."""

    total_uploads: int = 0
    successful_uploads: int = 0
    failed_uploads: int = 0
    total_bytes_uploaded: int = 0
    total_upload_time: float = 0.0
    error_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    active_uploads: Dict[str, UploadProgress] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        """Get upload success rate as percentage."""
        if self.total_uploads == 0:
            return 100.0
        return (self.successful_uploads / self.total_uploads) * 100.0

    @property
    def average_upload_time(self) -> float:
        """Get average upload time in seconds."""
        if self.successful_uploads == 0:
            return 0.0
        return self.total_upload_time / self.successful_uploads

    @property
    def average_upload_speed_mbps(self) -> float:
        """Get average upload speed in MB/s."""
        if self.total_upload_time == 0 or self.total_bytes_uploaded == 0:
            return 0.0
        return (self.total_bytes_uploaded / (1024 * 1024)) / self.total_upload_time


def _get_default_config() -> Dict[str, Any]:
    """Get default configuration from environment variables."""
    return {
        "max_concurrent_uploads": int(
            os.environ.get("RAY_ASYNC_CHECKPOINT_MAX_CONCURRENT", "2")
        ),
        "max_pending_uploads": int(
            os.environ.get("RAY_ASYNC_CHECKPOINT_MAX_PENDING", "5")
        ),
        "retry_attempts": int(
            os.environ.get("RAY_ASYNC_CHECKPOINT_RETRY_ATTEMPTS", "3")
        ),
        "retry_delay": float(os.environ.get("RAY_ASYNC_CHECKPOINT_RETRY_DELAY", "1.0")),
        "upload_timeout": float(
            os.environ.get("RAY_ASYNC_CHECKPOINT_UPLOAD_TIMEOUT", "300.0")
        ),
        "enable_compression": os.environ.get(
            "RAY_ASYNC_CHECKPOINT_COMPRESS", "false"
        ).lower()
        == "true",
    }


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

    def __init__(
        self,
        max_concurrent_uploads: Optional[int] = None,
        max_pending_uploads: Optional[int] = None,
        retry_attempts: Optional[int] = None,
        retry_delay: Optional[float] = None,
        upload_timeout: Optional[float] = None,
        enable_compression: Optional[bool] = None,
    ):
        """Initialize the async checkpoint writer.

        Args:
            max_concurrent_uploads: Maximum number of concurrent upload threads.
            max_pending_uploads: Maximum number of pending uploads before blocking.
            retry_attempts: Number of retry attempts for failed uploads.
            retry_delay: Base delay between retries (exponential backoff applied).
            upload_timeout: Timeout in seconds for individual uploads.
            enable_compression: Whether to compress checkpoints before upload.
        """
        # Get defaults from environment variables
        config = _get_default_config()

        self._max_concurrent_uploads = (
            max_concurrent_uploads or config["max_concurrent_uploads"]
        )
        self._max_pending_uploads = max_pending_uploads or config["max_pending_uploads"]
        self._retry_attempts = retry_attempts or config["retry_attempts"]
        self._retry_delay = retry_delay or config["retry_delay"]
        self._upload_timeout = upload_timeout or config["upload_timeout"]
        self._enable_compression = enable_compression or config["enable_compression"]
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._max_concurrent_uploads,
            thread_name_prefix="async_checkpoint",
        )
        self._pending_tasks: queue.Queue[_AsyncCheckpointTask] = queue.Queue(
            maxsize=self._max_pending_uploads
        )
        self._completed_tasks: queue.Queue[_AsyncCheckpointTask] = queue.Queue()
        self._failed_tasks: queue.Queue[
            Tuple[_AsyncCheckpointTask, Exception]
        ] = queue.Queue()
        self._shutdown = False
        self._lock = threading.Lock()

        self._metrics = UploadMetrics()
        self._metrics_lock = threading.Lock()

        # Start the task submission thread
        self._submission_thread = threading.Thread(
            target=self._submission_worker,
            daemon=True,
            name="async_checkpoint_submitter",
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

        checkpoint_fs_path = storage_context.checkpoint_fs_path

        checkpoint_size = self._calculate_checkpoint_size(checkpoint)
        task = _AsyncCheckpointTask(
            checkpoint=checkpoint,
            storage_context=storage_context,
            metrics=metrics,
            checkpoint_fs_path=checkpoint_fs_path,
            submit_time=time.time(),
            checkpoint_size_bytes=checkpoint_size,
        )

        try:
            self._pending_tasks.put(task, block=True, timeout=0.1)
            logger.debug(f"Queued async checkpoint upload to {checkpoint_fs_path}")
            with self._metrics_lock:
                self._metrics.total_uploads += 1

        except queue.Full:
            raise _AsyncCheckpointError(
                "Too many pending checkpoint uploads. Consider reducing checkpoint frequency "
                "or increasing max_pending_uploads.",
                queue.Full(),
            )

    def _calculate_checkpoint_size(self, checkpoint: Checkpoint) -> int:
        """Calculate the total size of a checkpoint in bytes."""
        try:
            if hasattr(checkpoint.filesystem, "get_file_info"):
                # PyArrow filesystem
                file_info = checkpoint.filesystem.get_file_info([checkpoint.path])
                if file_info and len(file_info) > 0:
                    info = file_info[0]
                    if info.type.name == "Directory":
                        # Sum all files in directory
                        total_size = 0
                        for file_path in checkpoint.filesystem.get_file_info(
                            checkpoint.filesystem.get_file_info(
                                checkpoint.path, recursive=True
                            )
                        ):
                            if hasattr(file_path, "size") and file_path.size:
                                total_size += file_path.size
                        return total_size
                    else:
                        return info.size or 0

            # Fallback: use local filesystem if possible
            if isinstance(
                checkpoint.filesystem, type(checkpoint.filesystem)
            ) and hasattr(checkpoint.filesystem, "type_name"):
                if checkpoint.filesystem.type_name == "local":
                    path = Path(checkpoint.path)
                    if path.is_file():
                        return path.stat().st_size
                    elif path.is_dir():
                        return sum(
                            f.stat().st_size for f in path.rglob("*") if f.is_file()
                        )

            return 0  # Unknown size

        except Exception as e:
            logger.warning(f"Could not calculate checkpoint size: {e}")
            return 0

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
        """Upload a single checkpoint with retry logic and timeout."""
        task.upload_start_time = time.time()
        last_exception = None

        # Create progress tracker
        progress = UploadProgress(
            checkpoint_path=task.checkpoint_fs_path,
            total_bytes=task.checkpoint_size_bytes,
            start_time=task.upload_start_time,
        )

        with self._metrics_lock:
            self._metrics.active_uploads[task.checkpoint_fs_path] = progress

        try:
            for attempt in range(self._retry_attempts):
                try:
                    task.retry_count = attempt

                    # Validate storage path access (same as sync version)
                    task.storage_context._check_validation_file()

                    # Create destination directory
                    task.storage_context.storage_filesystem.create_dir(
                        task.checkpoint_fs_path
                    )

                    # Copy files to storage with timeout
                    logger.debug(
                        f"Starting async upload attempt {attempt + 1}/{self._retry_attempts}: "
                        f"{task.checkpoint.path} -> {task.checkpoint_fs_path}"
                    )

                    upload_future = self._executor.submit(
                        self._copy_files_with_progress, task, progress
                    )

                    # Wait for upload with timeout
                    try:
                        upload_future.result(timeout=self._upload_timeout)
                    except concurrent.futures.TimeoutError:
                        upload_future.cancel()
                        raise TimeoutError(
                            f"Upload timed out after {self._upload_timeout} seconds"
                        )

                    # Create persisted checkpoint object
                    persisted_checkpoint = task.checkpoint.__class__(
                        filesystem=task.storage_context.storage_filesystem,
                        path=task.checkpoint_fs_path,
                    )

                    # Write atomic manifest (LATEST.json)
                    self._write_latest_manifest(
                        task.storage_context, task.metrics, persisted_checkpoint
                    )

                    # Update metrics on success
                    upload_time = time.time() - task.upload_start_time
                    with self._metrics_lock:
                        self._metrics.successful_uploads += 1
                        self._metrics.total_bytes_uploaded += task.checkpoint_size_bytes
                        self._metrics.total_upload_time += upload_time
                        if task.checkpoint_fs_path in self._metrics.active_uploads:
                            del self._metrics.active_uploads[task.checkpoint_fs_path]

                    logger.info(
                        f"Async checkpoint upload completed in {upload_time:.2f}s: {persisted_checkpoint}"
                    )
                    return persisted_checkpoint

                except Exception as e:
                    last_exception = e
                    error_type = type(e).__name__

                    with self._metrics_lock:
                        self._metrics.error_counts[error_type] += 1

                    if attempt < self._retry_attempts - 1:
                        wait_time = self._retry_delay * (
                            2**attempt
                        )  # Exponential backoff
                        logger.warning(
                            f"Upload attempt {attempt + 1} failed, retrying in {wait_time}s: {e}"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(
                            f"Upload failed after {self._retry_attempts} attempts: {e}"
                        )

            # If we get here, all retries failed
            with self._metrics_lock:
                self._metrics.failed_uploads += 1
                if task.checkpoint_fs_path in self._metrics.active_uploads:
                    del self._metrics.active_uploads[task.checkpoint_fs_path]

            raise last_exception or RuntimeError("Upload failed for unknown reason")

        finally:
            # Always clean up active uploads tracking
            with self._metrics_lock:
                if task.checkpoint_fs_path in self._metrics.active_uploads:
                    del self._metrics.active_uploads[task.checkpoint_fs_path]

    def _copy_files_with_progress(
        self, task: _AsyncCheckpointTask, progress: UploadProgress
    ):
        """Copy files with progress tracking."""
        try:
            _pyarrow_fs_copy_files(
                source=task.checkpoint.path,
                destination=task.checkpoint_fs_path,
                source_filesystem=task.checkpoint.filesystem,
                destination_filesystem=task.storage_context.storage_filesystem,
            )

            # Update progress to 100% on completion
            progress.bytes_uploaded = progress.total_bytes

        except Exception as e:
            logger.error(f"File copy failed: {e}")
            raise

    def _write_latest_manifest(
        self,
        storage_context: StorageContext,
        metrics: Dict[str, Any],
        checkpoint: Checkpoint,
    ):
        """Write atomic LATEST.json manifest to indicate completed upload."""
        try:
            manifest_data = {
                "checkpoint_path": checkpoint.path,
                "metrics": metrics,
                "timestamp": time.time(),
                "upload_completed": True,
            }

            # Write to experiment directory
            manifest_path = Path(
                storage_context.experiment_fs_path, "LATEST.json"
            ).as_posix()

            # Write atomically by writing to temp file then renaming
            temp_manifest_path = f"{manifest_path}.tmp"

            with storage_context.storage_filesystem.open_output_stream(
                temp_manifest_path
            ) as f:
                f.write(json.dumps(manifest_data, indent=2).encode())

            # Atomic rename (most filesystems support this atomically)
            storage_context.storage_filesystem.move(temp_manifest_path, manifest_path)

            logger.debug(f"Wrote LATEST.json manifest to {manifest_path}")

        except Exception as e:
            logger.warning(f"Failed to write LATEST.json manifest: {e}")
            # Don't fail the whole upload for manifest issues

    def _on_task_complete(
        self, future: concurrent.futures.Future, task: _AsyncCheckpointTask
    ):
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
                exception,
            )
        except queue.Empty:
            pass

    def get_stats(self) -> Dict[str, int]:
        """Get basic statistics about async checkpoint operations."""
        return {
            "pending_uploads": self._pending_tasks.qsize(),
            "completed_uploads": self._completed_tasks.qsize(),
            "failed_uploads": self._failed_tasks.qsize(),
        }

    def get_upload_metrics(self) -> UploadMetrics:
        """Get detailed upload metrics and progress information."""
        with self._metrics_lock:
            # Create a copy of metrics to avoid race conditions
            metrics_copy = UploadMetrics(
                total_uploads=self._metrics.total_uploads,
                successful_uploads=self._metrics.successful_uploads,
                failed_uploads=self._metrics.failed_uploads,
                total_bytes_uploaded=self._metrics.total_bytes_uploaded,
                total_upload_time=self._metrics.total_upload_time,
                error_counts=dict(self._metrics.error_counts),
                active_uploads=dict(self._metrics.active_uploads),
            )
        return metrics_copy

    def get_active_uploads(self) -> Dict[str, UploadProgress]:
        """Get progress information for currently active uploads."""
        with self._metrics_lock:
            return dict(self._metrics.active_uploads)

    def print_upload_status(self) -> None:
        """Print a human-readable status of all uploads."""
        metrics = self.get_upload_metrics()
        active_uploads = self.get_active_uploads()

        print(f"\n=== Async Checkpoint Upload Status ===")
        print(f"Total uploads: {metrics.total_uploads}")
        print(f"Successful: {metrics.successful_uploads} ({metrics.success_rate:.1f}%)")
        print(f"Failed: {metrics.failed_uploads}")
        print(f"Active uploads: {len(active_uploads)}")
        print(f"Pending uploads: {self._pending_tasks.qsize()}")

        if metrics.successful_uploads > 0:
            print(f"Average upload time: {metrics.average_upload_time:.2f}s")
            print(f"Average upload speed: {metrics.average_upload_speed_mbps:.2f} MB/s")
            print(
                f"Total data uploaded: {metrics.total_bytes_uploaded / (1024*1024):.1f} MB"
            )

        if active_uploads:
            print(f"\nActive uploads:")
            for path, progress in active_uploads.items():
                print(
                    f"  {Path(path).name}: {progress.progress_percent:.1f}% "
                    f"({progress.upload_speed_mbps:.2f} MB/s)"
                )

        if metrics.error_counts:
            print(f"\nError summary:")
            for error_type, count in metrics.error_counts.items():
                print(f"  {error_type}: {count}")
        print("=" * 40)

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


def get_async_checkpoint_metrics() -> Optional[UploadMetrics]:
    """Get detailed metrics about async checkpoint operations.

    Returns:
        UploadMetrics object with detailed statistics, or None if no writer exists.
    """
    global _async_checkpoint_writer
    if _async_checkpoint_writer is not None:
        return _async_checkpoint_writer.get_upload_metrics()
    return None


def print_async_checkpoint_status() -> None:
    """Print human-readable status of async checkpoint operations."""
    global _async_checkpoint_writer
    if _async_checkpoint_writer is not None:
        _async_checkpoint_writer.print_upload_status()
    else:
        print("No async checkpoint writer active.")


def _shutdown_async_checkpoint_writer():
    """Shutdown the global async checkpoint writer."""
    global _async_checkpoint_writer
    if _async_checkpoint_writer is not None:
        _async_checkpoint_writer.shutdown()
        _async_checkpoint_writer = None
