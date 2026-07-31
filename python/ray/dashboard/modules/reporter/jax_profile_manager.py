import asyncio
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class JaxProfilingManager:
    """JAX profiling manager for Ray Dashboard.

    It connects to the JAX profiler server running on the worker
    and captures a trace using TensorFlow's profiler client.
    """

    def __init__(self, profile_dir_path: str):
        self._root_log_dir = Path(profile_dir_path)
        self._profile_dir_path = self._root_log_dir / "profiles"
        self._profile_dir_path.mkdir(parents=True, exist_ok=True)
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="jax_profiling_executor"
        )
        self._inflight: Optional[Future] = None

    async def jax_profile(
        self, pid: int, port: int, duration_s: int = 5
    ) -> Tuple[bool, str]:
        """Perform JAX profiling by connecting to the JAX server.

        Args:
            pid: The process ID of the target process (for logging/tracking).
            port: The port where JAX profiler server is listening.
            duration_s: Duration of the profiling in seconds.

        Returns:
            Tuple[bool, str]: (success, trace file path relative to root log dir)
        """
        if self._inflight is not None and not self._inflight.done():
            return (
                False,
                "Another JAX profiling session is already in progress on this node.",
            )

        try:
            from tensorflow.python.profiler import profiler_client
        except ImportError as e:
            return (
                False,
                "TensorFlow is required to capture JAX profiles from the Dashboard. "
                f"Please install `tensorflow` on the node. Error: {e}",
            )

        address = f"grpc://localhost:{port}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        capture_dir = self._profile_dir_path / f"{pid}_{timestamp}"
        logger.info(
            f"Capturing JAX profile from {address} for pid {pid} "
            f"for {duration_s} seconds..."
        )

        def _capture():
            try:
                # profiler_client.trace captures the trace and saves it to logdir
                profiler_client.trace(
                    address,
                    logdir=str(capture_dir),
                    duration_ms=duration_s * 1000,
                )
                return True, ""
            except Exception as e:
                return False, f"Failed to capture trace: {e}"

        # Run in executor because trace is blocking
        future = self._executor.submit(_capture)
        self._inflight = future
        try:
            success, error_msg = await asyncio.wait_for(
                asyncio.wrap_future(future),
                timeout=duration_s + 20,
            )
        except asyncio.TimeoutError:
            logger.error(
                f"JAX profiling timed out after {duration_s + 20} seconds. "
                "The capture may still be finishing in the background."
            )
            return (
                False,
                f"JAX profiling timed out after {duration_s + 20} seconds. "
                "The capture may still be finishing, retry shortly.",
            )

        if not success:
            logger.error(f"JAX profiling failed: {error_msg}")
            return False, error_msg

        logger.info(f"JAX profiling finished. Files saved in {capture_dir}")

        # Return the directory path relative to the root log directory
        return True, str(capture_dir.relative_to(self._root_log_dir))
