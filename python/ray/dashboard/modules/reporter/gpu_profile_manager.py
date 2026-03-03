import asyncio
import functools
import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from ray.dashboard.modules.reporter.profile_manager import (
    _format_failed_profiler_command,
)

import psutil

logger = logging.getLogger(__name__)


class GpuProfilingManager:
    """GPU profiling manager for Ray Dashboard.

    NOTE: The current implementation is based on the `dynolog` OSS project,
    but these are mostly implementation details that can be changed in the future.
    `dynolog` needs to be installed on the nodes where profiling is being done.

    This only supports Torch training scripts with KINETO_USE_DAEMON=1 set.
    It is not supported for other frameworks.
    """

    # Port for the monitoring daemon.
    # This port was chosen arbitrarily to a value to avoid conflicts.
    _DYNOLOG_PORT = 65406

    # Default timeout for the profiling operation.
    _DEFAULT_TIMEOUT_S = 5 * 60

    _NO_PROCESSES_MATCHED_ERROR_MESSAGE_PREFIX = "No processes were matched"

    _DISABLED_ERROR_MESSAGE = (
        "GPU profiling is not enabled on node {ip_address}. "
        "This is the case if no GPUs are detected on the node or if "
        "the profiling dependency `dynolog` is not installed on the node.\n"
        "Please ensure that GPUs are available on the node and that "
        "`dynolog` is installed."
    )
    _NO_PROCESSES_MATCHED_ERROR_MESSAGE = (
        "The profiling command failed for pid={pid} on node {ip_address}. "
        "There are a few potential reasons for this:\n"
        "1. The `KINETO_USE_DAEMON=1 KINETO_DAEMON_INIT_DELAY_S=5` environment variables "
        "are not set for the training worker processes.\n"
        "2. The process requested for profiling is not running a "
        "PyTorch training script. GPU profiling is only supported for "
        "PyTorch training scripts, typically launched via "
        "`ray.train.torch.TorchTrainer`."
    )
    _DEAD_PROCESS_ERROR_MESSAGE = (
        "The requested process to profile with pid={pid} on node "
        "{ip_address} is no longer running. "
        "GPU profiling is not available for this process."
    )

    def __init__(self, profile_dir_path: str, *, ip_address: str):
        # Dump trace files to: /tmp/ray/session_latest/logs/profiles/
        self._root_log_dir = Path(profile_dir_path)
        self._profile_dir_path = self._root_log_dir / "profiles"
        self._daemon_log_file_path = (
            self._profile_dir_path / f"dynolog_daemon_{os.getpid()}.log"
        )
        self._ip_address = ip_address

        self._dynolog_bin = shutil.which("dynolog")
        self._dyno_bin = shutil.which("dyno")

        self._dynolog_daemon_process: Optional[subprocess.Popen] = None

        if not self.node_has_gpus():
            logger.warning(
                "[GpuProfilingManager] No GPUs found on this node, GPU profiling will not be setup."
            )
        if not self._dynolog_bin or not self._dyno_bin:
            logger.warning(
                "[GpuProfilingManager] `dynolog` is not installed, GPU profiling will not be available."
            )

        self._profile_dir_path.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return (
            self.node_has_gpus()
            and self._dynolog_bin is not None
            and self._dyno_bin is not None
        )

    @property
    def is_monitoring_daemon_running(self) -> bool:
        return (
            self._dynolog_daemon_process is not None
            and self._dynolog_daemon_process.poll() is None
        )

    @classmethod
    @functools.cache
    def node_has_gpus(cls) -> bool:
        try:
            subprocess.check_output(["nvidia-smi"], stderr=subprocess.DEVNULL)
            return True
        except Exception:
            return False

    @classmethod
    def is_pid_alive(cls, pid: int) -> bool:
        try:
            return psutil.pid_exists(pid) and psutil.Process(pid).is_running()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return False

    def start_monitoring_daemon(self):
        """Start the GPU profiling monitoring daemon if it's possible.
        This must be called before profiling.
        """

        if not self.enabled:
            logger.warning(
                "[GpuProfilingManager] GPU profiling is disabled, skipping daemon setup."
            )
            return

        if self.is_monitoring_daemon_running:
            logger.warning(
                "[GpuProfilingManager] GPU profiling monitoring daemon is already running."
            )
            return

        try:
            with open(self._daemon_log_file_path, "ab") as log_file:
                daemon = subprocess.Popen(
                    [
                        self._dynolog_bin,
                        "--enable_ipc_monitor",
                        "--port",
                        str(self._DYNOLOG_PORT),
                    ],
                    stdout=log_file,
                    stderr=log_file,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(
                f"[GpuProfilingManager] Failed to launch GPU profiling monitoring daemon: {e}\n"
                f"Check error log for more details: {self._daemon_log_file_path}"
            )
            return

        logger.info(
            "[GpuProfilingManager] Launched GPU profiling monitoring daemon "
            f"(pid={daemon.pid}, port={self._DYNOLOG_PORT})\n"
            f"Redirecting logs to: {self._daemon_log_file_path}"
        )
        self._dynolog_daemon_process = daemon

    def _get_trace_filename(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"gputrace_{self._ip_address}_{timestamp}.json"

    async def gpu_profile(
        self, pid: int, num_iterations: int, _timeout_s: int = _DEFAULT_TIMEOUT_S
    ) -> Tuple[bool, str]:
        """
        Perform GPU profiling on a specified process.

        Args:
            pid: The process ID (PID) of the target process to be profiled.
            num_iterations: The number of iterations to profile.
            _timeout_s: Maximum time in seconds to wait for profiling to complete.
                This is an advanced parameter that catches edge cases where the
                profiling request never completes and hangs indefinitely.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating the success
                of the profiling operation and a string with the
                filepath of the trace file relative to the root log directory,
                or an error message.
        """
        if not self.enabled:
            return False, self._DISABLED_ERROR_MESSAGE.format(
                ip_address=self._ip_address
            )

        if not self._dynolog_daemon_process:
            raise RuntimeError("Must call `start_monitoring_daemon` before profiling.")

        if not self.is_monitoring_daemon_running:
            error_msg = (
                f"GPU monitoring daemon (pid={self._dynolog_daemon_process.pid}) "
                f"is not running on node {self._ip_address}. "
                f"See log for more details: {self._daemon_log_file_path}"
            )
            logger.error(f"[GpuProfilingManager] {error_msg}")
            return False, error_msg

        if not self.is_pid_alive(pid):
            error_msg = self._DEAD_PROCESS_ERROR_MESSAGE.format(
                pid=pid, ip_address=self._ip_address
            )
            logger.error(f"[GpuProfilingManager] {error_msg}")
            return False, error_msg

        trace_file_name = self._get_trace_filename()
        trace_file_path = self._profile_dir_path / trace_file_name

        cmd = [
            self._dyno_bin,
            "--port",
            str(self._DYNOLOG_PORT),
            "gputrace",
            "--pids",
            str(pid),
            "--log-file",
            str(trace_file_path),
            "--process-limit",
            str(1),
            "--iterations",
            str(num_iterations),
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return False, _format_failed_profiler_command(cmd, "dyno", stdout, stderr)

        stdout_str = stdout.decode("utf-8")
        logger.info(f"[GpuProfilingManager] Launched profiling: {stdout_str}")

        # The initial launch command returns immediately,
        # so wait for the profiling to actually finish before returning.
        # The indicator of the profiling finishing is the creation of the trace file,
        # when the completed trace is moved from <prefix>.tmp.json -> <prefix>.json
        # If the profiling request is invalid (e.g. "No processes were matched"),
        # the trace file will not be created and this will hang indefinitely,
        # up until the timeout is reached.

        # TODO(ml-team): This logic is brittle, we should find a better way to do this.
        if self._NO_PROCESSES_MATCHED_ERROR_MESSAGE_PREFIX in stdout_str:
            error_msg = self._NO_PROCESSES_MATCHED_ERROR_MESSAGE.format(
                pid=pid, ip_address=self._ip_address
            )
            logger.error(f"[GpuProfilingManager] {error_msg}")
            return False, error_msg

        # The actual trace file gets dumped with a suffix of `_{pid}.json
        trace_file_name_pattern = trace_file_name.replace(".json", "*.json")

        return await self._wait_for_trace_file(pid, trace_file_name_pattern, _timeout_s)

    async def _wait_for_trace_file(
        self,
        pid: int,
        trace_file_name_pattern: str,
        timeout_s: int,
        sleep_interval_s: float = 0.25,
    ) -> Tuple[bool, str]:
        """Wait for the trace file to be created.

        Args:
            pid: The target process to be profiled.
            trace_file_name_pattern: The pattern of the trace file to be created
                within the `<log_dir>/profiles` directory.
            timeout_s: Maximum time in seconds to wait for profiling to complete.
            sleep_interval_s: Time in seconds to sleep between checking for the trace file.

        Returns:
            Tuple[bool, str]: (success, trace file path relative to the *root* log directory)
        """
        remaining_timeout_s = timeout_s

        logger.info(
            "[GpuProfilingManager] Waiting for trace file to be created "
            f"with the pattern: {trace_file_name_pattern}"
        )

        while True:
            dumped_trace_file_path = next(
                self._profile_dir_path.glob(trace_file_name_pattern), None
            )
            if dumped_trace_file_path is not None:
                break

            await asyncio.sleep(sleep_interval_s)

            remaining_timeout_s -= sleep_interval_s
            if remaining_timeout_s <= 0:
                return (
                    False,
                    f"GPU profiling timed out after {timeout_s} seconds, please try again.",
                )

            # If the process has already exited, return an error.
            if not self.is_pid_alive(pid):
                return (
                    False,
                    self._DEAD_PROCESS_ERROR_MESSAGE.format(
                        pid=pid, ip_address=self._ip_address
                    ),
                )

        logger.info(
            f"[GpuProfilingManager] GPU profiling finished, trace file: {dumped_trace_file_path}"
        )
        return True, str(dumped_trace_file_path.relative_to(self._root_log_dir))
