import os
import platform
import tempfile
import time
import unittest
from contextlib import contextmanager
from pathlib import Path

from ray.rllib.env import EnvContext
from ray.rllib.examples.envs.classes.multi_agent.footsies.footsies_env import (
    env_creator,
)

# Detect platform and choose appropriate binary
if platform.system() == "Darwin":
    binary_to_download = "mac_headless"
elif platform.system() == "Linux":
    binary_to_download = "linux_server"
else:
    raise RuntimeError(f"Unsupported platform: {platform.system()}")

FOOTSIES_ENV_BASE_CONFIG = {
    "max_t": 1000,
    "frame_skip": 4,
    "observation_delay": 16,
    "host": "localhost",
    "binary_download_dir": "/tmp/ray/binaries/footsies",
    "binary_extract_dir": "/tmp/ray/binaries/footsies",
    "binary_to_download": binary_to_download,
}

_port_counter = 45001


def _create_env(config_overrides):
    global _port_counter

    config = {
        **FOOTSIES_ENV_BASE_CONFIG,
        "train_start_port": _port_counter,
        "eval_start_port": _port_counter + 1,
        **config_overrides,
    }
    _port_counter += 2

    return env_creator(EnvContext(config, worker_index=0))


@contextmanager
def capture_stdout_stderr():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        log_path = tmp.name

    # Use file descriptors 1 and 2 directly (stdout and stderr)
    # This bypasses pytest's wrapping of sys.stdout/stderr
    stdout_fd = 1
    stderr_fd = 2

    # Save original file descriptors
    saved_stdout = os.dup(stdout_fd)
    saved_stderr = os.dup(stderr_fd)

    log_file = None
    try:
        # Open log file with line buffering
        log_file = open(log_path, "w", buffering=1)
        os.dup2(log_file.fileno(), stdout_fd)
        os.dup2(log_file.fileno(), stderr_fd)

        yield log_path

    finally:
        # Flush the log file
        if log_file:
            log_file.flush()

        # Restore original file descriptors
        os.dup2(saved_stdout, stdout_fd)
        os.dup2(saved_stderr, stderr_fd)

        # Close file and saved descriptors
        if log_file:
            log_file.close()
        os.close(saved_stdout)
        os.close(saved_stderr)


class TestFootsies(unittest.TestCase):
    def test_default_supress_output_mode(self):
        with capture_stdout_stderr() as log_path:
            env = _create_env({})
            time.sleep(2)  # Give Unity time to write output
            env.close()
            # Give a bit more time for any buffered output to be written
            time.sleep(0.5)

        # Read the captured output
        with open(log_path, "r") as f:
            captured_output = f.read()

        assert (
            "`log_unity_output` not set in environment config, not logging output by default"
            in captured_output
        )
        assert "[UnityMemory]" not in captured_output

        # Clean up
        if Path(log_path).exists():
            os.unlink(log_path)

    def test_enable_output_mode(self):
        with capture_stdout_stderr() as log_path:
            env = _create_env({"log_unity_output": True})
            time.sleep(2)  # Give Unity time to write output
            env.close()
            # Give a bit more time for any buffered output to be written
            time.sleep(0.5)

        # Read the captured output
        with open(log_path, "r") as f:
            captured_output = f.read()

        assert "[UnityMemory]" in captured_output

        # Clean up
        if Path(log_path).exists():
            os.unlink(log_path)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
