"""Shared test utilities for ci/ray_ci/automation tests."""

import os
import platform
import random
import shutil
import subprocess
import tempfile
import threading
import time

import pytest
import requests
import runfiles


def _registry_binary():
    """Get the path to the local registry binary."""
    r = runfiles.Create()
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise ValueError(f"Unsupported platform: {system}")
    return r.Rlocation("registry_x86_64/registry")


def _start_local_registry():
    """Start local registry for testing.

    Returns:
        Tuple of (registry_proc, registry_thread, temp_dir, port)
    """
    port = random.randint(2000, 20000)
    temp_dir = tempfile.mkdtemp()
    config_content = "\n".join(
        [
            "version: 0.1",
            "storage:",
            "    filesystem:",
            f"        rootdirectory: {temp_dir}",
            "http:",
            f"    addr: :{port}",
        ]
    )

    config_path = os.path.join(temp_dir, "registry.yml")
    with open(config_path, "w") as config_file:
        config_file.write(config_content)
        config_file.flush()

    registry_proc = subprocess.Popen(
        [_registry_binary(), "serve", config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    registry_thread = threading.Thread(target=lambda: registry_proc.wait(), daemon=True)
    registry_thread.start()

    for _ in range(10):
        try:
            response = requests.get(f"http://localhost:{port}/v2/")
            if response.status_code == 200:
                return registry_proc, registry_thread, temp_dir, port
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)

    raise TimeoutError("Registry failed to start within 10 seconds")


@pytest.fixture(scope="module")
def local_registry():
    """Fixture that provides a local Docker registry for testing.

    Yields:
        int: The port number of the local registry.
    """
    registry_proc, registry_thread, temp_dir, port = _start_local_registry()
    yield port
    registry_proc.kill()
    registry_thread.join(timeout=5)
    shutil.rmtree(temp_dir, ignore_errors=True)
