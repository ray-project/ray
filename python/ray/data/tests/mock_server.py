import shutil
import signal
import socket
import subprocess as sp
import time

# extracted from aioboto3
#    https://github.com/terrycain/aioboto3/blob/16a1a1085191ebe6d40ee45d9588b2173738af0c/tests/mock_server.py
import pytest
import requests

from ray._common.network_utils import build_address

_proxy_bypass = {
    "http": None,
    "https": None,
}


def _is_port_available(host, port):
    """Check if a port is available for use."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            return True
    except OSError:
        return False


def _find_available_port(host, preferred_port, max_attempts=10):
    """Find an available port starting from preferred_port."""

    # Try the preferred port first
    if _is_port_available(host, preferred_port):
        return preferred_port

    # Try a wider range if preferred port is busy
    for i in range(1, max_attempts):
        port = preferred_port + i
        if _is_port_available(host, port):
            return port

    # If all else fails, let the OS pick a port
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, 0))  # Let OS pick port
            _, port = s.getsockname()
            return port
    except OSError as e:
        raise RuntimeError(
            f"Could not find any available port starting from " f"{preferred_port}: {e}"
        ) from e


def start_service(service_name, host, port):
    moto_svr_path = shutil.which("moto_server")
    if not moto_svr_path:
        pytest.skip("moto not installed")

    # Always use port conflict resolution to be safe
    port = _find_available_port(host, port)

    args = [moto_svr_path, service_name, "-H", host, "-p", str(port)]
    # For debugging
    # args = '{0} {1} -H {2} -p {3} 2>&1 | \
    # tee -a /tmp/moto.log'.format(moto_svr_path, service_name, host, port)
    process = sp.Popen(
        args, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE
    )  # shell=True
    url = f"http://{build_address(host, port)}"

    for i in range(0, 30):
        output = process.poll()
        if output is not None:
            print("moto_server exited status {0}".format(output))
            stdout, stderr = process.communicate()
            print("moto_server stdout: {0}".format(stdout))
            print("moto_server stderr: {0}".format(stderr))
            pytest.fail("Can not start service: {}".format(service_name))

        try:
            # we need to bypass the proxies due to monkeypatches
            requests.get(url, timeout=5, proxies=_proxy_bypass)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)
    else:
        stop_process(process)  # pytest.fail doesn't call stop_process
        pytest.fail("Can not start service: {}".format(service_name))

    return process, url


def stop_process(process):
    """Stop process with shorter timeout to prevent test hangs."""
    if process is None or process.poll() is not None:
        return  # Already stopped

    try:
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=20)
    except sp.TimeoutExpired:
        process.kill()
        try:
            process.communicate(timeout=5)  # Short timeout for kill
        except sp.TimeoutExpired:
            print("Warning: Process cleanup timed out")
    except Exception as e:
        print(f"Warning: Error during process cleanup: {e}")


# TODO(Clark): We should be able to use "session" scope here, but we've found
# that the s3_fs fixture ends up hanging with S3 ops timing out (or the server
# being unreachable). This appears to only be an issue when using the tmp_dir
# fixture as the S3 dir path. We should fix this since "session" scope should
# reduce a lot of the per-test overhead (2x faster execution for IO methods in
# test_dataset.py).
@pytest.fixture(scope="function")
def s3_server():
    host = "localhost"
    port = 5002
    process, url = start_service("s3", host, port)
    yield url
    stop_process(process)
