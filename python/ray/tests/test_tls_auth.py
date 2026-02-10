# coding: utf-8
import logging
import os
import subprocess
import sys

import pytest

from ray._common.test_utils import run_string_as_driver

logger = logging.getLogger(__name__)


def build_env():
    env = os.environ.copy()
    if sys.platform == "win32" and "SYSTEMROOT" not in env:
        env["SYSTEMROOT"] = r"C:\Windows"

    return env


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_init_with_tls(use_tls):
    # Run as a new process to pick up environment variables set
    # in the use_tls fixture
    run_string_as_driver(
        """
import ray
try:
    ray.init()
finally:
    ray.shutdown()
    """,
        env=build_env(),
    )


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_put_get_with_tls(use_tls):
    run_string_as_driver(
        """
import ray
ray.init()
try:
    for i in range(100):
        value_before = i * 10**6
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = i * 10**6 * 1.0
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = "h" * i
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = [1] * i
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after
finally:
    ray.shutdown()
    """,
        env=build_env(),
    )


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True, scope="module")
def test_submit_with_tls(use_tls):
    run_string_as_driver(
        """
import ray
ray.init(num_cpus=2, num_gpus=1, resources={"Custom": 1})

@ray.remote
def f(n):
    return list(range(n))

id1, id2, id3 = f._remote(args=[3], num_returns=3)
assert ray.get([id1, id2, id3]) == [0, 1, 2]

@ray.remote
class Actor:
    def __init__(self, x, y=0):
        self.x = x
        self.y = y

    def method(self, a, b=0):
        return self.x, self.y, a, b

a = Actor._remote(
    args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})

id1, id2, id3, id4 = a.method._remote(
    args=["test"], kwargs={"b": 2}, num_returns=4)
assert ray.get([id1, id2, id3, id4]) == [0, 1, "test", 2]
    """,
        env=build_env(),
    )


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_client_connect_to_tls_server(use_tls, call_ray_start):
    tls_env = build_env()  # use_tls fixture sets TLS environment variables
    without_tls_env = {k: v for k, v in tls_env.items() if "TLS" not in k}

    # Attempt to connect without TLS
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        run_string_as_driver(
            """
from ray.util.client import ray as ray_client
ray_client.connect("localhost:10001")
     """,
            env=without_tls_env,
        )
    assert "ConnectionError" in exc_info.value.output.decode("utf-8")

    # Attempt to connect with TLS
    run_string_as_driver(
        """
import ray
from ray.util.client import ray as ray_client
ray_client.connect("localhost:10001")
assert ray.is_initialized()
     """,
        env=tls_env,
    )


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_metrics_export_with_tls(use_tls):
    """Test that metrics can be exported when TLS is enabled.

    This verifies that the OpenTelemetry metric exporter correctly configures
    TLS/mTLS credentials to communicate with the dashboard agent's gRPC server.
    See https://github.com/ray-project/ray/issues/59968
    """
    # Enable OpenTelemetry metrics along with TLS
    env = build_env()
    env["RAY_enable_open_telemetry"] = "true"

    run_string_as_driver(
        """
import ray
import time
import requests

# Configure faster metrics reporting
ray.init(_system_config={"metrics_report_interval_ms": 1000})
try:
    # Run a simple task to generate activity and trigger metrics
    @ray.remote
    def dummy_task():
        return 1
    ray.get(dummy_task.remote())

    # Wait for metrics to be available
    node_info = ray.nodes()[0]
    metrics_port = node_info["MetricsExportPort"]
    assert metrics_port > 0, f"Expected valid metrics port, got {metrics_port}"

    # Try to fetch metrics from the Prometheus endpoint
    metrics_url = f"http://127.0.0.1:{metrics_port}"

    # C++ component metrics that only appear when the OpenTelemetry gRPC exporter
    # successfully connects with TLS. Python metrics (~26) are exported even when
    # TLS is broken, but C++ metrics (~87 total) require working TLS configuration.
    cpp_metric_prefixes = ["ray_gcs_", "ray_object_directory_", "ray_grpc_"]

    # Give the metrics agent time to start and export C++ metrics
    # Use longer timeout since metrics may take time to be collected and exported
    for i in range(60):
        try:
            response = requests.get(metrics_url, timeout=2)
            if response.status_code == 200:
                # Check for C++ component metrics (proves TLS is working)
                has_cpp_metrics = any(
                    prefix in response.text for prefix in cpp_metric_prefixes
                )
                if has_cpp_metrics:
                    print(f"Found C++ metrics after {i+1} attempts")
                    break
                elif i % 10 == 0:
                    # Debug: show how many metrics we have
                    metric_lines = [l for l in response.text.split('\\n') if l.startswith('ray_')]
                    print(f"Attempt {i+1}: {len(metric_lines)} ray metrics found, waiting for C++ metrics...")
        except requests.exceptions.RequestException as e:
            if i % 10 == 0:
                print(f"Attempt {i+1}: request failed: {e}")
        time.sleep(1)
    else:
        # On failure, print what metrics we do have for debugging
        try:
            response = requests.get(metrics_url, timeout=2)
            metric_lines = [l for l in response.text.split('\\n') if l.startswith('ray_')]
            print(f"Final metrics ({len(metric_lines)}): {metric_lines[:10]}...")
        except Exception:
            pass
        raise AssertionError(
            "Failed to fetch C++ component metrics within timeout. "
            "This indicates TLS configuration may not be working correctly."
        )
finally:
    ray.shutdown()
    """,
        env=env,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
