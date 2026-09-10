"""Integration test for Prometheus-backed custom autoscaling policies.

Runs a real deployment through the controller's autoscaling loop. The policy
fetches from a mock Prometheus HTTP endpoint; only Prometheus query evaluation
is stubbed.
"""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.test_utils import check_num_replicas_eq
from ray.serve.autoscaling_policy import PrometheusQueryMixin
from ray.serve.config import AutoscalingPolicy

_PROMETHEUS_VALUE = {"value": 5.0}


class _MockPrometheusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payload = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {},
                        "value": [0, str(_PROMETHEUS_VALUE["value"])],
                    },
                ],
            },
        }
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _ThresholdPolicy(PrometheusQueryMixin):
    """Add a replica while the signal exceeds a threshold, else remove one."""

    def __init__(self, query="signal", threshold=1.0, **kwargs):
        super().__init__(prometheus_queries=[query], **kwargs)
        self.query = query
        self.threshold = threshold

    def __call__(self, ctx):
        target = ctx.target_num_replicas
        metrics = self.prometheus_metrics
        if metrics is None or self.query not in metrics:
            return float(target), {"signal": "no_data"}
        value = metrics[self.query]
        if value > self.threshold:
            return float(target + 1), {"signal": "up", "value": value}
        floor = ctx.capacity_adjusted_min_replicas
        return float(max(floor, target - 1)), {"signal": "down", "value": value}


def test_prometheus_mixin_drives_autoscaling(serve_instance):
    _PROMETHEUS_VALUE["value"] = 5.0
    server = ThreadingHTTPServer(("0.0.0.0", 0), _MockPrometheusHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    address = f"http://{ray.util.get_node_ip_address()}:{server.server_address[1]}"

    try:

        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 2,
                "upscale_delay_s": 1,
                "downscale_delay_s": 1,
                "metrics_interval_s": 0.5,
                "look_back_period_s": 1,
                "policy": AutoscalingPolicy(
                    policy_function=_ThresholdPolicy,
                    policy_kwargs={
                        "prometheus_address": address,
                        "query": "signal",
                        "threshold": 1.0,
                        "fetch_interval_s": 0.5,
                        "cache_ttl_s": 10.0,
                    },
                ),
            },
        )
        class PrometheusScaled:
            def __call__(self):
                return "ok"

        serve.run(PrometheusScaled.bind())

        wait_for_condition(
            check_num_replicas_eq,
            name="PrometheusScaled",
            target=2,
            timeout=40,
            retry_interval_ms=1000,
        )

        _PROMETHEUS_VALUE["value"] = 0.0
        wait_for_condition(
            check_num_replicas_eq,
            name="PrometheusScaled",
            target=1,
            timeout=40,
            retry_interval_ms=1000,
        )
    finally:
        serve.delete("default")
        server.shutdown()
        server.server_close()
        server_thread.join()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
