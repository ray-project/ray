"""Integration test: a PrometheusQueryMixin policy drives real autoscaling.

Runs a real deployment through the controller's autoscaling loop, with the
policy's mixin actually HTTP-fetching a mock Prometheus query endpoint. Only
Prometheus's query evaluation is stubbed; the fetch, cache, policy decision,
and controller scaling are all real.
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

# Mutable value the mock Prometheus serves; the test flips it to drive scaling.
_PROM_VALUE = {"v": 5.0}


class _MockPrometheusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payload = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"value": [0, str(_PROM_VALUE["v"])]}],
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


class _PromStepPolicy(PrometheusQueryMixin):
    """Step off target: +1 while the metric exceeds threshold, else -1."""

    def __init__(self, query="q", threshold=1.0, **kwargs):
        super().__init__(prometheus_queries=[query], **kwargs)
        self.query = query
        self.threshold = threshold

    def __call__(self, ctx):
        target = ctx.target_num_replicas
        metrics = self.prometheus_metrics
        if metrics is None:
            return float(target), {"signal": "no_metrics"}
        value = metrics.get(self.query)
        if value is None:
            return float(target), {"signal": "no_data"}
        if value > self.threshold:
            return float(target + 1), {"signal": "up", "value": value}
        floor = ctx.capacity_adjusted_min_replicas
        return float(max(floor, target - 1)), {"signal": "down", "value": value}


def test_prometheus_mixin_drives_autoscaling(serve_instance):
    _PROM_VALUE["v"] = 5.0
    server = ThreadingHTTPServer(("0.0.0.0", 0), _MockPrometheusHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
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
                    policy_function=_PromStepPolicy,
                    policy_kwargs=dict(
                        prometheus_address=address,
                        query="q",
                        threshold=1.0,
                        fetch_interval_s=0.5,
                        cache_ttl_s=10.0,
                    ),
                ),
            },
        )
        class PromScaled:
            def __call__(self):
                return "ok"

        serve.run(PromScaled.bind())

        # Metric above threshold -> scale up to max.
        wait_for_condition(
            check_num_replicas_eq,
            name="PromScaled",
            target=2,
            timeout=40,
            retry_interval_ms=1000,
        )

        # Metric drops -> scale back down to min.
        _PROM_VALUE["v"] = 0.0
        wait_for_condition(
            check_num_replicas_eq,
            name="PromScaled",
            target=1,
            timeout=40,
            retry_interval_ms=1000,
        )
    finally:
        server.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
