"""Serve fixture with optional startup and health check failures.

Set the builder argument fail or the environment variable FAIL_ON_INIT=1
to fail the constructor. FAIL_HEALTH_CHECK=1 lets the replica start, then
fails subsequent health checks. A fixed max_constructor_retry_count keeps
the failure threshold independent of the replica count.
"""
import os

from ray import serve


@serve.deployment(num_replicas=2, max_ongoing_requests=7, max_constructor_retry_count=3)
class FailOnFlag:
    def __init__(self, fail: bool):
        if fail or os.environ.get("FAIL_ON_INIT") == "1":
            raise RuntimeError("constructor failure requested by the test")
        self._health_checks = 0

    def check_health(self):
        self._health_checks += 1
        if self._health_checks > 1 and os.environ.get("FAIL_HEALTH_CHECK") == "1":
            raise RuntimeError("health check failure requested by the test")

    def __call__(self, *args):
        return "ok"


def build(args):
    return FailOnFlag.bind(args.get("fail", False))
