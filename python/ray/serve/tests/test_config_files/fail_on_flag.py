"""Serve fixture with optional startup and health check failures.

Set the builder argument fail or the environment variable FAIL_ON_INIT=1
to fail the constructor. FAIL_HEALTH_CHECK=1 lets the replica start, then
fails subsequent health checks. BLOCK_ON_SIGNAL holds requests until the actor
provided by serve_instance_with_signal is released. RECORD_FAILED_GANGS names
an accumulator actor that records constructor failures by gang. A fixed
max_constructor_retry_count keeps the failure threshold independent of the
replica count.
"""
import os

import ray
from ray import serve
from ray.serve._private.test_utils import SERVE_INSTANCE_SIGNAL_ACTOR_NAME


@serve.deployment(max_constructor_retry_count=3)
class FailOnFlag:
    def __init__(self, fail: bool):
        if fail or os.environ.get("FAIL_ON_INIT") == "1":
            if store_name := os.environ.get("RECORD_FAILED_GANGS"):
                context = serve.context._get_internal_replica_context()
                ray.get(
                    ray.get_actor(store_name).add.remote(context.gang_context.gang_id)
                )
            raise RuntimeError("constructor failure requested by the test")
        self._health_checks = 0

    def check_health(self):
        self._health_checks += 1
        if self._health_checks > 1 and os.environ.get("FAIL_HEALTH_CHECK") == "1":
            raise RuntimeError("health check failure requested by the test")

    async def __call__(self, *args):
        if os.environ.get("BLOCK_ON_SIGNAL") == "1":
            await ray.get_actor(SERVE_INSTANCE_SIGNAL_ACTOR_NAME).wait.remote()
        return "ok"


def build(args):
    return FailOnFlag.bind(args.get("fail", False))
