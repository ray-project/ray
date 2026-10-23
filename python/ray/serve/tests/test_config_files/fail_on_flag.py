"""Serve fixture with optional startup and health check failures.

Set the builder argument fail or the environment variable FAIL_ON_INIT=1
to fail the constructor. FAIL_HEALTH_CHECK=1 lets the replica start, then
fails subsequent health checks. BLOCK_INIT_ON_SIGNAL=1 holds the constructor,
and BLOCK_ON_SIGNAL=1 holds requests, until the actor provided by
serve_instance_with_signal is released. RECORD_FAILED_GANGS names
an accumulator actor that records constructor failures by gang. A fixed
max_constructor_retry_count keeps the failure threshold independent of the
replica count. RECORD_APPLIED_CONFIGS names an accumulator actor that records
(PID, user_config) on every reconfigure(). ranks() returns initial and current
(rank, world_size) pairs. counter() returns the PID and the deployment actor's
counter value.
"""
import os
from typing import Any, Dict, Tuple

import ray
from ray import serve
from ray.serve._private.test_utils import SERVE_INSTANCE_SIGNAL_ACTOR_NAME

# The HAProxy shard drains for 0.01s, shorter than the reload that stops routing to a
# stopping replica, so it closes connections HAProxy still sends on. Traffic tests use 5s.
INGRESS_DRAIN_ENV = {"RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S": "5"}


def _rank_and_world_size() -> Tuple[int, int]:
    context = serve.get_replica_context()
    return context.rank.rank, context.world_size


@serve.deployment(num_replicas=2, max_ongoing_requests=7, max_constructor_retry_count=3)
class FailOnFlag:
    def __init__(self, fail: bool):
        if os.environ.get("BLOCK_INIT_ON_SIGNAL") == "1":
            ray.get(ray.get_actor(SERVE_INSTANCE_SIGNAL_ACTOR_NAME).wait.remote())
        if fail or os.environ.get("FAIL_ON_INIT") == "1":
            if store_name := os.environ.get("RECORD_FAILED_GANGS"):
                context = serve.context._get_internal_replica_context()
                ray.get(
                    ray.get_actor(store_name).add.remote(context.gang_context.gang_id)
                )
            raise RuntimeError("constructor failure requested by the test")
        self._health_checks = 0
        self._init_rank = _rank_and_world_size()

    def reconfigure(self, user_config: Any):
        if store_name := os.environ.get("RECORD_APPLIED_CONFIGS"):
            ray.get(ray.get_actor(store_name).add.remote((os.getpid(), user_config)))

    def ranks(self) -> Dict[str, Tuple[int, int]]:
        return {"init": self._init_rank, "current": _rank_and_world_size()}

    def counter(self) -> Tuple[int, int]:
        return os.getpid(), ray.get(serve.get_deployment_actor("counter").get.remote())

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


def build_draining(args):
    """build() with INGRESS_DRAIN_ENV code-defined, so dropping config overrides keeps it."""
    options = {"runtime_env": {"env_vars": INGRESS_DRAIN_ENV}}
    return FailOnFlag.options(ray_actor_options=options).bind(args.get("fail", False))
