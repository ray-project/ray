# flake8: noqa
# import asyncio
# import ray
# from ray import serve
#
#
# ray.init()
# serve.start()
#
# loop = asyncio.get_event_loop()
# print(f"is loop running? {loop.is_running()=}")


# __begin_deploy_app_with_uniform_request_router__
from ray import serve
from ray.serve.context import _get_internal_replica_context
from ray.serve.request_router import ReplicaID


@serve.deployment(
    request_router_class="custom_request_router:UniformRequestRouter",
    num_replicas=10,
    ray_actor_options={"num_cpus": 0},
)
class UniformRequestRouterApp:
    def __init__(self):
        context = _get_internal_replica_context()
        self.replica_id: ReplicaID = context.replica_id

    async def __call__(self):
        return self.replica_id


handle = serve.run(UniformRequestRouterApp.bind())
response = handle.remote().result()
print(f"Response from UniformRequestRouterApp: {response}")
# Example output:
#   Response from UniformRequestRouterApp:
#   Replica(id='67vc4ts5', deployment='UniformRequestRouterApp', app='default')
# __end_deploy_app_with_uniform_request_router__


# serve.shutdown()


# __begin_deploy_app_with_throughput_aware_request_router__
import time
from collections import defaultdict
from ray import serve
from ray.serve.context import _get_internal_replica_context
from typing import Any, Dict


def _time_ms() -> int:
    return int(time.time() * 1000)


@serve.deployment(
    request_router_class="custom_request_router:ThroughputAwareRequestRouter",
    num_replicas=3,
    request_routing_stats_period_s=1,
    request_routing_stats_timeout_s=1,
    ray_actor_options={"num_cpus": 0},
)
class ThroughputAwareRequestRouterApp:
    def __init__(self):
        self.throughput_buckets: Dict[int, int] = defaultdict(int)
        self.last_throughput_buckets = _time_ms()
        context = _get_internal_replica_context()
        self.replica_id: ReplicaID = context.replica_id

    def __call__(self, reset: bool = False):
        self.update_throughput()
        return self.replica_id

    def update_throughput(self):
        current_timestamp_ms = _time_ms()

        # Skip if the last throughput bucket is not expired
        if current_timestamp_ms < self.last_throughput_buckets - 1000:
            return

        # Record the request to the bucket
        self.throughput_buckets[current_timestamp_ms] += 1
        self.last_throughput_buckets = current_timestamp_ms

    def record_routing_stats(self) -> Dict[str, Any]:
        current_timestamp_ms = _time_ms()
        throughput = 0
        for t in range(current_timestamp_ms - 1000, current_timestamp_ms):
            throughput += self.throughput_buckets[t]

        return {
            "throughput": throughput,
        }


handle = serve.run(ThroughputAwareRequestRouterApp.bind())
response = handle.remote().result()
print(f"Response from ThroughputAwareRequestRouterApp: {response}")
# Example output:
#   Response from ThroughputAwareRequestRouterApp:
#   Replica(id='tkywafya', deployment='ThroughputAwareRequestRouterApp', app='default')
# __end_deploy_app_with_throughput_aware_request_router__
