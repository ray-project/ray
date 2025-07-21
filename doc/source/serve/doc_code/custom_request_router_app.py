# flake8: noqa

# __begin_deploy_app_with_uniform_request_router__
from ray import serve
from ray.serve.request_router import ReplicaID
import time
from collections import defaultdict
from ray.serve.context import _get_internal_replica_context
from typing import Any, Dict
from ray.serve.config import RequestRouterConfig


@serve.deployment(
    request_router_config=RequestRouterConfig(
        request_router_class="custom_request_router:UniformRequestRouter",
    ),
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


# __begin_deploy_app_with_throughput_aware_request_router__
def _time_ms() -> int:
    return int(time.time() * 1000)


@serve.deployment(
    request_router_config=RequestRouterConfig(
        request_router_class="custom_request_router:ThroughputAwareRequestRouter",
        request_routing_stats_period_s=1,
        request_routing_stats_timeout_s=1,
    ),
    num_replicas=3,
    ray_actor_options={"num_cpus": 0},
)
class ThroughputAwareRequestRouterApp:
    def __init__(self):
        self.throughput_buckets: Dict[int, int] = defaultdict(int)
        self.last_throughput_buckets = _time_ms()
        context = _get_internal_replica_context()
        self.replica_id: ReplicaID = context.replica_id

    def __call__(self):
        self.update_throughput()
        return self.replica_id

    def update_throughput(self):
        current_timestamp_ms = _time_ms()

        # Under high concurrency, requests can come in at different times. This
        # early return helps to skip if the current_timestamp_ms is more than a
        # second older than the last throughput bucket.
        if current_timestamp_ms < self.last_throughput_buckets - 1000:
            return

        # Record the request to the bucket
        self.throughput_buckets[current_timestamp_ms] += 1
        self.last_throughput_buckets = current_timestamp_ms

    def record_routing_stats(self) -> Dict[str, Any]:
        current_timestamp_ms = _time_ms()
        throughput = 0

        for t, c in list(self.throughput_buckets.items()):
            if t < current_timestamp_ms - 1000:
                # Remove the bucket if it is older than 1 second
                self.throughput_buckets.pop(t)
            else:
                throughput += c

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
