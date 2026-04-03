# flake8: noqa

# __begin_deploy_app_with_capacity_queue_router__
import ray
from ray import serve
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig
from ray.serve.context import _get_internal_replica_context

from capacity_queue_request_router import CapacityQueue


@serve.deployment(
    deployment_actors=[
        DeploymentActorConfig(
            name="capacity_queue",
            actor_class=CapacityQueue,
            init_kwargs={"acquire_timeout_s": 30.0},
            actor_options={"num_cpus": 0},
        ),
    ],
    request_router_config=RequestRouterConfig(
        request_router_class=(
            "capacity_queue_request_router:CapacityQueueRouter"
        ),
    ),
    num_replicas=3,
    max_ongoing_requests=5,
    ray_actor_options={"num_cpus": 0},
)
class CapacityQueueApp:
    def __init__(self):
        context = _get_internal_replica_context()
        self.replica_id = context.replica_id

        # Register this replica with the capacity queue deployment actor
        queue = serve.get_deployment_actor("capacity_queue")
        ray.get(
            queue.register_replica.remote(
                self.replica_id.unique_id,
                context._deployment_config.max_ongoing_requests,
            )
        )

    async def __call__(self):
        return self.replica_id


handle = serve.run(CapacityQueueApp.bind())
response = handle.remote().result()
print(f"Response from CapacityQueueApp: {response}")
# __end_deploy_app_with_capacity_queue_router__
