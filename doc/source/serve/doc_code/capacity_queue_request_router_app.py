# flake8: noqa

# __begin_deploy_app_with_capacity_queue_router__
import ray
from ray import serve
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig
from ray.serve.context import _get_internal_replica_context

from ray.serve.experimental.capacity_queue import (
    CapacityQueue,
)


@serve.deployment(
    deployment_actors=[
        DeploymentActorConfig(
            name="capacity_queue",
            actor_class=CapacityQueue,
            init_kwargs={
                "acquire_timeout_s": 30.0,
                # The queue subscribes to controller updates for this deployment
                # so it automatically registers/unregisters replicas.
                "deployment_id_name": "CapacityQueueApp",
                "deployment_id_app": "default",
            },
            actor_options={"num_cpus": 0},
        ),
    ],
    request_router_config=RequestRouterConfig(
        request_router_class=(
            "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
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

    async def __call__(self):
        return self.replica_id


handle = serve.run(CapacityQueueApp.bind())
response = handle.remote().result()
print(f"Response from CapacityQueueApp: {response}")
# __end_deploy_app_with_capacity_queue_router__
