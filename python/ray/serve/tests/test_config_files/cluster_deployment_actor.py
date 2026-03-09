"""Minimal app for multi-node cluster test: deployment actor requires 2 CPUs.

Used by test_deployment_actor_blocks_until_node_available to verify that
replica scheduling blocks until a worker node with sufficient resources
comes up.
"""

import ray
from ray import serve
from ray.serve.config import DeploymentActorConfig


@serve.deployment(
    deployment_actors=[
        DeploymentActorConfig(
            name="counter",
            actor_class="ray.serve.tests.test_deployment_actors:SharedCounter",
            init_kwargs={"start": 0},
            actor_options={"num_cpus": 2},
        ),
    ],
)
class ClusterDeploymentActorDriver:
    def __call__(self):
        counter = serve.get_deployment_actor("counter")
        return str(ray.get(counter.get.remote()))


app = ClusterDeploymentActorDriver.bind()
