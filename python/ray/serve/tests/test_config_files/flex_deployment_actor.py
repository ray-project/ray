"""Flexible driver that works with or without deployment actors.

Used by declarative redeployment tests that need to transition from
no-deployment-actors to having them.
"""

import ray
from ray import serve


@serve.deployment(ray_actor_options={"num_cpus": 0.1})
class FlexDriver:
    def __call__(self):
        try:
            actor = serve.get_deployment_actor("counter")
            return str(ray.get(actor.get.remote()))
        except Exception:
            return "no_actor"


app = FlexDriver.bind()
