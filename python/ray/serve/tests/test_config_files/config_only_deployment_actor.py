"""Deployment that uses deployment actors but defines them only via config override.

The deployment has no deployment_actors in the @serve.deployment decorator;
they are added purely through the declarative config override.
"""

import ray
from ray import serve


@serve.deployment(ray_actor_options={"num_cpus": 0.1})
class ConfigOnlyDriver:
    """Uses get_deployment_actor; deployment_actors come from config only."""

    def __call__(self):
        counter = serve.get_deployment_actor("counter")
        return str(ray.get(counter.get.remote()))


app = ConfigOnlyDriver.bind()
