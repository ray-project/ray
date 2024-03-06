# flake8: noqa

import ray

# __deployment_start__
# File name: configure_serve.py

from ray import serve


@serve.deployment(
    name="Translator",
    num_replicas=2,
    ray_actor_options={"num_cpus": 0.2, "num_gpus": 0},
    max_ongoing_requests=100,
    health_check_period_s=10,
    health_check_timeout_s=30,
    graceful_shutdown_timeout_s=20,
    graceful_shutdown_wait_loop_s=2,
)
class Example:
    ...


example_app = Example.bind()
# __deployment_end__

example_app = Example.options(
    ray_actor_options={"num_cpus": 0.2, "num_gpus": 0.0}
).bind()

# __options_end__
serve.run(example_app)

serve.shutdown()
ray.shutdown()
