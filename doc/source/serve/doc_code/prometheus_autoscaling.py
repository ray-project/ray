# __serve_example_begin__
from ray import serve
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy


@serve.deployment(
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=10,
        upscale_delay_s=3,
        downscale_delay_s=60,
        policy=AutoscalingPolicy(
            policy_function=(
                "prometheus_autoscaling_policy:QueueDepthAutoscalingPolicy"
            ),
            policy_kwargs={
                # Any PromQL that resolves to a single scalar. Scope it to this
                # application so unrelated load doesn't move these replicas.
                "query": 'sum(my_app_pending_requests{app="my_app"})',
                "scale_up_threshold": 10.0,
            },
        ),
    ),
    max_ongoing_requests=100,
)
class MyDeployment:
    async def __call__(self) -> str:
        return "Hello, world!"


app = MyDeployment.bind()
# __serve_example_end__

if __name__ == "__main__":
    serve.run(app)
