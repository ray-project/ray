# __serve_example_begin__
import asyncio

from ray import serve
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy


@serve.deployment(
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=12,
        policy=AutoscalingPolicy(
            policy_function="autoscaling_policy:scheduled_batch_processing_policy"
        ),
        metrics_interval_s=0.1,
    ),
    max_ongoing_requests=3,
)
class BatchProcessingDeployment:
    async def __call__(self) -> str:
        # Simulate batch processing work
        await asyncio.sleep(0.5)
        return "Hello, world!"


app = BatchProcessingDeployment.bind()
# __serve_example_end__

if __name__ == "__main__":
    import requests  # noqa

    serve.run(app)
    resp = requests.get("http://localhost:8000/")
    assert resp.text == "Hello, world!"
