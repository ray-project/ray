# __serve_example_begin__
import json
import tempfile

from ray import serve
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy

# Create a JSON file with the initial target replica count.
# In production this file would be written by an external system.
scaling_file = tempfile.NamedTemporaryFile(
    mode="w", suffix=".json", delete=False
)
json.dump({"replicas": 2}, scaling_file)
scaling_file.close()


@serve.deployment(
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=10,
        upscale_delay_s=3,
        downscale_delay_s=10,
        policy=AutoscalingPolicy(
            policy_function="class_based_autoscaling_policy:FileBasedAutoscalingPolicy",
            policy_kwargs={
                "file_path": scaling_file.name,
                "poll_interval_s": 2.0,
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
    import requests  # noqa

    serve.run(app)
    resp = requests.get("http://localhost:8000/")
    assert resp.text == "Hello, world!"
