# __serve_example_begin__
import time
from typing import Dict

from ray import serve


@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "metrics_interval_s": 0.1,
        "policy": {
            "policy_function": "autoscaling_policy:custom_metrics_autoscaling_policy"
        },
    },
    max_ongoing_requests=5,
)
class CustomMetricsDeployment:
    def __init__(self):
        self.cpu_usage = 50.0
        self.memory_usage = 60.0

    def __call__(self) -> str:
        time.sleep(0.5)
        self.cpu_usage = min(100, self.cpu_usage + 5)
        self.memory_usage = min(100, self.memory_usage + 3)
        return "Hello, world!"

    def record_autoscaling_stats(self) -> Dict[str, float]:
        self.cpu_usage = max(20, self.cpu_usage - 2)
        self.memory_usage = max(30, self.memory_usage - 1)
        return {
            "cpu_usage": self.cpu_usage,
            "memory_usage": self.memory_usage,
        }


# Create the app
app = CustomMetricsDeployment.bind()
# __serve_example_end__

if __name__ == "__main__":
    import requests  # noqa

    serve.run(app)
    for _ in range(10):
        resp = requests.get("http://localhost:8000/")
        assert resp.text == "Hello, world!"
