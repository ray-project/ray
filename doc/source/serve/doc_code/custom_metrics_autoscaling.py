# __serve_example_begin__
import time
from typing import Dict

import psutil
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
        self.process = psutil.Process()

    def __call__(self) -> str:
        # Simulate some work
        time.sleep(0.5)
        return "Hello, world!"

    def record_autoscaling_stats(self) -> Dict[str, float]:
        # Get CPU usage as a percentage
        cpu_usage = self.process.cpu_percent(interval=0.1)

        # Get memory usage as a percentage of system memory
        memory_info = self.process.memory_full_info()
        system_memory = psutil.virtual_memory().total
        memory_usage = (memory_info.uss / system_memory) * 100

        return {
            "cpu_usage": cpu_usage,
            "memory_usage": memory_usage,
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
