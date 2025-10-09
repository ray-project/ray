# __serve_example_begin__
import time
from typing import Dict, Any

from ray import serve
from ray.serve._private.autoscaling_state import AutoscalingContext


def custom_metrics_autoscaling_policy(
    ctx: AutoscalingContext,
) -> tuple[int, Dict[str, Any]]:
    cpu_usage_metric = ctx.aggregated_metrics.get("cpu_usage", {})
    memory_usage_metric = ctx.aggregated_metrics.get("memory_usage", {})
    max_cpu_usage = max(cpu_usage_metric.values())
    max_memory_usage = max(memory_usage_metric.values())

    if max_cpu_usage > 80 or max_memory_usage > 85:
        return min(ctx.capacity_adjusted_max_replicas, ctx.current_num_replicas + 1), {}
    elif max_cpu_usage < 30 and max_memory_usage < 40:
        return max(ctx.capacity_adjusted_min_replicas, ctx.current_num_replicas - 1), {}
    else:
        return ctx.current_num_replicas, {}


@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "policy": {
            "name": "doc.source.serve.doc_code.custom_metrics_autoscaling:custom_metrics_autoscaling_policy"
        },
    },
    max_ongoing_requests=5,
)
class CustomMetricsDeployment:
    def __init__(self):
        self.cpu_usage = 50.0
        self.memory_usage = 60.0

    def __call__(self) -> str:
        time.sleep(0.1)
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
