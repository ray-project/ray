# __begin_scheduled_batch_processing_policy__
from datetime import datetime
from typing import Any, Dict
from ray.serve.config import AutoscalingContext


def scheduled_batch_processing_policy(
    ctx: AutoscalingContext,
) -> tuple[int, Dict[str, Any]]:
    current_time = datetime.now()
    current_hour = current_time.hour
    # Scale up during business hours (9 AM - 5 PM)
    if 9 <= current_hour < 17:
        return 2, {"reason": "Business hours"}
    # Scale up for evening batch processing (6 PM - 8 PM)
    elif 18 <= current_hour < 20:
        return 4, {"reason": "Evening batch processing"}
    # Minimal scaling during off-peak hours
    else:
        return 1, {"reason": "Off-peak hours"}


# __end_scheduled_batch_processing_policy__


# __begin_custom_metrics_autoscaling_policy__
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


# __end_custom_metrics_autoscaling_policy__
