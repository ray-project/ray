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
from typing import Any, Dict
from ray.serve.config import AutoscalingContext


def custom_metrics_autoscaling_policy(
    ctx: AutoscalingContext,
) -> tuple[int, Dict[str, Any]]:
    cpu_usage_metric = ctx.aggregated_metrics.get("cpu_usage", {})
    memory_usage_metric = ctx.aggregated_metrics.get("memory_usage", {})
    max_cpu_usage = list(cpu_usage_metric.values())[-1] if cpu_usage_metric else 0
    max_memory_usage = (
        list(memory_usage_metric.values())[-1] if memory_usage_metric else 0
    )

    if max_cpu_usage > 80 or max_memory_usage > 85:
        return min(ctx.capacity_adjusted_max_replicas, ctx.current_num_replicas + 1), {}
    elif max_cpu_usage < 30 and max_memory_usage < 40:
        return max(ctx.capacity_adjusted_min_replicas, ctx.current_num_replicas - 1), {}
    else:
        return ctx.current_num_replicas, {}


# __end_custom_metrics_autoscaling_policy__


# __begin_application_level_autoscaling_policy__
from typing import Dict, Tuple
from ray.serve.config import AutoscalingContext

from ray.serve._private.common import DeploymentID
from ray.serve.config import AutoscalingContext


def coordinated_scaling_policy(
    contexts: Dict[DeploymentID, AutoscalingContext]
) -> Tuple[Dict[DeploymentID, int], Dict]:
    """Scale deployments based on coordinated load balancing."""
    decisions = {}

    # Example: Scale a preprocessing deployment
    preprocessing_id = [d for d in contexts if d.name == "Preprocessor"][0]
    preprocessing_ctx = contexts[preprocessing_id]

    # Scale based on queue depth
    preprocessing_replicas = max(
        preprocessing_ctx.capacity_adjusted_min_replicas,
        min(
            preprocessing_ctx.capacity_adjusted_max_replicas,
            preprocessing_ctx.total_num_requests // 10,
        ),
    )
    decisions[preprocessing_id] = preprocessing_replicas

    # Example: Scale a model deployment proportionally
    model_id = [d for d in contexts if d.name == "Model"][0]
    model_ctx = contexts[model_id]

    # Scale model to handle preprocessing output
    # Assuming model takes 2x longer than preprocessing
    model_replicas = max(
        model_ctx.capacity_adjusted_min_replicas,
        min(model_ctx.capacity_adjusted_max_replicas, preprocessing_replicas * 2),
    )
    decisions[model_id] = model_replicas

    return decisions, {}


# __end_application_level_autoscaling_policy__
