# __serve_example_begin__
import asyncio
import time
from datetime import datetime
from typing import Dict, Any

from ray import serve
from ray.serve._private.autoscaling_state import AutoscalingContext
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy


def scheduled_batch_processing_policy(ctx: AutoscalingContext) -> tuple[int, Dict[str, Any]]:
    current_time = datetime.now()
    current_hour = current_time.hour
    # Scale up during business hours (9 AM - 5 PM)
    if 9 <= current_hour < 17:
        return 8, {"reason": "Business hours"}
    # Scale up for evening batch processing (6 PM - 8 PM)
    elif 18 <= current_hour < 20:
        return 10, {"reason": "Evening batch processing"}
    # Minimal scaling during off-peak hours
    else:
        return 1, {"reason": "Off-peak hours"}


@serve.deployment(
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=12,
        policy=AutoscalingPolicy(name=scheduled_batch_processing_policy),
    ),
    max_ongoing_requests=3,
)
class BatchProcessingDeployment:
    async def __call__(self) -> str:
        """Process batch data with simulated work."""
        # Simulate batch processing work
        await asyncio.sleep(0.5)
        return "Hello, world!"


# Create the app
app = BatchProcessingDeployment.bind()
# __serve_example_end__
