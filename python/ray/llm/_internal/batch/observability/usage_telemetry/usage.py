from enum import Enum
from typing import Callable, Dict
from collections import defaultdict
import ray

from ray._private.usage.usage_lib import record_extra_usage_tag

from ray.llm._internal.batch.observability.logging import get_logger

LLM_BATCH_TELEMETRY_NAMESPACE = "llm_batch_telemetry"
LLM_BATCH_TELEMETRY_ACTOR_NAME = "llm_batch_telemetry"

logger = get_logger(__name__)


class BatchTelemetryTags(str, Enum):
    """Telemetry tags for RayLLM Batch."""

    LLM_BATCH_PROCESSOR_CONFIG_NAME = "LLM_BATCH_PROCESSOR_CONFIG_NAME"
    LLM_BATCH_MODEL_ARCHITECTURE = "LLM_BATCH_MODEL_ARCHITECTURE"
    LLM_BATCH_SIZE = "LLM_BATCH_SIZE"
    LLM_BATCH_ACCELERATOR_TYPE = "LLM_BATCH_ACCELERATOR_TYPE"
    LLM_BATCH_CONCURRENCY = "LLM_BATCH_CONCURRENCY"
    LLM_BATCH_TASK_TYPE = "LLM_BATCH_TASK_TYPE"
    LLM_BATCH_PIPELINE_PARALLEL_SIZE = "LLM_BATCH_PIPELINE_PARALLEL_SIZE"
    LLM_BATCH_TENSOR_PARALLEL_SIZE = "LLM_BATCH_TENSOR_PARALLEL_SIZE"


@ray.remote(
    name=LLM_BATCH_TELEMETRY_ACTOR_NAME,
    namespace=LLM_BATCH_TELEMETRY_NAMESPACE,
    num_cpus=0,
    lifetime="detached",
)
class _TelemetryAgent:
    """Named Actor to keep the state of all deployed models and record telemetry."""

    def __init__(self):
        self._tracking_telemetries = defaultdict(list)
        self._record_tag_func = record_extra_usage_tag

    def _update_record_tag_func(self, record_tag_func: Callable) -> None:
        self._record_tag_func = record_tag_func

    def record(self, telemetries: Dict[str, str]) -> None:
        """Append and record telemetries."""
        from ray._private.usage.usage_lib import TagKey

        for key in BatchTelemetryTags:
            value = telemetries.get(key, "")
            self._tracking_telemetries[TagKey.Value(key)].append(value)
        for key, values in self._tracking_telemetries.items():
            self._record_tag_func(key, ",".join(values))


class TelemetryAgent:
    """Wrapper around the telemetry agent that calls the remote method until
    push_telemetry_report is called."""

    def __init__(self):
        try:
            self.remote_telemetry_agent = ray.get_actor(
                LLM_BATCH_TELEMETRY_ACTOR_NAME, namespace=LLM_BATCH_TELEMETRY_NAMESPACE
            )
        except ValueError:
            from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME
            from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

            self.remote_telemetry_agent = _TelemetryAgent.options(
                # Ensure the actor is created on the head node.
                resources={HEAD_NODE_RESOURCE_NAME: 0.001},
                # Ensure the actor is not scheduled with the existing placement group.
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=None
                ),
            ).remote()

    def _update_record_tag_func(self, record_tag_func: Callable):
        self.remote_telemetry_agent._update_record_tag_func.remote(record_tag_func)

    def push_telemetry_report(self, telemetries: Dict[BatchTelemetryTags, str]):
        for key in telemetries.keys():
            if key not in BatchTelemetryTags:
                logger.warning("Invalid telemetry key: %s", key)

        ray.get(self.remote_telemetry_agent.record.remote(telemetries))


def get_or_create_telemetry_agent() -> TelemetryAgent:
    """Helper to get or create the telemetry agent."""
    return TelemetryAgent()
