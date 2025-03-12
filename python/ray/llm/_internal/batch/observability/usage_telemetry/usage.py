from enum import Enum
from typing import Callable, Dict, Optional

import ray

from ray._private.usage.usage_lib import record_extra_usage_tag

from ray.llm._internal.serve.observability.usage_telemetry.usage import (
    RAYLLM_TELEMETRY_NAMESPACE,
)
from ray.llm._internal.batch.observability.logging import get_logger

RAYLLM_BATCH_TELEMETRY_ACTOR_NAME = "rayllm_batch_telemetry"

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
    name=RAYLLM_BATCH_TELEMETRY_ACTOR_NAME,
    namespace=RAYLLM_TELEMETRY_NAMESPACE,
    num_cpus=0,
    lifetime="detached",
)
class _TelemetryAgent:
    """Named Actor to keep the state of all deployed models and record telemetry."""

    def __init__(self):
        self.record_tag_func = record_extra_usage_tag

    def update_record_tag_func(self, record_tag_func: Callable) -> None:
        self.record_tag_func = record_tag_func

    def record(self, telemetries: Dict[str, str]) -> None:
        """Record telemetry model."""
        from ray._private.usage.usage_lib import TagKey

        for key, value in telemetries.items():
            try:
                self.record_tag_func(TagKey.Value(key), value)
            except ValueError:
                # If the key doesn't exist in the TagKey enum, skip it.
                continue


class TelemetryAgent:
    """Wrapper around the telemetry agent that calls the remote method until
    push_telemetry_report is called."""

    def __init__(self):
        try:
            self.remote_telemetry_agent = ray.get_actor(
                RAYLLM_BATCH_TELEMETRY_ACTOR_NAME, namespace=RAYLLM_TELEMETRY_NAMESPACE
            )
        except ValueError:
            self.remote_telemetry_agent = _TelemetryAgent.remote()

    def update_record_tag_func(self, record_tag_func: Callable):
        self.remote_telemetry_agent.update_record_tag_func.remote(record_tag_func)

    def push_telemetry_report(self, telemetries: Dict[BatchTelemetryTags, str]):
        for key in telemetries.keys():
            if key not in BatchTelemetryTags:
                logger.warning(f"Invalid telemetry key: {key}.")

        ray.get(self.remote_telemetry_agent.record.remote(telemetries))


def get_or_create_telemetry_agent() -> TelemetryAgent:
    """Helper to get or create the telemetry agent."""
    return TelemetryAgent()
