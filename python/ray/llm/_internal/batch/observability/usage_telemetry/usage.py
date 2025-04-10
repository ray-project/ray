from enum import Enum
from typing import Callable, Dict, List
import ray

from ray._private.usage.usage_lib import record_extra_usage_tag
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.batch.observability.logging import get_logger

LLM_BATCH_TELEMETRY_NAMESPACE = "llm_batch_telemetry"
LLM_BATCH_TELEMETRY_ACTOR_NAME = "llm_batch_telemetry"

logger = get_logger(__name__)


class BatchModelTelemetry(BaseModelExtended):
    processor_config_name: str = ""
    model_architecture: str = ""
    batch_size: int = 0
    accelerator_type: str = ""
    concurrency: int = 0
    task_type: str = ""
    pipeline_parallel_size: int = 0
    tensor_parallel_size: int = 0


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
        self._tracking_telemetries: List[BatchModelTelemetry] = []
        self._record_tag_func = record_extra_usage_tag

    def _update_record_tag_func(self, record_tag_func: Callable) -> None:
        self._record_tag_func = record_tag_func

    def generate_report(self) -> Dict[str, str]:
        return {
            BatchTelemetryTags.LLM_BATCH_PROCESSOR_CONFIG_NAME: ",".join(
                [t.processor_config_name for t in self._tracking_telemetries]
            ),
            BatchTelemetryTags.LLM_BATCH_MODEL_ARCHITECTURE: ",".join(
                [t.model_architecture for t in self._tracking_telemetries]
            ),
            BatchTelemetryTags.LLM_BATCH_SIZE: ",".join(
                [str(t.batch_size) for t in self._tracking_telemetries]
            ),
            BatchTelemetryTags.LLM_BATCH_ACCELERATOR_TYPE: ",".join(
                [t.accelerator_type for t in self._tracking_telemetries]
            ),
            BatchTelemetryTags.LLM_BATCH_CONCURRENCY: ",".join(
                [str(t.concurrency) for t in self._tracking_telemetries]
            ),
            BatchTelemetryTags.LLM_BATCH_TASK_TYPE: ",".join(
                [t.task_type for t in self._tracking_telemetries]
            ),
            BatchTelemetryTags.LLM_BATCH_PIPELINE_PARALLEL_SIZE: ",".join(
                [str(t.pipeline_parallel_size) for t in self._tracking_telemetries]
            ),
            BatchTelemetryTags.LLM_BATCH_TENSOR_PARALLEL_SIZE: ",".join(
                [str(t.tensor_parallel_size) for t in self._tracking_telemetries]
            ),
        }

    def record(self, telemetry: BatchModelTelemetry) -> None:
        """Append and record telemetries."""
        from ray._private.usage.usage_lib import TagKey

        self._tracking_telemetries.append(telemetry)
        telemetry_dict = self.generate_report()
        for key, value in telemetry_dict.items():
            self._record_tag_func(TagKey.Value(key), value)


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

    def push_telemetry_report(self, telemetry: BatchModelTelemetry):
        ray.get(self.remote_telemetry_agent.record.remote(telemetry))


def get_or_create_telemetry_agent() -> TelemetryAgent:
    """Helper to get or create the telemetry agent."""
    return TelemetryAgent()
