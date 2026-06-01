from enum import Enum
from typing import Callable, Dict, Tuple, Union

import ray
from ray._common.usage.usage_lib import record_extra_usage_tag
from ray.llm._internal.batch.observability.logging import get_logger
from ray.llm._internal.common.base_pydantic import BaseModelExtended

LLM_BATCH_TELEMETRY_NAMESPACE = "llm_batch_telemetry"
LLM_BATCH_TELEMETRY_ACTOR_NAME = "llm_batch_telemetry"

logger = get_logger(__name__)


class BatchModelTelemetry(BaseModelExtended):
    processor_config_name: str = ""
    model_architecture: str = ""
    batch_size: int = 0
    accelerator_type: str = ""
    concurrency: Union[int, Tuple[int, int]] = 0
    task_type: str = ""

    # For the parallel size, 0 means not supported.
    pipeline_parallel_size: int = 0
    tensor_parallel_size: int = 0
    data_parallel_size: int = 0


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
    LLM_BATCH_DATA_PARALLEL_SIZE = "LLM_BATCH_DATA_PARALLEL_SIZE"


@ray.remote(
    name=LLM_BATCH_TELEMETRY_ACTOR_NAME,
    namespace=LLM_BATCH_TELEMETRY_NAMESPACE,
    num_cpus=0,
    lifetime="detached",
)
class _TelemetryAgent:
    """Named Actor to keep the state of all deployed models and record telemetry."""

    def __init__(self):
        # Keyed by telemetry identity so repeated identical processor builds
        # overwrite rather than accumulate.
        self._tracking_telemetries: Dict[str, BatchModelTelemetry] = {}
        self._record_tag_func = record_extra_usage_tag

    def _update_record_tag_func(self, record_tag_func: Callable) -> None:
        self._record_tag_func = record_tag_func

    def _reset(self) -> None:
        """Only used in tests to clear accumulated telemetries."""
        self._tracking_telemetries = {}

    def generate_report(self) -> Dict[str, str]:
        return {
            BatchTelemetryTags.LLM_BATCH_PROCESSOR_CONFIG_NAME: ",".join(
                [t.processor_config_name for t in self._tracking_telemetries.values()]
            ),
            BatchTelemetryTags.LLM_BATCH_MODEL_ARCHITECTURE: ",".join(
                [t.model_architecture for t in self._tracking_telemetries.values()]
            ),
            BatchTelemetryTags.LLM_BATCH_SIZE: ",".join(
                [str(t.batch_size) for t in self._tracking_telemetries.values()]
            ),
            BatchTelemetryTags.LLM_BATCH_ACCELERATOR_TYPE: ",".join(
                [t.accelerator_type for t in self._tracking_telemetries.values()]
            ),
            BatchTelemetryTags.LLM_BATCH_CONCURRENCY: ",".join(
                [str(t.concurrency) for t in self._tracking_telemetries.values()]
            ),
            BatchTelemetryTags.LLM_BATCH_TASK_TYPE: ",".join(
                [t.task_type for t in self._tracking_telemetries.values()]
            ),
            BatchTelemetryTags.LLM_BATCH_PIPELINE_PARALLEL_SIZE: ",".join(
                [
                    str(t.pipeline_parallel_size)
                    for t in self._tracking_telemetries.values()
                ]
            ),
            BatchTelemetryTags.LLM_BATCH_TENSOR_PARALLEL_SIZE: ",".join(
                [
                    str(t.tensor_parallel_size)
                    for t in self._tracking_telemetries.values()
                ]
            ),
            BatchTelemetryTags.LLM_BATCH_DATA_PARALLEL_SIZE: ",".join(
                [str(t.data_parallel_size) for t in self._tracking_telemetries.values()]
            ),
        }

    def record(self, telemetry: BatchModelTelemetry) -> None:
        """Upsert by identity and record telemetries."""
        from ray._common.usage.usage_lib import TagKey

        self._tracking_telemetries[telemetry.model_dump_json()] = telemetry
        for key, value in self.generate_report().items():
            try:
                self._record_tag_func(TagKey.Value(key), value)
            except ValueError:
                # Tag not in the installed usage proto; skip rather than fail.
                continue


class TelemetryAgent:
    """Wrapper around the telemetry agent that calls the remote method until
    push_telemetry_report is called."""

    def __init__(self):
        from ray._common.constants import HEAD_NODE_RESOURCE_NAME
        from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

        # get_if_exists makes creation atomic across concurrent drivers.
        self.remote_telemetry_agent = _TelemetryAgent.options(
            name=LLM_BATCH_TELEMETRY_ACTOR_NAME,
            namespace=LLM_BATCH_TELEMETRY_NAMESPACE,
            get_if_exists=True,
            # Ensure the actor is created on the head node.
            resources={HEAD_NODE_RESOURCE_NAME: 0.001},
            # Ensure the actor is not scheduled with the existing placement group.
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None),
        ).remote()

    def _update_record_tag_func(self, record_tag_func: Callable):
        self.remote_telemetry_agent._update_record_tag_func.remote(record_tag_func)

    def push_telemetry_report(self, telemetry: BatchModelTelemetry):
        # Telemetry must never break processor construction.
        try:
            ray.get(self.remote_telemetry_agent.record.remote(telemetry))
        except Exception:
            logger.exception("Failed to push LLM batch telemetry")


def get_or_create_telemetry_agent() -> TelemetryAgent:
    """Helper to get or create the telemetry agent."""
    return TelemetryAgent()
