import ray
from ray._private.usage.usage_lib import TagKey
import pytest
import sys

from ray.llm._internal.batch.observability.usage_telemetry.usage import (
    get_or_create_telemetry_agent,
)
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.vllm_engine_proc import (
    vLLMEngineProcessorConfig,
)


@ray.remote(num_cpus=0)
class FakeTelemetryRecorder:
    def __init__(self):
        self._telemetry = {}

    def record(self, key, value):
        self._telemetry[key] = value

    def telemetry(self):
        return self._telemetry


def test_push_telemetry_report():
    recorder = FakeTelemetryRecorder.remote()

    def record_tag_func(key, value):
        recorder.record.remote(key, value)

    telemetry_agent = get_or_create_telemetry_agent()
    telemetry_agent.update_record_tag_func(record_tag_func)

    config = vLLMEngineProcessorConfig(
        model_source="model_opt_125m",
        engine_kwargs=dict(
            max_model_len=8192,
        ),
        runtime_env=dict(
            env_vars=dict(
                RANDOM_ENV_VAR="12345",
            ),
        ),
        accelerator_type="A10G",
        concurrency=4,
        batch_size=64,
        max_pending_requests=111,
        apply_chat_template=True,
        tokenize=True,
        detokenize=True,
        has_image=True,
    )
    _ = ProcessorBuilder.build(config)

    # Ensure that the telemetry is correct after pushing the reports.
    telemetry = ray.get(recorder.telemetry.remote())
    try:
        assert telemetry == {
            TagKey.LLM_BATCH_PROCESSOR_CONFIG_NAME: "vLLMEngineProcessorConfig",
            TagKey.LLM_BATCH_MODEL_ARCHITECTURE: "model_opt_125m",
            TagKey.LLM_BATCH_SIZE: "64",
            TagKey.LLM_BATCH_ACCELERATOR_TYPE: "A10G",
            TagKey.LLM_BATCH_CONCURRENCY: "4",
            TagKey.LLM_BATCH_TASK_TYPE: "chat",
            TagKey.LLM_BATCH_PIPELINE_PARALLEL_SIZE: "1",
            TagKey.LLM_BATCH_TENSOR_PARALLEL_SIZE: "1",
        }
    except AttributeError:
        # If the key doesn't exist in the TagKey, no telemetry should be logged.
        assert telemetry == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
