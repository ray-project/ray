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
from ray.llm._internal.batch.processor.http_request_proc import (
    HttpRequestProcessorConfig,
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
    telemetry_agent._update_record_tag_func(record_tag_func)

    config = vLLMEngineProcessorConfig(
        model_source="facebook/opt-125m",
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

    _ = ProcessorBuilder.build(
        HttpRequestProcessorConfig(
            url="http://localhost:8000",
            headers={"Authorization": "Bearer 1234567890"},
            qps=2,
            concurrency=4,
            batch_size=64,
        )
    )

    # Ensure that the telemetry is correct after pushing the reports.
    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry == {
        TagKey.LLM_BATCH_PROCESSOR_CONFIG_NAME: "vLLMEngineProcessorConfig,HttpRequestProcessorConfig",
        TagKey.LLM_BATCH_MODEL_ARCHITECTURE: "OPTForCausalLM,",
        TagKey.LLM_BATCH_SIZE: "64,0",
        TagKey.LLM_BATCH_ACCELERATOR_TYPE: "A10G,",
        TagKey.LLM_BATCH_CONCURRENCY: "4,4",
        TagKey.LLM_BATCH_TASK_TYPE: "generate,",
        TagKey.LLM_BATCH_PIPELINE_PARALLEL_SIZE: "1,0",
        TagKey.LLM_BATCH_TENSOR_PARALLEL_SIZE: "1,0",
    }, f"actual telemetry: {telemetry}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
