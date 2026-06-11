import sys

import pytest

import ray
from ray._common.usage.usage_lib import TagKey
from ray.llm._internal.batch.observability.usage_telemetry.usage import (
    BatchModelTelemetry,
    get_or_create_telemetry_agent,
)
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.http_request_proc import (
    HttpRequestProcessorConfig,
)
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
    ray.get(telemetry_agent.remote_telemetry_agent._reset.remote())
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
        prepare_multimodal_stage=True,
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
        TagKey.LLM_BATCH_SIZE: "64,64",
        TagKey.LLM_BATCH_ACCELERATOR_TYPE: "A10G,",
        TagKey.LLM_BATCH_CONCURRENCY: "4,4",
        TagKey.LLM_BATCH_TASK_TYPE: "generate,",
        TagKey.LLM_BATCH_PIPELINE_PARALLEL_SIZE: "1,0",
        TagKey.LLM_BATCH_TENSOR_PARALLEL_SIZE: "1,0",
        TagKey.LLM_BATCH_DATA_PARALLEL_SIZE: "1,0",
    }, f"actual telemetry: {telemetry}"


def test_telemetry_dedups_by_model_identity():
    """Distinct models sharing reported fields stay separate; identical builds merge."""
    recorder = FakeTelemetryRecorder.remote()

    def record_tag_func(key, value):
        ray.get(recorder.record.remote(key, value))

    telemetry_agent = get_or_create_telemetry_agent()
    ray.get(telemetry_agent.remote_telemetry_agent._reset.remote())
    telemetry_agent._update_record_tag_func(record_tag_func)

    # Two distinct models with identical reported fields (same architecture/config).
    common = dict(
        model_architecture="LlamaForCausalLM",
        batch_size=64,
        concurrency=4,
        task_type="generate",
    )
    telemetry_agent.push_telemetry_report(
        BatchModelTelemetry(model_id_hash="hash_a", **common)
    )
    telemetry_agent.push_telemetry_report(
        BatchModelTelemetry(model_id_hash="hash_b", **common)
    )
    # A repeated identical build of model A must not add a third entry.
    telemetry_agent.push_telemetry_report(
        BatchModelTelemetry(model_id_hash="hash_a", **common)
    )

    telemetry = ray.get(recorder.telemetry.remote())
    assert (
        telemetry[TagKey.LLM_BATCH_MODEL_ARCHITECTURE]
        == "LlamaForCausalLM,LlamaForCausalLM"
    ), f"actual telemetry: {telemetry}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
