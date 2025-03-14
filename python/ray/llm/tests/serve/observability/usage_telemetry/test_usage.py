import ray
from ray._private.usage.usage_lib import TagKey
import pytest
import sys

from ray.llm._internal.serve.observability.usage_telemetry.usage import (
    _get_or_create_telemetry_agent,
    push_telemetry_report_for_all_models,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    LLMEngine,
    LoraConfig,
    ModelLoadingConfig,
)


@ray.remote(num_cpus=0)
class TelemetryRecorder:
    def __init__(self):
        self._telemetry = {}

    def record(self, key, value):
        self._telemetry[key] = value

    def telemetry(self):
        return self._telemetry


def test_push_telemetry_report_for_all_models():
    recorder = TelemetryRecorder.remote()

    def record_tag_func(key, value):
        recorder.record.remote(key, value)

    telemetry_agent = _get_or_create_telemetry_agent()
    telemetry_agent._reset_models.remote()
    telemetry_agent._update_record_tag_func.remote(record_tag_func)

    dynamic_lora_loading_path = "s3://fake_bucket/fake_path"
    llm_config_model = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_model_id",
        ),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="L4",
    )
    llm_config_model._set_model_architecture(model_architecture="llm_model_arch")
    llm_config_autoscale_model = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_config_autoscale_model_id",
        ),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="A10G",
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=2,
                max_replicas=3,
            ),
        ),
    )
    llm_config_autoscale_model._set_model_architecture(
        model_architecture="llm_config_autoscale_model_arch"
    )
    llm_config_json_mode_model = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_config_json_model_id",
        ),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="A10G",
    )
    llm_config_json_mode_model._set_model_architecture(
        model_architecture="llm_config_json_model_arch"
    )
    llm_config_lora_model = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_config_lora_model_id",
        ),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="A10G",
        lora_config=LoraConfig(dynamic_lora_loading_path=dynamic_lora_loading_path),
    )
    llm_config_lora_model._set_model_architecture(
        model_architecture="llm_config_lora_model_arch"
    )
    all_models = [
        llm_config_model,
        llm_config_autoscale_model,
        llm_config_json_mode_model,
        llm_config_lora_model,
    ]

    def fake_get_lora_model_ids(dynamic_lora_loading_path, base_model_id):
        return ["lora_model_id_1", "lora_model_id_2"]

    # Ensure that the telemetry is empty before pushing the reports.
    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry == {}
    push_telemetry_report_for_all_models(
        all_models=all_models,
        get_lora_model_func=fake_get_lora_model_ids,
    )

    # Ensure that the telemetry is correct after pushing the reports.
    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry == {
        TagKey.LLM_SERVE_SERVE_MULTIPLE_MODELS: "1",
        TagKey.LLM_SERVE_SERVE_MULTIPLE_APPS: "0",
        TagKey.LLM_SERVE_JSON_MODE_MODELS: "llm_model_arch,llm_config_autoscale_model_arch,llm_config_json_model_arch,llm_config_lora_model_arch",
        TagKey.LLM_SERVE_JSON_MODE_NUM_REPLICAS: "1,2,1,1",
        TagKey.LLM_SERVE_LORA_BASE_MODELS: "llm_config_lora_model_arch",
        TagKey.LLM_SERVE_INITIAL_NUM_LORA_ADAPTERS: "2",
        TagKey.LLM_SERVE_AUTOSCALING_ENABLED_MODELS: "llm_config_autoscale_model_arch",
        TagKey.LLM_SERVE_AUTOSCALING_MIN_REPLICAS: "2",
        TagKey.LLM_SERVE_AUTOSCALING_MAX_REPLICAS: "3",
        TagKey.LLM_SERVE_TENSOR_PARALLEL_DEGREE: "1,1,1,1",
        TagKey.LLM_SERVE_NUM_REPLICAS: "1,2,1,1",
        TagKey.LLM_SERVE_MODELS: "llm_model_arch,llm_config_autoscale_model_arch,llm_config_json_model_arch,llm_config_lora_model_arch",
        TagKey.LLM_SERVE_GPU_TYPE: "L4,A10G,A10G,A10G",
        TagKey.LLM_SERVE_NUM_GPUS: "1,1,1,1",
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
