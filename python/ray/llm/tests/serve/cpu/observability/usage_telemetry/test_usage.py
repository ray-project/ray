import asyncio
import json
import sys

import pytest

import ray
from ray._common.usage.usage_lib import TagKey
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    LLMEngine,
    LoraConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.observability.usage_telemetry.usage import (
    HardwareUsage,
    _get_or_create_telemetry_agent,
    _retry_get_telemetry_agent,
    classify_start_failure,
    exception_type,
    push_telemetry_report_for_all_models,
    root_exception_type,
)


@ray.remote(num_cpus=0)
class TelemetryRecorder:
    def __init__(self):
        self._telemetry = {}

    def record(self, key, value):
        self._telemetry[key] = value

    def telemetry(self):
        return self._telemetry


def test_push_telemetry_report_for_all_models(disable_placement_bundles):
    recorder = TelemetryRecorder.remote()

    def record_tag_func(key, value):
        ray.get(recorder.record.remote(key, value))

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
    llm_config_no_accelerator_type = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_config_no_accelerator_type_id",
        ),
    )
    llm_config_no_accelerator_type._set_model_architecture(
        model_architecture="llm_config_no_accelerator_type_arch"
    )
    all_models = [
        llm_config_model,
        llm_config_autoscale_model,
        llm_config_json_mode_model,
        llm_config_lora_model,
        llm_config_no_accelerator_type,
    ]

    def fake_get_lora_model_ids(dynamic_lora_loading_path, base_model_id):
        return ["lora_model_id_1", "lora_model_id_2"]

    def fake_get_gpu_type(*args, **kwargs):
        return ["Intel Xeon", "L40S"]

    # Ensure that the telemetry is empty before pushing the reports.
    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry == {}
    push_telemetry_report_for_all_models(
        all_models=all_models,
        get_lora_model_func=fake_get_lora_model_ids,
        get_hardware_fn=fake_get_gpu_type,
    )

    # All five models use engine defaults, so each gets the same config object.
    default_engine_config = json.dumps(
        [
            {
                "quantization": "none",
                "dtype": "auto",
                "max_model_len": 0,
                "prefix_caching": "default",
                "chunked_prefill": "default",
                "kv_cache_dtype": "auto",
                "speculative_decoding": "off",
                "enforce_eager": "default",
                "pipeline_parallel": 1,
                "accelerator_kind": "gpu",
                "log_engine_metrics": "on",
                "distributed_executor_backend": "default",
                "load_format": "auto",
                "multimodal": "0",
                "set_engine_kwargs": [],
            }
        ]
        * 5,
        separators=(",", ":"),
    )

    # Ensure that the telemetry is correct after pushing the reports.
    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry == {
        TagKey.LLM_SERVE_SERVE_MULTIPLE_MODELS: "1",
        TagKey.LLM_SERVE_SERVE_MULTIPLE_APPS: "0",
        TagKey.LLM_SERVE_LORA_BASE_MODELS: "llm_config_lora_model_arch",
        TagKey.LLM_SERVE_INITIAL_NUM_LORA_ADAPTERS: "2",
        TagKey.LLM_SERVE_AUTOSCALING_ENABLED_MODELS: "llm_config_autoscale_model_arch",
        TagKey.LLM_SERVE_AUTOSCALING_MIN_REPLICAS: "2",
        TagKey.LLM_SERVE_AUTOSCALING_MAX_REPLICAS: "3",
        TagKey.LLM_SERVE_TENSOR_PARALLEL_DEGREE: "1,1,1,1,1",
        TagKey.LLM_SERVE_NUM_REPLICAS: "1,2,1,1,1",
        TagKey.LLM_SERVE_MODELS: "llm_model_arch,llm_config_autoscale_model_arch,llm_config_json_model_arch,llm_config_lora_model_arch,llm_config_no_accelerator_type_arch",
        TagKey.LLM_SERVE_GPU_TYPE: "L4,A10G,A10G,A10G,L40S",
        TagKey.LLM_SERVE_NUM_GPUS: "1,1,1,1,1",
        TagKey.LLM_SERVE_DEPLOY_OUTCOME: "success,success,success,success,success",
        TagKey.LLM_SERVE_SERVING_PATTERN: "default,default,default,default,default",
        TagKey.LLM_SERVE_ENGINE_CONFIG: default_engine_config,
        # All deploys succeeded, so each failure object is empty.
        TagKey.LLM_SERVE_DEPLOY_FAILURE: "[{},{},{},{},{}]",
    }


@ray.remote(num_cpus=0)
class Replica:
    def wait_for_init(self):
        """
        When this method returns, the actor initialization is guaranteed
        to be complete.

        This is used for synchronization between multiple replicas,
        increasing the chance for get_telemetry_agent() to be called
        at the same time.
        """
        pass

    def get_telemetry_agent(self):
        return _retry_get_telemetry_agent()


def test_telemetry_race_condition():
    replicas = [Replica.remote() for _ in range(30)]
    init_refs = [replica.wait_for_init.remote() for replica in replicas]
    ray.get(init_refs)

    get_refs = [replica.get_telemetry_agent.remote() for replica in replicas]
    telemetry_agents = ray.get(get_refs)
    for telemetry_agent in telemetry_agents:
        assert telemetry_agent is not None
    assert len(set(telemetry_agents)) == 1


def test_infer_gpu_from_hardware():
    # Test with a valid GPU type
    def fake_get_gpu_type(*args, **kwargs):
        return ["Intel Xeon", "A10G"]

    result = HardwareUsage(fake_get_gpu_type).infer_gpu_from_hardware()
    assert result == "A10G"

    # Test with an unsupported GPU type
    def fake_get_gpu_type(*args, **kwargs):
        return ["Intel Xeon", "G"]

    result = HardwareUsage(fake_get_gpu_type).infer_gpu_from_hardware()
    assert result == "UNSPECIFIED"


def test_telemetry_dedups_replicas_and_restarts(disable_placement_bundles):
    """The same model reported by many replicas/restarts collapses to one entry."""
    recorder = TelemetryRecorder.remote()

    def record_tag_func(key, value):
        ray.get(recorder.record.remote(key, value))

    telemetry_agent = _get_or_create_telemetry_agent()
    telemetry_agent._reset_models.remote()
    telemetry_agent._update_record_tag_func.remote(record_tag_func)

    config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="dup_model_id"),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="L4",
    )
    config._set_model_architecture(model_architecture="dup_arch")

    # Simulate three replicas (or restarts) of the SAME model reporting.
    for _ in range(3):
        push_telemetry_report_for_all_models(
            all_models=[config],
            get_hardware_fn=lambda *a, **k: ["L4"],
        )

    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry[TagKey.LLM_SERVE_MODELS] == "dup_arch"
    assert telemetry[TagKey.LLM_SERVE_NUM_REPLICAS] == "1"
    assert telemetry[TagKey.LLM_SERVE_GPU_TYPE] == "L4"


def test_telemetry_reports_fixed_num_replicas(disable_placement_bundles):
    """A fixed (non-autoscaling) num_replicas is reported, not hardcoded to 1."""
    recorder = TelemetryRecorder.remote()

    def record_tag_func(key, value):
        ray.get(recorder.record.remote(key, value))

    telemetry_agent = _get_or_create_telemetry_agent()
    telemetry_agent._reset_models.remote()
    telemetry_agent._update_record_tag_func.remote(record_tag_func)

    config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="fixed_replicas_model"),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="L4",
        deployment_config=dict(num_replicas=4),
    )
    config._set_model_architecture(model_architecture="fixed_arch")

    push_telemetry_report_for_all_models(
        all_models=[config],
        get_hardware_fn=lambda *a, **k: ["L4"],
    )

    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry[TagKey.LLM_SERVE_NUM_REPLICAS] == "4"


def test_telemetry_reports_zero_num_replicas(disable_placement_bundles):
    """An explicit num_replicas=0 is reported as 0, not coerced to 1."""
    recorder = TelemetryRecorder.remote()

    def record_tag_func(key, value):
        ray.get(recorder.record.remote(key, value))

    telemetry_agent = _get_or_create_telemetry_agent()
    telemetry_agent._reset_models.remote()
    telemetry_agent._update_record_tag_func.remote(record_tag_func)

    config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="zero_replicas_model"),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="L4",
        deployment_config=dict(num_replicas=0),
    )
    config._set_model_architecture(model_architecture="zero_arch")

    push_telemetry_report_for_all_models(
        all_models=[config],
        get_hardware_fn=lambda *a, **k: ["L4"],
    )

    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry[TagKey.LLM_SERVE_NUM_REPLICAS] == "0"


def test_telemetry_reports_auto_num_replicas(disable_placement_bundles):
    """num_replicas="auto" is reported as autoscaling, not dropped."""
    recorder = TelemetryRecorder.remote()

    def record_tag_func(key, value):
        ray.get(recorder.record.remote(key, value))

    telemetry_agent = _get_or_create_telemetry_agent()
    telemetry_agent._reset_models.remote()
    telemetry_agent._update_record_tag_func.remote(record_tag_func)

    config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="auto_replicas_model"),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="L4",
        deployment_config=dict(num_replicas="auto"),
    )
    config._set_model_architecture(model_architecture="auto_arch")

    push_telemetry_report_for_all_models(
        all_models=[config],
        get_hardware_fn=lambda *a, **k: ["L4"],
    )

    telemetry = ray.get(recorder.telemetry.remote())
    # Recorded as autoscaling with an integer replica count (not the string "auto").
    assert telemetry[TagKey.LLM_SERVE_AUTOSCALING_ENABLED_MODELS] == "auto_arch"
    assert telemetry[TagKey.LLM_SERVE_NUM_REPLICAS].isdigit()


def test_deploy_outcome_and_engine_facts(disable_placement_bundles):
    """Non-default deploy_outcome / serving_pattern (flat) and the bundled
    engine-config JSON are recorded."""
    recorder = TelemetryRecorder.remote()

    def record_tag_func(key, value):
        ray.get(recorder.record.remote(key, value))

    telemetry_agent = _get_or_create_telemetry_agent()
    telemetry_agent._reset_models.remote()
    telemetry_agent._update_record_tag_func.remote(record_tag_func)

    config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="facts_model"),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="L4",
        engine_kwargs=dict(
            quantization="fp8",
            dtype="bfloat16",
            max_model_len=8192,
            enable_prefix_caching=True,
            enforce_eager=False,
            kv_cache_dtype="fp8",
            pipeline_parallel_size=2,
            distributed_executor_backend="ray",
            load_format="runai_streamer",
        ),
    )
    config._set_model_architecture(model_architecture="facts_arch")

    push_telemetry_report_for_all_models(
        all_models=[config],
        get_hardware_fn=lambda *a, **k: ["L4"],
        deploy_outcome="oom",
        serving_pattern="pd",
    )

    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry[TagKey.LLM_SERVE_DEPLOY_OUTCOME] == "oom"
    assert telemetry[TagKey.LLM_SERVE_SERVING_PATTERN] == "pd"

    (engine_config,) = json.loads(telemetry[TagKey.LLM_SERVE_ENGINE_CONFIG])
    assert engine_config == {
        "quantization": "fp8",
        "dtype": "bfloat16",
        "max_model_len": 8192,
        "prefix_caching": "on",
        "chunked_prefill": "default",
        "kv_cache_dtype": "fp8",
        "speculative_decoding": "off",
        "enforce_eager": "off",
        "pipeline_parallel": 2,
        "accelerator_kind": "gpu",
        "log_engine_metrics": "on",
        "distributed_executor_backend": "ray",
        "load_format": "runai_streamer",
        "multimodal": "0",
        "set_engine_kwargs": sorted(
            [
                "quantization",
                "dtype",
                "max_model_len",
                "enable_prefix_caching",
                "enforce_eager",
                "kv_cache_dtype",
                "pipeline_parallel_size",
                "distributed_executor_backend",
                "load_format",
            ]
        ),
    }


class OutOfMemoryError(RuntimeError):
    """Stands in for torch.cuda.OutOfMemoryError (matched by type name)."""


@pytest.mark.parametrize(
    "exc,expected",
    [
        (asyncio.TimeoutError(), "engine_start_timeout"),
        (ImportError("no module named flash_attn"), "import_error"),
        (ModuleNotFoundError("flash_attn"), "import_error"),
        (MemoryError(), "oom"),
        (OutOfMemoryError("cuda oom"), "oom"),
        (ValueError("bad config value"), "invalid_config"),
        (RuntimeError("worker crashed"), "other"),
        # The message is never inspected, so embedded tokens can't be
        # misclassified: a plain RuntimeError stays "other" regardless of text.
        (RuntimeError("CUDA out of memory while loading the model"), "other"),
        (RuntimeError("failed to download from /home/user/oom_test/"), "other"),
    ],
)
def test_classify_start_failure(exc, expected):
    """Failures map to a fixed enum by exception type; the message is never read."""
    assert classify_start_failure(exc) == expected


def test_exception_type_is_qualified_and_non_identifying():
    assert exception_type(ValueError("x")) == "builtins.ValueError"
    assert exception_type(OutOfMemoryError("x")).endswith(".OutOfMemoryError")


def test_root_exception_type_walks_cause_chain():
    """The root cause is recorded, since engine errors surface wrapped."""
    try:
        try:
            raise RuntimeError("worker died")
        except RuntimeError as inner:
            raise ValueError("wrapper") from inner
    except ValueError as wrapper:
        assert exception_type(wrapper) == "builtins.ValueError"
        assert root_exception_type(wrapper) == "builtins.RuntimeError"
    # No chain: root is the exception itself.
    assert root_exception_type(KeyError("k")) == "builtins.KeyError"


def test_deploy_failure_detail_is_recorded(disable_placement_bundles):
    """A failed deploy records exc_type / root_type / phase in the JSON tag."""
    recorder = TelemetryRecorder.remote()

    def record_tag_func(key, value):
        ray.get(recorder.record.remote(key, value))

    telemetry_agent = _get_or_create_telemetry_agent()
    telemetry_agent._reset_models.remote()
    telemetry_agent._update_record_tag_func.remote(record_tag_func)

    config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="failed_model"),
        llm_engine=LLMEngine.vLLM,
        accelerator_type="L4",
    )
    config._set_model_architecture(model_architecture="failed_arch")

    push_telemetry_report_for_all_models(
        all_models=[config],
        get_hardware_fn=lambda *a, **k: ["L4"],
        deploy_outcome="oom",
        deploy_failure={
            "exc_type": "ray.exceptions.RayTaskError",
            "root_type": "torch.cuda.OutOfMemoryError",
            "phase": "engine_start",
        },
    )

    telemetry = ray.get(recorder.telemetry.remote())
    assert telemetry[TagKey.LLM_SERVE_DEPLOY_OUTCOME] == "oom"
    (failure,) = json.loads(telemetry[TagKey.LLM_SERVE_DEPLOY_FAILURE])
    assert failure == {
        "exc_type": "ray.exceptions.RayTaskError",
        "root_type": "torch.cuda.OutOfMemoryError",
        "phase": "engine_start",
    }


def test_curated_engine_keys_still_exist_in_vllm():
    """Guard against engine drift: the engine_kwargs we decode must stay real
    vLLM EngineArgs fields, so a rename breaks CI instead of silently recording
    nothing. Skips where vLLM isn't importable."""
    pytest.importorskip("vllm")
    import dataclasses

    from vllm.engine.arg_utils import AsyncEngineArgs

    fields = {f.name for f in dataclasses.fields(AsyncEngineArgs)}
    curated = {
        "quantization",
        "dtype",
        "max_model_len",
        "enable_prefix_caching",
        "enable_chunked_prefill",
        "kv_cache_dtype",
        "enforce_eager",
        "pipeline_parallel_size",
        "distributed_executor_backend",
        "load_format",
    }
    assert curated <= fields, f"no longer in vLLM AsyncEngineArgs: {curated - fields}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
