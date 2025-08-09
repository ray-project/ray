"""The vLLM engine processor."""

import uuid
from typing import Any, Dict, Optional

import transformers
from pydantic import Field, root_validator

import ray
from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.observability.usage_telemetry.usage import (
    BatchModelTelemetry,
    TelemetryAgent,
    get_or_create_telemetry_agent,
)
from ray.llm._internal.batch.processor.base import (
    DEFAULT_MAX_TASKS_IN_FLIGHT,
    OfflineProcessorConfig,
    Processor,
    ProcessorBuilder,
    ProcessorConfig,
)
from ray.llm._internal.batch.processor.shared_engine import _shared_engine_registry
from ray.llm._internal.batch.stages import (
    ChatTemplateStage,
    DetokenizeStage,
    PrepareImageStage,
    TokenizeStage,
    vLLMEngineStage,
    vLLMSharedEngineStage,
)
from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMTaskType
from ray.llm._internal.common.observability.telemetry_utils import DEFAULT_GPU_TYPE
from ray.llm._internal.common.utils.download_utils import (
    NodeModelDownloadable,
    download_model_files,
)
from ray.llm._internal.serve.builders.application_builders import build_llm_deployment
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.serve import run

DEFAULT_MODEL_ARCHITECTURE = "UNKNOWN_MODEL_ARCHITECTURE"


class vLLMEngineProcessorConfig(OfflineProcessorConfig):
    """The configuration for the vLLM engine processor."""

    # vLLM stage configurations.
    engine_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="The kwargs to pass to the vLLM engine. See "
        "https://docs.vllm.ai/en/latest/serving/engine_args.html "
        "for more details.",
    )
    task_type: vLLMTaskType = Field(
        default=vLLMTaskType.GENERATE,
        description="The task type to use. If not specified, will use "
        "'generate' by default.",
    )
    # LoRA configurations.
    dynamic_lora_loading_path: Optional[str] = Field(
        default=None,
        description="The path to the dynamic LoRA adapter. It is expected "
        "to hold subfolders each for a different lora checkpoint. If not "
        "specified and LoRA is enabled, then the 'model' in LoRA "
        "requests will be interpreted as model ID used by HF transformers.",
    )

    @root_validator(pre=True)
    def validate_task_type(cls, values):
        task_type_str = values.get("task_type", "generate")
        values["task_type"] = vLLMTaskType(task_type_str)
        return values


class vLLMSharedEngineProcessorConfig(OfflineProcessorConfig):
    """Configuration for vLLM shared engine processor using Ray Serve.

    Since Ray Data prohibits sharing actors across stages, this processor uses
    Ray Serve deployments to share the same vLLM engine across multiple processing
    stages. This configuration wraps LLMConfig for Serve deployment setup and
    adds task type and map_batches parameters for the processing stage.
    """

    # LLMConfig for Serve deployment
    llm_config: LLMConfig = Field(
        description="The LLMConfig for the Serve deployment. This directly configures "
        "the vLLM engine and deployment settings."
    )
    task_type: vLLMTaskType = Field(
        default=vLLMTaskType.GENERATE,
        description="The task type to use. If not specified, will use "
        "'generate' by default.",
    )

    @root_validator(pre=True)
    def validate_task_type(cls, values):
        if isinstance(values, dict):
            task_type_str = values.get("task_type", "generate")
            values["task_type"] = vLLMTaskType(task_type_str)
        return values


def _create_serve_deployment_for_shared_engine(
    shared_processor_config: vLLMSharedEngineProcessorConfig,
) -> str:
    """Create a Ray Serve deployment for the shared engine configuration."""
    deployment_name = f"shared_llm_engine_{uuid.uuid4().hex}:"
    app = build_llm_deployment(
        shared_processor_config.llm_config, name_prefix=deployment_name
    )
    run(app, name=deployment_name)

    return deployment_name


def build_vllm_engine_processor(
    config: ProcessorConfig,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
    telemetry_agent: Optional[TelemetryAgent] = None,
) -> Processor:
    """Construct a Processor and configure stages.
    Args:
        config: The configuration for the processor.
        preprocess: An optional lambda function that takes a row (dict) as input
            and returns a preprocessed row (dict). The output row must contain the
            required fields for the following processing stages.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict).
        telemetry_agent: An optional telemetry agent for collecting usage telemetry.

    Returns:
        The constructed processor.
    """
    ray.init(runtime_env=config.runtime_env, ignore_reinit_error=True)

    stages = []
    if isinstance(config.concurrency, int):
        # For CPU-only stages, we leverage auto-scaling to recycle resources.
        processor_concurrency = (1, config.concurrency)
    else:
        raise ValueError(
            "``concurrency`` is expected to be set as an integer,"
            f" but got: {config.concurrency}."
        )

    if config.has_image:
        stages.append(
            PrepareImageStage(
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=processor_concurrency,
                    batch_size=config.batch_size,
                ),
            )
        )
    if config.apply_chat_template:
        stages.append(
            ChatTemplateStage(
                fn_constructor_kwargs=dict(
                    model=config.model_source,
                    chat_template=config.chat_template,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=processor_concurrency,
                    batch_size=config.batch_size,
                    runtime_env=config.runtime_env,
                ),
            )
        )

    if config.tokenize:
        stages.append(
            TokenizeStage(
                fn_constructor_kwargs=dict(
                    model=config.model_source,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=processor_concurrency,
                    batch_size=config.batch_size,
                    runtime_env=config.runtime_env,
                ),
            )
        )

    if isinstance(config, vLLMSharedEngineProcessorConfig):
        engine_kwargs = config.llm_config.engine_kwargs
    else:
        engine_kwargs = config.engine_kwargs

    # Core stage -- the vLLM engine
    if isinstance(config, vLLMSharedEngineProcessorConfig):
        shared_engine_metadata = _shared_engine_registry.get_shared_metadata(config)

        if (
            shared_engine_metadata is None
            or shared_engine_metadata.deployment_name is None
        ):
            deployment_name = _create_serve_deployment_for_shared_engine(config)
            _shared_engine_registry.set_serve_deployment(config, deployment_name)
        else:
            deployment_name = shared_engine_metadata.deployment_name

        stages.append(
            vLLMSharedEngineStage(
                fn_constructor_kwargs=dict(
                    serve_deployment_name=deployment_name,
                    task_type=config.task_type,
                    model_id=config.llm_config.model_loading_config.model_id,
                ),
                map_batches_kwargs=dict(
                    batch_size=config.batch_size,
                    concurrency=config.concurrency,
                    zero_copy_batch=True,
                ),
            )
        )
    else:
        stages.append(
            vLLMEngineStage(
                fn_constructor_kwargs=dict(
                    batch_size=config.batch_size,
                    max_concurrent_batches=config.max_concurrent_batches,
                    model=config.model_source,
                    engine_kwargs=engine_kwargs,
                    task_type=config.task_type,
                    max_pending_requests=config.max_pending_requests,
                    dynamic_lora_loading_path=config.dynamic_lora_loading_path,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    # The number of running replicas. This is a deprecated field, but
                    # we need to set `max_tasks_in_flight_per_actor` through `compute`,
                    # which initiates enough many overlapping UDF calls per actor, to
                    # saturate `max_concurrency`.
                    compute=ray.data.ActorPoolStrategy(
                        # vLLM start up time is significant, so if user give fixed
                        # concurrency, start all instances without auto-scaling.
                        min_size=config.concurrency,
                        max_size=config.concurrency,
                        max_tasks_in_flight_per_actor=config.experimental.get(
                            "max_tasks_in_flight_per_actor", DEFAULT_MAX_TASKS_IN_FLIGHT
                        ),
                    ),
                    # The number of running batches "per actor" in Ray Core level.
                    # This is used to make sure we overlap batches to avoid the tail
                    # latency of each batch.
                    max_concurrency=config.max_concurrent_batches,
                    resources=config.resources_per_bundle,
                    accelerator_type=config.accelerator_type,
                    runtime_env=config.runtime_env,
                ),
            )
        )

    if config.detokenize:
        stages.append(
            DetokenizeStage(
                fn_constructor_kwargs=dict(
                    model=config.model_source,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=processor_concurrency,
                    batch_size=config.batch_size,
                    runtime_env=config.runtime_env,
                ),
            )
        )

    model_path = download_model_files(
        model_id=config.model_source,
        mirror_config=None,
        download_model=NodeModelDownloadable.TOKENIZER_ONLY,
        download_extra_files=False,
    )
    hf_config = transformers.AutoConfig.from_pretrained(
        model_path,
        trust_remote_code=engine_kwargs.get("trust_remote_code", False),
    )

    architectures = getattr(hf_config, "architectures", [])
    architecture = architectures[0] if architectures else DEFAULT_MODEL_ARCHITECTURE

    telemetry_agent = get_or_create_telemetry_agent()
    telemetry_agent.push_telemetry_report(
        BatchModelTelemetry(
            processor_config_name=type(config).__name__,
            model_architecture=architecture,
            batch_size=config.batch_size,
            accelerator_type=config.accelerator_type or DEFAULT_GPU_TYPE,
            concurrency=config.concurrency,
            task_type=vLLMTaskType(config.task_type),
            pipeline_parallel_size=engine_kwargs.get("pipeline_parallel_size", 1),
            tensor_parallel_size=engine_kwargs.get("tensor_parallel_size", 1),
        )
    )

    processor = Processor(
        config,
        stages,
        preprocess=preprocess,
        postprocess=postprocess,
    )
    return processor


ProcessorBuilder.register(vLLMEngineProcessorConfig, build_vllm_engine_processor)
ProcessorBuilder.register(vLLMSharedEngineProcessorConfig, build_vllm_engine_processor)
