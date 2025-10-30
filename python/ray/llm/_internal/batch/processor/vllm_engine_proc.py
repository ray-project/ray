"""The vLLM engine processor."""

from typing import Any, Dict, List, Literal, Optional

import transformers
from pydantic import ConfigDict, Field, root_validator

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
)
from ray.llm._internal.batch.stages import (
    ChatTemplateStage,
    DetokenizeStage,
    PrepareImageStage,
    TokenizeStage,
    vLLMEngineStage,
)
from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMTaskType
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.observability.telemetry_utils import DEFAULT_GPU_TYPE
from ray.llm._internal.common.utils.download_utils import (
    STREAMING_LOAD_FORMATS,
    NodeModelDownloadable,
    download_model_files,
)

DEFAULT_MODEL_ARCHITECTURE = "UNKNOWN_MODEL_ARCHITECTURE"


class BundleSchema(BaseModelExtended):
    model_config = ConfigDict(extra="allow")
    CPU: Optional[int] = Field(default=1, description="The number of CPUs per bundle.")
    GPU: Optional[int] = Field(default=1, description="The number of GPUs per bundle.")


class PlacementGroupSchema(BaseModelExtended):
    bundles: List[BundleSchema] = Field(
        default_factory=list, description="The bundles for the placement group."
    )
    strategy: Literal["PACK", "STRICT_PACK", "SPREAD", "STRICT_SPREAD"] = Field(
        default="PACK", description="The strategy for the placement group."
    )


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
    # Custom placement group config for TP/PP.
    placement_group_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Ray placement group configuration for scheduling vLLM engine workers. "
        "Should be a dictionary with 'bundles' (list of resource dicts, e.g., {'CPU': 1, 'GPU': 1}) "
        "and an optional 'strategy' key ('PACK', 'STRICT_PACK', 'SPREAD', or 'STRICT_SPREAD'). "
        "For ray distributed executor backend, each bundle must specify at most one GPU. "
        "For mp backend, the 'strategy' field is ignored.",
    )

    @root_validator(pre=True)
    def validate_task_type(cls, values):
        task_type_str = values.get("task_type", "generate")
        values["task_type"] = vLLMTaskType(task_type_str)
        return values

    @root_validator(pre=True)
    def validate_placement_group_config(cls, values):
        placement_group_config = values.get("placement_group_config")
        if placement_group_config is not None:
            values["placement_group_config"] = PlacementGroupSchema(
                **placement_group_config
            ).model_dump()
        return values


def build_vllm_engine_processor(
    config: vLLMEngineProcessorConfig,
    chat_template_kwargs: Optional[Dict[str, Any]] = None,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
    preprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    postprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    telemetry_agent: Optional[TelemetryAgent] = None,
) -> Processor:
    """Construct a Processor and configure stages.

    Args:
        config: The configuration for the processor.
        chat_template_kwargs: The optional kwargs to pass to apply_chat_template.
        preprocess: An optional lambda function that takes a row (dict) as input
            and returns a preprocessed row (dict). The output row must contain the
            required fields for the following processing stages.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict).
        preprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            preprocess stage (e.g., num_cpus, memory, concurrency).
        postprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            postprocess stage (e.g., num_cpus, memory, concurrency).
        telemetry_agent: An optional telemetry agent for collecting usage telemetry.

    Returns:
        The constructed processor.
    """
    ray.init(runtime_env=config.runtime_env, ignore_reinit_error=True)

    stages = []

    if config.has_image:
        stages.append(
            PrepareImageStage(
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=config.get_concurrency(),
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
                    chat_template_kwargs=chat_template_kwargs,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=config.get_concurrency(),
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
                    concurrency=config.get_concurrency(),
                    batch_size=config.batch_size,
                    runtime_env=config.runtime_env,
                ),
            )
        )

    # Core stage -- the vLLM engine.

    stages.append(
        vLLMEngineStage(
            fn_constructor_kwargs=dict(
                batch_size=config.batch_size,
                max_concurrent_batches=config.max_concurrent_batches,
                model=config.model_source,
                engine_kwargs=config.engine_kwargs,
                task_type=config.task_type,
                max_pending_requests=config.max_pending_requests,
                dynamic_lora_loading_path=config.dynamic_lora_loading_path,
                placement_group_config=config.placement_group_config,
            ),
            map_batches_kwargs=dict(
                zero_copy_batch=True,
                # The number of running replicas. This is a deprecated field, but
                # we need to set `max_tasks_in_flight_per_actor` through `compute`,
                # which initiates enough many overlapping UDF calls per actor, to
                # saturate `max_concurrency`.
                compute=ray.data.ActorPoolStrategy(
                    min_size=config.get_concurrency(autoscaling_enabled=False)[0],
                    max_size=config.get_concurrency(autoscaling_enabled=False)[1],
                    max_tasks_in_flight_per_actor=config.experimental.get(
                        "max_tasks_in_flight_per_actor", DEFAULT_MAX_TASKS_IN_FLIGHT
                    ),
                ),
                # The number of running batches "per actor" in Ray Core level.
                # This is used to make sure we overlap batches to avoid the tail
                # latency of each batch.
                max_concurrency=config.max_concurrent_batches,
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
                    concurrency=config.get_concurrency(),
                    batch_size=config.batch_size,
                    runtime_env=config.runtime_env,
                ),
            )
        )

    # We download the config files here so that we can report the underlying architecture to the telemetry system.
    # This should be a lightweight operation.
    if config.engine_kwargs.get("load_format", None) in STREAMING_LOAD_FORMATS:
        download_model_mode = NodeModelDownloadable.EXCLUDE_SAFETENSORS
    else:
        download_model_mode = NodeModelDownloadable.TOKENIZER_ONLY
    model_path = download_model_files(
        model_id=config.model_source,
        mirror_config=None,
        download_model=download_model_mode,
        download_extra_files=False,
    )
    hf_config = transformers.AutoConfig.from_pretrained(
        model_path,
        trust_remote_code=config.engine_kwargs.get("trust_remote_code", False),
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
            pipeline_parallel_size=config.engine_kwargs.get(
                "pipeline_parallel_size", 1
            ),
            tensor_parallel_size=config.engine_kwargs.get("tensor_parallel_size", 1),
        )
    )

    processor = Processor(
        config,
        stages,
        preprocess=preprocess,
        postprocess=postprocess,
        preprocess_map_kwargs=preprocess_map_kwargs,
        postprocess_map_kwargs=postprocess_map_kwargs,
    )
    return processor


ProcessorBuilder.register(vLLMEngineProcessorConfig, build_vllm_engine_processor)
