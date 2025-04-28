"""The vLLM engine processor."""

from typing import Any, Dict, Optional
from pydantic import Field, root_validator

import ray
from ray.data.block import UserDefinedFunction
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    DEFAULT_MAX_TASKS_IN_FLIGHT,
)
from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorConfig,
    ProcessorBuilder,
)
from ray.llm._internal.batch.stages import (
    vLLMEngineStage,
    ChatTemplateStage,
    PrepareImageStage,
    TokenizeStage,
    DetokenizeStage,
)
from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMTaskType
from ray.llm._internal.batch.observability.usage_telemetry.usage import (
    TelemetryAgent,
    BatchModelTelemetry,
)
from ray.llm._internal.common.observability.telemetry_utils import DEFAULT_GPU_TYPE
from ray.llm._internal.common.utils.download_utils import (
    download_model_files,
    NodeModelDownloadable,
)
from ray.llm._internal.batch.observability.usage_telemetry.usage import (
    get_or_create_telemetry_agent,
)

import transformers

DEFAULT_MODEL_ARCHITECTURE = "UNKNOWN_MODEL_ARCHITECTURE"


class vLLMEngineProcessorConfig(ProcessorConfig):
    """The configuration for the vLLM engine processor."""

    # vLLM stage configurations.
    model_source: str = Field(
        description="The model source to use for the vLLM engine.",
    )
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
    runtime_env: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The runtime environment to use for the vLLM engine.",
    )
    max_pending_requests: Optional[int] = Field(
        default=None,
        description="The maximum number of pending requests. If not specified, "
        "will use the default value from the vLLM engine.",
    )
    max_concurrent_batches: int = Field(
        default=8,
        description="The maximum number of concurrent batches in the engine. "
        "This is to overlap the batch processing to avoid the tail latency of "
        "each batch. The default value may not be optimal when the batch size "
        "or the batch processing latency is too small, but it should be good "
        "enough for batch size >= 32.",
    )

    # Processor stage configurations.
    apply_chat_template: bool = Field(
        default=True, description="Whether to apply chat template."
    )
    chat_template: Optional[str] = Field(
        default=None,
        description="The chat template to use. This is usually not needed if the "
        "model checkpoint already contains the chat template.",
    )
    tokenize: bool = Field(
        default=True,
        description="Whether to tokenize the input before passing it to the "
        "vLLM engine. If not, vLLM will tokenize the prompt in the engine.",
    )
    detokenize: bool = Field(
        default=True,
        description="Whether to detokenize the output.",
    )
    has_image: bool = Field(
        default=False,
        description="Whether the input messages have images.",
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


def build_vllm_engine_processor(
    config: vLLMEngineProcessorConfig,
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

    Returns:
        The constructed processor.
    """
    ray.init(runtime_env=config.runtime_env, ignore_reinit_error=True)

    stages = []
    if isinstance(config.concurrency, int):
        processor_concurrency = (1, config.concurrency)  # copied from previous logic
    elif isinstance(config.concurrency, tuple):
        processor_concurrency = config.concurrency
    else:
        raise ValueError(
            "``concurrency`` is expected to be set as an integer or a "
            f"tuple of integers, but got: {config.concurrency}."
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
            ),
            map_batches_kwargs=dict(
                zero_copy_batch=True,
                # The number of running replicas. This is a deprecated field, but
                # we need to set `max_tasks_in_flight_per_actor` through `compute`,
                # which initiates enough many overlapping UDF calls per actor, to
                # saturate `max_concurrency`.
                compute=ray.data.ActorPoolStrategy(
                    min_size=config.concurrency,
                    max_size=config.concurrency,
                    max_tasks_in_flight_per_actor=max(
                        DEFAULT_MAX_TASKS_IN_FLIGHT, config.max_concurrent_batches
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
        trust_remote_code=config.engine_kwargs.get("trust_remote_code", False),
    )
    architecture = getattr(hf_config, "architectures", [DEFAULT_MODEL_ARCHITECTURE])[0]

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
    )
    return processor


ProcessorBuilder.register(vLLMEngineProcessorConfig, build_vllm_engine_processor)
