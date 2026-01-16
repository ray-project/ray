"""The SGLang engine processor."""

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
)
from ray.llm._internal.batch.processor.utils import (
    build_cpu_stage_map_kwargs,
    get_value_or_fallback,
)
from ray.llm._internal.batch.stages import (
    ChatTemplateStage,
    DetokenizeStage,
    SGLangEngineStage,
    TokenizeStage,
)
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    TokenizerStageConfig,
    resolve_stage_config,
)
from ray.llm._internal.batch.stages.sglang_engine_stage import SGLangTaskType
from ray.llm._internal.common.observability.telemetry_utils import DEFAULT_GPU_TYPE

DEFAULT_MODEL_ARCHITECTURE = "UNKNOWN_MODEL_ARCHITECTURE"


class SGLangEngineProcessorConfig(OfflineProcessorConfig):
    """The configuration for the SGLang engine processor."""

    # SGLang stage configurations.
    engine_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="The kwargs to pass to the SGLang engine. See "
        "https://docs.sglang.ai/backend/server_arguments.html "
        "for more details.",
    )
    task_type: SGLangTaskType = Field(
        default=SGLangTaskType.GENERATE,
        description="The task type to use. If not specified, will use "
        "'generate' by default.",
    )

    @root_validator(pre=True)
    def validate_task_type(cls, values):
        task_type_str = values.get("task_type", "generate")
        values["task_type"] = SGLangTaskType(task_type_str)
        return values


def build_sglang_engine_processor(
    config: SGLangEngineProcessorConfig,
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

    # Prepare processor defaults for merging into stage configs
    processor_defaults = {
        "batch_size": config.batch_size,
        "concurrency": config.concurrency,
        "runtime_env": config.runtime_env,
        "model_source": config.model_source,
    }

    # Resolve and build ChatTemplateStage if enabled
    chat_template_stage_cfg = resolve_stage_config(
        config.chat_template_stage,
        ChatTemplateStageConfig,
        processor_defaults,
    )
    if chat_template_stage_cfg.enabled:
        stages.append(
            ChatTemplateStage(
                fn_constructor_kwargs=dict(
                    model=chat_template_stage_cfg.model_source,
                    chat_template=get_value_or_fallback(
                        chat_template_stage_cfg.chat_template, config.chat_template
                    ),
                    chat_template_kwargs=get_value_or_fallback(
                        chat_template_stage_cfg.chat_template_kwargs,
                        chat_template_kwargs,
                    ),
                ),
                map_batches_kwargs=build_cpu_stage_map_kwargs(chat_template_stage_cfg),
            )
        )

    # Resolve and build TokenizeStage if enabled
    tokenize_stage_cfg = resolve_stage_config(
        getattr(config, "tokenize_stage", config.tokenize),
        TokenizerStageConfig,
        processor_defaults,
    )
    if tokenize_stage_cfg.enabled:
        stages.append(
            TokenizeStage(
                fn_constructor_kwargs=dict(
                    model=tokenize_stage_cfg.model_source,
                ),
                map_batches_kwargs=build_cpu_stage_map_kwargs(tokenize_stage_cfg),
            )
        )

    # Core stage -- the SGLang engine.
    stages.append(
        SGLangEngineStage(
            fn_constructor_kwargs=dict(
                model=config.model_source,
                engine_kwargs=config.engine_kwargs,
                task_type=config.task_type,
                max_pending_requests=config.max_pending_requests,
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

    # Resolve and build DetokenizeStage if enabled
    detokenize_stage_cfg = resolve_stage_config(
        getattr(config, "detokenize_stage", config.detokenize),
        DetokenizeStageConfig,
        processor_defaults,
    )
    if detokenize_stage_cfg.enabled:
        stages.append(
            DetokenizeStage(
                fn_constructor_kwargs=dict(
                    model=detokenize_stage_cfg.model_source,
                ),
                map_batches_kwargs=build_cpu_stage_map_kwargs(detokenize_stage_cfg),
            )
        )

    hf_config = transformers.AutoConfig.from_pretrained(config.model_source)
    architecture = getattr(hf_config, "architectures", [DEFAULT_MODEL_ARCHITECTURE])[0]

    telemetry_agent = get_or_create_telemetry_agent()
    telemetry_agent.push_telemetry_report(
        BatchModelTelemetry(
            processor_config_name=type(config).__name__,
            model_architecture=architecture,
            batch_size=config.batch_size,
            accelerator_type=config.accelerator_type or DEFAULT_GPU_TYPE,
            concurrency=config.concurrency,
            task_type=SGLangTaskType(config.task_type),
            tensor_parallel_size=config.engine_kwargs.get("tp_size", 1),
            data_parallel_size=config.engine_kwargs.get("dp_size", 1),
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


ProcessorBuilder.register(SGLangEngineProcessorConfig, build_sglang_engine_processor)
