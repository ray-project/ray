"""The multimodal processor."""

from typing import Any, Dict, Optional

from pydantic import Field

from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.observability.usage_telemetry.usage import (
    BatchModelTelemetry,
    get_or_create_telemetry_agent,
)
from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorBuilder,
    ProcessorConfig,
)
from ray.llm._internal.batch.processor.utils import (
    build_cpu_stage_map_kwargs,
)
from ray.llm._internal.batch.stages import PrepareMultimodalStage
from ray.llm._internal.batch.stages.configs import (
    PrepareMultimodalStageConfig,
    resolve_stage_config,
)


class MultimodalProcessorConfig(ProcessorConfig):
    """The configuration for the multimodal processor."""

    model_source: str = Field(
        description="Name or path of the Hugging Face model to use for the multimodal processor. "
        "This is required to process multimodal data according to a specific model.",
    )

    # Nested stage configurations
    prepare_multimodal_stage: Any = Field(
        default=True,
        description="Prepare multimodal stage config (bool | dict | PrepareMultimodalStageConfig).",
    )


def build_multimodal_processor(
    config: MultimodalProcessorConfig,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
    preprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    postprocess_map_kwargs: Optional[Dict[str, Any]] = None,
) -> Processor:
    """Construct a Processor and configure stages.

    Args:
        config: The configuration for the processor.
        preprocess: An optional lambda function that takes a row (dict) as input
            and returns a preprocessed row (dict). The output row must contain the
            required fields for the following processing stages.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict).
        preprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            preprocess stage (e.g., num_cpus, memory, concurrency).
        postprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            postprocess stage (e.g., num_cpus, memory, concurrency).

    Returns:
        The constructed processor.
    """
    # Prepare processor defaults for merging into stage configs
    processor_defaults = {
        "batch_size": config.batch_size,
        "concurrency": config.concurrency,
        "model_source": config.model_source,
    }

    prepare_multimodal_stage_cfg = resolve_stage_config(
        config.prepare_multimodal_stage,
        PrepareMultimodalStageConfig,
        processor_defaults,
    )

    stages = []
    if prepare_multimodal_stage_cfg.enabled:
        stages.append(
            PrepareMultimodalStage(
                fn_constructor_kwargs=dict(
                    model=prepare_multimodal_stage_cfg.model_source,
                    chat_template_content_format=prepare_multimodal_stage_cfg.chat_template_content_format,
                ),
                map_batches_kwargs=build_cpu_stage_map_kwargs(
                    prepare_multimodal_stage_cfg
                ),
            )
        )

    telemetry_agent = get_or_create_telemetry_agent()
    telemetry_agent.push_telemetry_report(
        BatchModelTelemetry(
            processor_config_name=type(config).__name__,
            concurrency=config.concurrency,
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


ProcessorBuilder.register(MultimodalProcessorConfig, build_multimodal_processor)
