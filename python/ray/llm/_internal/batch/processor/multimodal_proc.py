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
from ray.llm._internal.batch.stages import PrepareMultimodalStage


class MultimodalProcessorConfig(ProcessorConfig):
    """The configuration for the multimodal processor."""

    model: str = Field(
        description="Name or path of the Hugging Face model to use for the multimodal processor. "
        "This is required to process multimodal data according to a specific model.",
    )
    # The format to render message content.
    #   - "string" will render the content as a string.
    #     Example: `"Who are you?"`
    #   - "openai" will render the content as a list of dictionaries, similar to OpenAI schema.
    #     Example: `[{"type": "text", "text": "Who are you?"}]`
    chat_template_content_format: str = Field(
        default="string",
        choices=["string", "openai"],
        description="The content format to use for the chat template. "
        "This is used to format the chat template content according to a specific model.",
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
    stages = [
        PrepareMultimodalStage(
            fn_constructor_kwargs=dict(
                model=config.model,
                chat_template_content_format=config.chat_template_content_format,
            ),
            map_batches_kwargs=dict(
                zero_copy_batch=True,
                concurrency=config.concurrency,
                batch_size=config.batch_size,
            ),
        ),
    ]
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
