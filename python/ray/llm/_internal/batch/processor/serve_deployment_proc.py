"""The processor that runs serve deployment."""

from typing import Any, Dict, Optional, Type

from pydantic import Field

from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorBuilder,
    ProcessorConfig,
)
from ray.llm._internal.batch.stages import (
    ServeDeploymentStage,
)


class ServeDeploymentProcessorConfig(ProcessorConfig):
    """The configuration for the serve deployment processor."""

    # Configurations used to build the serve deployment
    deployment_name: str = Field(
        description="The name of the serve deployment to use.",
    )
    app_name: str = Field(
        description="The name of the serve application to use.",
        default="default",
    )
    dtype_mapping: Dict[str, Type[Any]] = Field(
        description="A dictionary mapping data type names to their corresponding request classes for the serve deployment.",
        default=None,
    )


def build_serve_deployment_processor(
    config: ServeDeploymentProcessorConfig,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
    preprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    postprocess_map_kwargs: Optional[Dict[str, Any]] = None,
) -> Processor:
    """Construct a processor that runs a serve deployment.

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
        ServeDeploymentStage(
            fn_constructor_kwargs=dict(
                deployment_name=config.deployment_name,
                app_name=config.app_name,
                dtype_mapping=config.dtype_mapping,
            ),
            map_batches_kwargs=dict(
                concurrency=config.concurrency,
            ),
        )
    ]
    # TODO (Kourosh): Add telemetry for ServeDeploymentStage
    processor = Processor(
        config,
        stages,
        preprocess=preprocess,
        postprocess=postprocess,
        preprocess_map_kwargs=preprocess_map_kwargs,
        postprocess_map_kwargs=postprocess_map_kwargs,
    )
    return processor


ProcessorBuilder.register(
    ServeDeploymentProcessorConfig, build_serve_deployment_processor
)
