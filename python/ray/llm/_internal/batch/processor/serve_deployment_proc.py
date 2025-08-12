"""The processor that runs serve deployment."""

from typing import Optional
from pydantic import Field

import ray
from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.observability.usage_telemetry.usage import (
    BatchModelTelemetry,
    TelemetryAgent,
    get_or_create_telemetry_agent,
)
from ray.llm._internal.batch.processor.base import (
    ProcessorConfig,
    Processor,
    ProcessorBuilder,
)
from ray.llm._internal.batch.stages import (
    ServeDeploymentStage,
)
from ray.serve.llm import LLMConfig


class ServeDeploymentProcessorConfig(ProcessorConfig):
    """The configuration for the serve deployment processor."""

    # Configurations that was used to build the serve deployment
    llm_config: LLMConfig = Field(
        description="The LLM config to use for the serve deployment.",
    )
    app_name: str = Field(
        description="The name of the serve application to use.",
    )
    # Method to invoke on the serve deployment
    method: str = Field(
        description="The method to use for the serve deployment.",
    )


def build_serve_deployment_processor(
    config: ServeDeploymentProcessorConfig,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
    telemetry_agent: Optional[TelemetryAgent] = None,
) -> Processor:
    """
    Construct a processor that runs a serve deployment.

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
    stages = [
        ServeDeploymentStage(
            fn_constructor_kwargs=dict(
                deployment_name=config.llm_config.deployment_name,
                app_name=config.app_name,
                method=config.method,
            ),
            map_batches_kwargs=dict(
                concurrency=config.concurrency,
            ),
        )
    ]
    telemetry_agent = get_or_create_telemetry_agent()
    telemetry_agent.push_telemetry_report(
        BatchModelTelemetry(
            processor_config_name=type(config).__name__,
            model_architecture=config.llm_config.model_architecture,
            batch_size=config.batch_size,
            accelerator_type=config.llm_config.accelerator_type,
            concurrency=config.concurrency,
            task_type=config.method,
        )
    )
    processor = Processor(
        config,
        stages,
        preprocess=preprocess,
        postprocess=postprocess,
    )
    return processor


ProcessorBuilder.register(
    ServeDeploymentProcessorConfig, build_serve_deployment_processor
)
