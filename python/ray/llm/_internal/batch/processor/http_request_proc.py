"""The HTTP request processor."""

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
from ray.llm._internal.batch.stages import HttpRequestStage


class HttpRequestProcessorConfig(ProcessorConfig):
    """The configuration for the HTTP request processor."""

    batch_size: int = Field(
        default=64,
        description="The batch size.",
    )
    url: str = Field(
        description="The URL to query.",
    )
    headers: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The query header. Note that we will add "
        "'Content-Type: application/json' to be the header for sure "
        "because we only deal with requests body in JSON.",
    )
    qps: Optional[int] = Field(
        default=None,
        description="The maximum number of requests per second to avoid rate limit. "
        "If None, the request will be sent sequentially.",
    )
    max_retries: int = Field(
        default=0,
        description="The maximum number of retries per request in the event of failures.",
    )
    base_retry_wait_time_in_s: float = Field(
        default=1,
        description="The base wait time for a retry during exponential backoff.",
    )
    # Since `session_factory` is a callable, we use type Any to avoid pydantic serialization issues
    session_factory: Optional[Any] = Field(
        default=None,
        description="Optional session factory to be used for initializing a client session. Type: Callable[[], ClientSession]",
        # exclude from JSON serialization since `session_factory` is a callable
        exclude=True,
    )


def build_http_request_processor(
    config: HttpRequestProcessorConfig,
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
        HttpRequestStage(
            fn_constructor_kwargs=dict(
                url=config.url,
                additional_header=config.headers,
                qps=config.qps,
                max_retries=config.max_retries,
                base_retry_wait_time_in_s=config.base_retry_wait_time_in_s,
                session_factory=config.session_factory,
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


ProcessorBuilder.register(HttpRequestProcessorConfig, build_http_request_processor)
