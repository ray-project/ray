"""The HTTP request processor."""

from typing import Any, Dict, Optional

from pydantic import Field

from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorConfig,
    ProcessorBuilder,
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


def build_http_request_processor(
    config: HttpRequestProcessorConfig, **kwargs
) -> Processor:
    """Construct a Processor and configure stages.

    Args:
        config: The configuration for the processor.
        **kwargs: The keyword arguments for the processor.

    Returns:
        The constructed processor.
    """
    stages = [
        HttpRequestStage(
            fn_constructor_kwargs=dict(
                url=config.url,
                additional_header=config.headers,
                qps=config.qps,
            ),
            map_batches_kwargs=dict(
                concurrency=config.concurrency,
            ),
        )
    ]
    processor = Processor(config, stages, **kwargs)
    return processor


ProcessorBuilder.register(HttpRequestProcessorConfig, build_http_request_processor)
