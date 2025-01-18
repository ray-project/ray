"""The HTTP request processor."""

from typing import Optional

from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorConfig,
    ProcessorBuilder,
)
from ray.llm._internal.batch.stages import HttpRequestStage


class HttpRequestProcessorConfig(ProcessorConfig):
    """The configuration for the HTTP request processor."""

    # The URL to query.
    url: str
    # The query header. Note that we will add
    # "Content-Type: application/json" to be the heahder for sure
    # because we only deal with requests body in JSON.
    header: Optional[str] = None
    # Queries per second. If None, the query will be sent sequentially.
    qps: Optional[int] = None


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
    processor = Processor(config, **kwargs)
    processor.append_stage(
        HttpRequestStage(
            fn_constructor_kwargs=dict(
                url=config.url,
                additional_header=config.header,
                qps=config.qps,
            ),
            map_batches_kwargs=dict(
                concurrency=processor.concurrency,
            ),
        )
    )
    return processor


ProcessorBuilder.register(HttpRequestProcessorConfig, build_http_request_processor)
