"""The HTTP request processor."""

from typing import Any, Dict, Optional

from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorConfig,
    ProcessorBuilder,
)
from ray.llm._internal.batch.stages import HttpRequestStage


class HttpRequestProcessorConfig(ProcessorConfig):
    """The configuration for the HTTP request processor.

    Examples:
        .. testcode::

            import ray
            from ray.data.llm import HttpRequestProcessorConfig, build_llm_processor

            config = HttpRequestProcessorConfig(
                url="https://api.openai.com/v1/chat/completions",
                headers={"Authorization": "Bearer sk-..."},
            )
            processor = build_llm_processor(
                config,
                preprocess=lambda row: dict(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a calculator"},
                        {"role": "user", "content": f"{row['id']} ** 3 = ?"},
                    ],
                    temperature=0.3,
                    max_tokens=20,
                ),
                postprocess=lambda row: dict(
                    resp=row["choices"][0]["message"]["content"],
                ),
                concurrency=1,
            )

            ds = ray.data.range(10)
            ds = processor(ds)
            for row in ds.take_all():
                print(row)

    Args:
        url: The base URL to send the request to.
        headers: The headers to send with the request. Note that the
            default headers ("Content-Type": "application/json") is always added, so
            you don't need to specify it here.
        qps: The maximum number of requests per second to avoid rate limit.
            If None, the request will be sent sequentially.
    """

    # The batch size.
    batch_size: int = 64
    # The URL to query.
    url: str
    # The query header. Note that we will add
    # "Content-Type: application/json" to be the heahder for sure
    # because we only deal with requests body in JSON.
    headers: Optional[Dict[str, Any]] = None
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
