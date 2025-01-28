from ray.llm._internal.batch.processor import (
    ProcessorConfig as _ProcessorConfig,
    Processor,
    HttpRequestProcessorConfig as _HttpRequestProcessorConfig,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ProcessorConfig(_ProcessorConfig):
    """The processor configuration."""

    pass


@PublicAPI(stability="alpha")
class HttpRequestProcessorConfig(_HttpRequestProcessorConfig):
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
    """

    pass


@PublicAPI(stability="alpha")
def build_llm_processor(config: ProcessorConfig, **kwargs) -> Processor:
    """Build a LLM processor using the given config.

    Args:
        config: The processor config.
        **kwargs: Additional keyword arguments to pass to the processor.
            See `Processor` for argument details.

    Returns:
        The built processor.
    """
    from ray.llm._internal.batch.processor import ProcessorBuilder

    return ProcessorBuilder.build(config, **kwargs)


__all__ = [
    "ProcessorConfig",
    "Processor",
    "HttpRequestProcessorConfig",
    "build_llm_processor",
]
