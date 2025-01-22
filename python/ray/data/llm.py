from ray.llm._internal.batch.processor import (
    ProcessorConfig,
    Processor,
    HttpRequestProcessorConfig,
)
from ray.util.annotations import PublicAPI


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
