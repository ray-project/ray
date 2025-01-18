from ray.llm._internal.batch.processor import (
    ProcessorConfig as _ProcessorConfig,
    Processor as _Processor,
    HttpRequestProcessorConfig as _HttpRequestProcessorConfig,
    vLLMEngineProcessorConfig as _vLLMEngineProcessorConfig,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ProcessorConfig(_ProcessorConfig):
    """TBA."""

    pass


@PublicAPI(stability="alpha")
class Processor(_Processor):
    """A Processor is ... TBA ...

    Examples:
        .. testcode::
            :skipif: True

            ... TBA ...
    """

    pass


@PublicAPI(stability="alpha")
def build_llm_processor(config: ProcessorConfig, **kwargs) -> Processor:
    """Build a LLM processor.

    Args:
        config: The processor config.
        **kwargs: Additional keyword arguments to pass to the processor builder.

    Returns:
        The built processor.
    """
    from ray.llm._internal.batch.processor import ProcessorBuilder

    return ProcessorBuilder.build(config, **kwargs)


@PublicAPI(stability="alpha")
class HttpRequestProcessorConfig(_HttpRequestProcessorConfig):
    """TBA."""

    pass


@PublicAPI(stability="alpha")
class vLLMEngineProcessorConfig(_vLLMEngineProcessorConfig):
    """TBA."""

    pass
