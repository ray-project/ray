from typing import Optional
from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.processor import (
    ProcessorConfig as _ProcessorConfig,
    Processor,
    HttpRequestProcessorConfig as _HttpRequestProcessorConfig,
    vLLMEngineProcessorConfig as _vLLMEngineProcessorConfig,
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
            :skipif: True

            import ray
            from ray.data.llm import HttpRequestProcessorConfig, build_llm_processor

            config = HttpRequestProcessorConfig(
                url="https://api.openai.com/v1/chat/completions",
                headers={"Authorization": "Bearer sk-..."},
                concurrency=1,
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
            )

            ds = ray.data.range(10)
            ds = processor(ds)
            for row in ds.take_all():
                print(row)
    """

    pass


@PublicAPI(stability="alpha")
class vLLMEngineProcessorConfig(_vLLMEngineProcessorConfig):
    """The configuration for the vLLM engine processor.

    Examples:
        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

            config = vLLMEngineProcessorConfig(
                model="meta-llama/Meta-Llama-3.1-8B-Instruct",
                engine_kwargs=dict(
                    enable_prefix_caching=True,
                    enable_chunked_prefill=True,
                    max_num_batched_tokens=4096,
                ),
                accelerator_type="L4",
                concurrency=1,
                batch_size=64,
            )
            processor = build_llm_processor(
                config,
                preprocess=lambda row: dict(
                    messages=[
                        {"role": "system", "content": "You are a calculator"},
                        {"role": "user", "content": f"{row['id']} ** 3 = ?"},
                    ],
                    sampling_params=dict(
                        temperature=0.3,
                        max_tokens=20,
                        detokenize=False,
                    ),
                ),
                postprocess=lambda row: dict(
                    resp=row["generated_text"],
                ),
            )

            ds = ray.data.range(300)
            ds = processor(ds)
            for row in ds.take_all():
                print(row)
    """

    pass


@PublicAPI(stability="alpha")
def build_llm_processor(
    config: ProcessorConfig,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
) -> Processor:
    """Build a LLM processor using the given config.

    Args:
        config: The processor config.
        preprocess: An optional lambda function that takes a row (dict) as input
            and returns a preprocessed row (dict). The output row must contain the
            required fields for the following processing stages.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict).

    Returns:
        The built processor.
    """
    from ray.llm._internal.batch.processor import ProcessorBuilder

    return ProcessorBuilder.build(
        config,
        preprocess=preprocess,
        postprocess=postprocess,
    )


__all__ = [
    "ProcessorConfig",
    "Processor",
    "HttpRequestProcessorConfig",
    "vLLMEngineProcessorConfig",
    "build_llm_processor",
]
