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

    Args:
        batch_size: The batch size to send to the HTTP request.
        url: The URL to send the HTTP request to.
        headers: The headers to send with the HTTP request.
        concurrency: The number of concurrent requests to send.

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

    Args:
        model: The model to use for the vLLM engine.
        engine_kwargs: The kwargs to pass to the vLLM engine.
        task_type: The task type to use. If not specified, will use 'generate' by default.
        runtime_env: The runtime environment to use for the vLLM engine.
        max_pending_requests: The maximum number of pending requests. If not specified,
            will use the default value from the vLLM engine.
        max_concurrent_batches: The maximum number of concurrent batches in the engine.
            This is to overlap the batch processing to avoid the tail latency of
            each batch. The default value may not be optimal when the batch size
            or the batch processing latency is too small, but it should be good
            enough for batch size >= 64.
        apply_chat_template: Whether to apply chat template.
        chat_template: The chat template to use. This is usually not needed if the
            model checkpoint already contains the chat template.
        tokenize: Whether to tokenize the input before passing it to the vLLM engine.
            If not, vLLM will tokenize the prompt in the engine.
        detokenize: Whether to detokenize the output.
        has_image: Whether the input messages have images.

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

    Example:
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
