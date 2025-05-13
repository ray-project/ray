from typing import Optional

from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.processor import (
    HttpRequestProcessorConfig as _HttpRequestProcessorConfig,
    Processor,
    ProcessorConfig as _ProcessorConfig,
    SGLangEngineProcessorConfig as _SGLangEngineProcessorConfig,
    vLLMEngineProcessorConfig as _vLLMEngineProcessorConfig,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ProcessorConfig(_ProcessorConfig):
    """The processor configuration.

    Args:
        batch_size: Configures batch size for the processor. Large batch sizes are
            likely to saturate the compute resources and could achieve higher throughput.
            On the other hand, small batch sizes are more fault-tolerant and could
            reduce bubbles in the data pipeline. You can tune the batch size to balance
            the throughput and fault-tolerance based on your use case.
        resources_per_bundle: The resource bundles for placement groups.
            You can specify a custom device label e.g. {'NPU': 1}.
            The default resource bundle for LLM Stage is always a GPU resource i.e. {'GPU': 1}.
        accelerator_type: The accelerator type used by the LLM stage in a processor.
            Default to None, meaning that only the CPU will be used.
        concurrency: The number of workers for data parallelism. Default to 1.
    """

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
                    payload=dict(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": "You are a calculator"},
                            {"role": "user", "content": f"{row['id']} ** 3 = ?"},
                        ],
                        temperature=0.3,
                        max_tokens=20,
                    ),
                ),
                postprocess=lambda row: dict(
                    resp=row["http_response"]["choices"][0]["message"]["content"],
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
        model_source: The model source to use for the vLLM engine.
        batch_size: The batch size to send to the vLLM engine. Large batch sizes are
            likely to saturate the compute resources and could achieve higher throughput.
            On the other hand, small batch sizes are more fault-tolerant and could
            reduce bubbles in the data pipeline. You can tune the batch size to balance
            the throughput and fault-tolerance based on your use case.
        engine_kwargs: The kwargs to pass to the vLLM engine. Default engine kwargs are
            pipeline_parallel_size: 1, tensor_parallel_size: 1, max_num_seqs: 128,
            distributed_executor_backend: "mp".
        task_type: The task type to use. If not specified, will use 'generate' by default.
        runtime_env: The runtime environment to use for the vLLM engine. See
            :ref:`this doc <handling_dependencies>` for more details.
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
        accelerator_type: The accelerator type used by the LLM stage in a processor.
            Default to None, meaning that only the CPU will be used.
        concurrency: The number of workers for data parallelism. Default to 1.

    Examples:

        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

            config = vLLMEngineProcessorConfig(
                model_source="meta-llama/Meta-Llama-3.1-8B-Instruct",
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

            # The processor requires specific input columns, which depend on
            # your processor config. You can use the following API to check
            # the required input columns:
            processor.log_input_column_names()
            # Example log:
            # The first stage of the processor is ChatTemplateStage.
            # Required input columns:
            #     messages: A list of messages in OpenAI chat format.

            ds = ray.data.range(300)
            ds = processor(ds)
            for row in ds.take_all():
                print(row)
    """

    pass


@PublicAPI(stability="alpha")
class SGLangEngineProcessorConfig(_SGLangEngineProcessorConfig):
    """The configuration for the SGLang engine processor.

    Args:
        model_source: The model source to use for the SGLang engine.
        batch_size: The batch size to send to the vLLM engine. Large batch sizes are
            likely to saturate the compute resources and could achieve higher throughput.
            On the other hand, small batch sizes are more fault-tolerant and could
            reduce bubbles in the data pipeline. You can tune the batch size to balance
            the throughput and fault-tolerance based on your use case.
        engine_kwargs: The kwargs to pass to the SGLang engine. Default engine kwargs are
            tp_size: 1, dp_size: 1, skip_tokenizer_init: True.
        task_type: The task type to use. If not specified, will use 'generate' by default.
        runtime_env: The runtime environment to use for the SGLang engine. See
            :ref:`this doc <handling_dependencies>` for more details.
        max_pending_requests: The maximum number of pending requests. If not specified,
            will use the default value from the SGLang engine.
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
        accelerator_type: The accelerator type used by the LLM stage in a processor.
            Default to None, meaning that only the CPU will be used.
        concurrency: The number of workers for data parallelism. Default to 1.

    Examples:
        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import SGLangEngineProcessorConfig, build_llm_processor

            config = SGLangEngineProcessorConfig(
                model_source="meta-llama/Meta-Llama-3.1-8B-Instruct",
                engine_kwargs=dict(
                    dtype="half",
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
                        max_new_tokens=20,
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
            required fields for the following processing stages. Each row
            can contain a `sampling_params` field which will be used by the
            engine for row-specific sampling parameters.
            Note that all columns will be carried over until the postprocess stage.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict). To keep all the original columns,
            you can use the `**row` syntax to return all the original columns.

    Returns:
        The built processor.

    Example:
        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

            config = vLLMEngineProcessorConfig(
                model_source="meta-llama/Meta-Llama-3.1-8B-Instruct",
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
                    **row,  # This will return all the original columns in the dataset.
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
    "SGLangEngineProcessorConfig",
    "build_llm_processor",
]
