import logging
from typing import Any, Dict, Optional

from ray._common.deprecation import Deprecated
from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.processor import (
    HttpRequestProcessorConfig as _HttpRequestProcessorConfig,
    Processor,
    ProcessorConfig as _ProcessorConfig,
    ServeDeploymentProcessorConfig as _ServeDeploymentProcessorConfig,
    SGLangEngineProcessorConfig as _SGLangEngineProcessorConfig,
    vLLMEngineProcessorConfig as _vLLMEngineProcessorConfig,
)
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig as _ChatTemplateStageConfig,
    DetokenizeStageConfig as _DetokenizeStageConfig,
    PrepareImageStageConfig as _PrepareImageStageConfig,
    PrepareMultimodalStageConfig as _PrepareMultimodalStageConfig,
    TokenizerStageConfig as _TokenizerStageConfig,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


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
            If ``concurrency`` is a ``tuple`` ``(m, n)``, Ray creates an autoscaling
            actor pool that scales between ``m`` and ``n`` workers (``1 <= m <= n``).
            If ``concurrency`` is an ``int`` ``n``, Ray uses either a fixed pool of ``n``
            workers or an autoscaling pool from ``1`` to ``n`` workers, depending on
            the processor and stage.
    """

    pass


@PublicAPI(stability="alpha")
class HttpRequestProcessorConfig(_HttpRequestProcessorConfig):
    """The configuration for the HTTP request processor.

    Args:
        batch_size: The batch size to send to the HTTP request.
        url: The URL to send the HTTP request to.
        headers: The headers to send with the HTTP request.
        concurrency: The number of concurrent requests to send. Default to 1.
            If ``concurrency`` is a ``tuple`` ``(m, n)``,
            autoscaling strategy is used (``1 <= m <= n``).

    Examples:
        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import HttpRequestProcessorConfig, build_processor

            config = HttpRequestProcessorConfig(
                url="https://api.openai.com/v1/chat/completions",
                headers={"Authorization": "Bearer sk-..."},
                concurrency=1,
            )
            processor = build_processor(
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
        should_continue_on_error: If True, continue processing when inference fails for a row
            instead of raising an exception. Failed rows will have a non-null
            ``__inference_error__`` column containing the error message, and other
            output columns will be None. Error rows bypass postprocess. If False
            (default), any inference error will raise an exception.
        chat_template_stage: Chat templating stage config (bool | dict | ChatTemplateStageConfig).
            Defaults to True. Use nested config for per-stage control over batch_size,
            concurrency, runtime_env, num_cpus, and memory. Legacy ``apply_chat_template``
            and ``chat_template`` fields are deprecated but still supported.
        tokenize_stage: Tokenizer stage config (bool | dict | TokenizerStageConfig).
            Defaults to True. Use nested config for per-stage control over batch_size,
            concurrency, runtime_env, num_cpus, memory, and model_source. Legacy
            ``tokenize`` field is deprecated but still supported.
        detokenize_stage: Detokenizer stage config (bool | dict | DetokenizeStageConfig).
            Defaults to True. Use nested config for per-stage control over batch_size,
            concurrency, runtime_env, num_cpus, memory, and model_source. Legacy
            ``detokenize`` field is deprecated but still supported.
        prepare_image_stage: Prepare image stage config (bool | dict | PrepareImageStageConfig).
            Defaults to False. Use nested config for per-stage control over batch_size,
            concurrency, runtime_env, num_cpus, and memory. Both the legacy ``has_image`` field
            and ``prepare_image_stage`` are deprecated but still supported. Prefer to use multimodal
            processor to process multimodal data instead.
        accelerator_type: The accelerator type used by the LLM stage in a processor.
            Default to None, meaning that only the CPU will be used.
        concurrency: The number of workers for data parallelism. Default to 1.
            If ``concurrency`` is a tuple ``(m, n)``, Ray creates an autoscaling
            actor pool that scales between ``m`` and ``n`` workers (``1 <= m <= n``).
            If ``concurrency`` is an ``int`` ``n``, CPU stages use an autoscaling
            pool from ``(1, n)``, while GPU stages use a fixed pool of ``n`` workers.
            Stage-specific concurrency can be set via nested stage configs.

    Examples:

        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import vLLMEngineProcessorConfig, build_processor

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
            processor = build_processor(
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
        batch_size: The batch size to send to the SGLang engine. Large batch sizes are
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
        chat_template_stage: Chat templating stage config (bool | dict | ChatTemplateStageConfig).
            Defaults to True. Use nested config for per-stage control over batch_size,
            concurrency, runtime_env, num_cpus, and memory. Legacy ``apply_chat_template``
            and ``chat_template`` fields are deprecated but still supported.
        tokenize_stage: Tokenizer stage config (bool | dict | TokenizerStageConfig).
            Defaults to True. Use nested config for per-stage control over batch_size,
            concurrency, runtime_env, num_cpus, memory, and model_source. Legacy
            ``tokenize`` field is deprecated but still supported.
        detokenize_stage: Detokenizer stage config (bool | dict | DetokenizeStageConfig).
            Defaults to True. Use nested config for per-stage control over batch_size,
            concurrency, runtime_env, num_cpus, memory, and model_source. Legacy
            ``detokenize`` field is deprecated but still supported.
        accelerator_type: The accelerator type used by the LLM stage in a processor.
            Default to None, meaning that only the CPU will be used.
        concurrency: The number of workers for data parallelism. Default to 1.
            If ``concurrency`` is a tuple ``(m, n)``, Ray creates an autoscaling
            actor pool that scales between ``m`` and ``n`` workers (``1 <= m <= n``).
            If ``concurrency`` is an ``int`` ``n``, CPU stages use an autoscaling
            pool from ``(1, n)``, while GPU stages use a fixed pool of ``n`` workers.
            Stage-specific concurrency can be set via nested stage configs.

    Examples:
        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import SGLangEngineProcessorConfig, build_processor

            config = SGLangEngineProcessorConfig(
                model_source="meta-llama/Meta-Llama-3.1-8B-Instruct",
                engine_kwargs=dict(
                    dtype="half",
                ),
                concurrency=1,
                batch_size=64,
            )
            processor = build_processor(
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
class ServeDeploymentProcessorConfig(_ServeDeploymentProcessorConfig):
    """The configuration for the serve deployment processor.

    This processor enables sharing serve deployments across multiple processors. This is useful
    for sharing the same LLM engine across multiple processors.

    Args:
        deployment_name: The name of the serve deployment to use.
        app_name: The name of the serve application to use.
        batch_size: The batch size to send to the serve deployment. Large batch sizes are
            likely to saturate the compute resources and could achieve higher throughput.
            On the other hand, small batch sizes are more fault-tolerant and could
            reduce bubbles in the data pipeline. You can tune the batch size to balance
            the throughput and fault-tolerance based on your use case.
        dtype_mapping: The mapping of the request class name to the request class. If this is
            not provided, the serve deployment is expected to accept a dict as the request.
        concurrency: The number of workers for data parallelism. Default to 1. Note that this is
            not the concurrency of the underlying serve deployment.

    Examples:

        .. testcode::
            :skipif: True

            import ray
            from ray import serve
            from ray.data.llm import ServeDeploymentProcessorConfig, build_processor
            from ray.serve.llm import (
                LLMConfig,
                ModelLoadingConfig,
                build_llm_deployment,
            )
            from ray.serve.llm.openai_api_models import CompletionRequest

            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    model_id="facebook/opt-1.3b",
                    model_source="facebook/opt-1.3b",
                ),
                accelerator_type="A10G",
                deployment_config=dict(
                    name="facebook",
                    autoscaling_config=dict(
                        min_replicas=1,
                        max_replicas=1,
                    ),
                ),
                engine_kwargs=dict(
                    enable_prefix_caching=True,
                    enable_chunked_prefill=True,
                    max_num_batched_tokens=4096,
                ),
            )

            APP_NAME = "facebook_opt_app"
            DEPLOYMENT_NAME = "facebook_deployment"
            override_serve_options = dict(name=DEPLOYMENT_NAME)

            llm_app = build_llm_deployment(
                llm_config, override_serve_options=override_serve_options
            )
            app = serve.run(llm_app, name=APP_NAME)

            config = ServeDeploymentProcessorConfig(
                deployment_name=DEPLOYMENT_NAME,
                app_name=APP_NAME,
                dtype_mapping={
                    "CompletionRequest": CompletionRequest,
                },
                concurrency=1,
                batch_size=64,
            )
            processor = build_processor(
                config,
                preprocess=lambda row: dict(
                    method="completions",
                    dtype="CompletionRequest",
                    request_kwargs=dict(
                        model="facebook/opt-1.3b",
                        prompt=f"This is a prompt for {row['id']}",
                        stream=False,
                    ),
                ),
                postprocess=lambda row: dict(
                    resp=row["choices"][0]["text"],
                ),
            )

            # The processor requires specific input columns, which depend on
            # your processor config. You can use the following API to check
            # the required input columns:
            processor.log_input_column_names()

            ds = ray.data.range(10)
            ds = processor(ds)
            for row in ds.take_all():
                print(row)
    """

    pass


@PublicAPI(stability="alpha")
class ChatTemplateStageConfig(_ChatTemplateStageConfig):
    """The configuration for the chat template stage.

    Args:
        enabled: Whether this stage is enabled. Defaults to True.
        model_source: Model source/identifier for this stage. If not specified,
            will use the processor-level model_source.
        chat_template: The chat template in Jinja template format. This is
            usually not needed if the model checkpoint already contains the
            chat template.
        chat_template_kwargs: Optional kwargs to pass to apply_chat_template.
        batch_size: Rows per batch. If not specified, will use the processor-level
            batch_size.
        concurrency: Actor pool size or range for this stage. If not specified,
            will use the processor-level concurrency. If ``concurrency`` is a
            tuple ``(m, n)``, Ray creates an autoscaling actor pool that scales
            between ``m`` and ``n`` workers (``1 <= m <= n``). If ``concurrency``
            is an ``int`` ``n``, CPU stages use an autoscaling pool from ``(1, n)``.
        runtime_env: Optional runtime environment for this stage. If not specified,
            will use the processor-level runtime_env. See
            :ref:`this doc <handling_dependencies>` for more details.
        num_cpus: Number of CPUs to reserve for each map worker in this stage.
        memory: Heap memory in bytes to reserve for each map worker in this stage.
    """

    pass


@PublicAPI(stability="alpha")
class DetokenizeStageConfig(_DetokenizeStageConfig):
    """The configuration for the detokenize stage.

    Args:
        enabled: Whether this stage is enabled. Defaults to True.
        model_source: Model source/identifier for this stage. If not specified,
            will use the processor-level model_source.
        batch_size: Rows per batch. If not specified, will use the processor-level
            batch_size.
        concurrency: Actor pool size or range for this stage. If not specified,
            will use the processor-level concurrency. If ``concurrency`` is a
            tuple ``(m, n)``, Ray creates an autoscaling actor pool that scales
            between ``m`` and ``n`` workers (``1 <= m <= n``). If ``concurrency``
            is an ``int`` ``n``, CPU stages use an autoscaling pool from ``(1, n)``.
        runtime_env: Optional runtime environment for this stage. If not specified,
            will use the processor-level runtime_env. See
            :ref:`this doc <handling_dependencies>` for more details.
        num_cpus: Number of CPUs to reserve for each map worker in this stage.
        memory: Heap memory in bytes to reserve for each map worker in this stage.
    """

    pass


@PublicAPI(stability="alpha")
class PrepareMultimodalStageConfig(_PrepareMultimodalStageConfig):
    """The configuration for the prepare multimodal stage.

    Args:
        enabled: Whether this stage is enabled. Defaults to True.
        model_source: Name or path of the Hugging Face model to use for the
            multimodal processor. This is required to process multimodal data
            according to a specific model. If not specified, will use the
            processor-level model_source.
        chat_template_content_format: The content format to use for the chat
            template. This is used to format the chat template content according
            to a specific model. Choices are "string" or "openai". Defaults to
            "string".
        apply_sys_msg_formatting: Whether to apply formatting system messages.
            Defaults to False.
        batch_size: Rows per batch. If not specified, will use the processor-level
            batch_size.
        concurrency: Actor pool size or range for this stage. If not specified,
            will use the processor-level concurrency. If ``concurrency`` is a
            tuple ``(m, n)``, Ray creates an autoscaling actor pool that scales
            between ``m`` and ``n`` workers (``1 <= m <= n``). If ``concurrency``
            is an ``int`` ``n``, CPU stages use an autoscaling pool from ``(1, n)``.
        runtime_env: Optional runtime environment for this stage. If not specified,
            will use the processor-level runtime_env. See
            :ref:`this doc <handling_dependencies>` for more details.
        num_cpus: Number of CPUs to reserve for each map worker in this stage.
        memory: Heap memory in bytes to reserve for each map worker in this stage.
    """

    pass


@PublicAPI(stability="alpha")
class TokenizerStageConfig(_TokenizerStageConfig):
    """The configuration for the tokenizer stage.

    Args:
        enabled: Whether this stage is enabled. Defaults to True.
        model_source: Model source/identifier for this stage. If not specified,
            will use the processor-level model_source.
        batch_size: Rows per batch. If not specified, will use the processor-level
            batch_size.
        concurrency: Actor pool size or range for this stage. If not specified,
            will use the processor-level concurrency. If ``concurrency`` is a
            tuple ``(m, n)``, Ray creates an autoscaling actor pool that scales
            between ``m`` and ``n`` workers (``1 <= m <= n``). If ``concurrency``
            is an ``int`` ``n``, CPU stages use an autoscaling pool from ``(1, n)``.
        runtime_env: Optional runtime environment for this stage. If not specified,
            will use the processor-level runtime_env. See
            :ref:`this doc <handling_dependencies>` for more details.
        num_cpus: Number of CPUs to reserve for each map worker in this stage.
        memory: Heap memory in bytes to reserve for each map worker in this stage.
    """

    pass


@PublicAPI(stability="alpha")
class PrepareImageStageConfig(_PrepareImageStageConfig):
    """The configuration for the prepare image stage.

    Args:
        enabled: Whether this stage is enabled. Defaults to True.
        batch_size: Rows per batch. If not specified, will use the processor-level
            batch_size.
        concurrency: Actor pool size or range for this stage. If not specified,
            will use the processor-level concurrency. If ``concurrency`` is a
            tuple ``(m, n)``, Ray creates an autoscaling actor pool that scales
            between ``m`` and ``n`` workers (``1 <= m <= n``). If ``concurrency``
            is an ``int`` ``n``, CPU stages use an autoscaling pool from ``(1, n)``.
        runtime_env: Optional runtime environment for this stage. If not specified,
            will use the processor-level runtime_env. See
            :ref:`this doc <handling_dependencies>` for more details.
        num_cpus: Number of CPUs to reserve for each map worker in this stage.
        memory: Heap memory in bytes to reserve for each map worker in this stage.
    """

    pass


@Deprecated(new="build_processor", error=False)
def build_llm_processor(
    config: ProcessorConfig,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
    preprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    postprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    builder_kwargs: Optional[Dict[str, Any]] = None,
) -> Processor:
    """
    [DEPRECATED] Prefer build_processor. Build a LLM processor using the given config.
    """
    return build_processor(
        config,
        preprocess,
        postprocess,
        preprocess_map_kwargs,
        postprocess_map_kwargs,
        builder_kwargs,
    )


@PublicAPI(stability="alpha")
def build_processor(
    config: ProcessorConfig,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
    preprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    postprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    builder_kwargs: Optional[Dict[str, Any]] = None,
) -> Processor:
    """Build a processor using the given config.

    Args:
        config: The processor config. Supports nested stage configs for per-stage
            control over batch_size, concurrency, runtime_env, num_cpus, and memory
            (e.g., ``chat_template_stage=ChatTemplateStageConfig(batch_size=128)``
            or ``tokenize_stage={"batch_size": 256, "concurrency": 2}``). Legacy
            boolean flags (``apply_chat_template``, ``tokenize``, ``detokenize``,
            ``has_image``) are deprecated but still supported with deprecation warnings.
        preprocess: An optional lambda function that takes a row (dict) as input
            and returns a preprocessed row (dict). The output row must contain the
            required fields for the following processing stages. Each row
            can contain a `sampling_params` field which will be used by the
            engine for row-specific sampling parameters.
            Note that all columns will be carried over until the postprocess stage.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict). To keep all the original columns,
            you can use the `**row` syntax to return all the original columns.
        preprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            preprocess stage. Useful for controlling resources (e.g., num_cpus=0.5)
            and concurrency independently of the main LLM stage.
        postprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            postprocess stage. Useful for controlling resources (e.g., num_cpus=0.25)
            and concurrency independently of the main LLM stage.
        builder_kwargs: Optional additional kwargs to pass to the processor builder
            function. These will be passed through to the registered builder and
            should match the signature of the specific builder being used.
            For example, vLLM and SGLang processors support `chat_template_kwargs`.

    Returns:
        The built processor.

    Examples:
        Basic usage:

        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import vLLMEngineProcessorConfig, build_processor

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

            processor = build_processor(
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

        Using map_kwargs to control preprocess/postprocess resources:

        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import vLLMEngineProcessorConfig, build_processor

            config = vLLMEngineProcessorConfig(
                model_source="meta-llama/Meta-Llama-3.1-8B-Instruct",
                concurrency=1,
                batch_size=64,
            )

            processor = build_processor(
                config,
                preprocess=lambda row: dict(
                    messages=[{"role": "user", "content": row["prompt"]}],
                    sampling_params=dict(temperature=0.3, max_tokens=20),
                ),
                postprocess=lambda row: dict(resp=row["generated_text"]),
                preprocess_map_kwargs={"num_cpus": 0.5},
                postprocess_map_kwargs={"num_cpus": 0.25},
            )

            ds = ray.data.range(300)
            ds = processor(ds)
            for row in ds.take_all():
                print(row)

        Using builder_kwargs to pass chat_template_kwargs:

        .. testcode::
            :skipif: True

            import ray
            from ray.data.llm import vLLMEngineProcessorConfig, build_processor

            config = vLLMEngineProcessorConfig(
                model_source="Qwen/Qwen3-0.6B",
                chat_template_stage={"enabled": True},
                concurrency=1,
                batch_size=64,
            )

            processor = build_processor(
                config,
                preprocess=lambda row: dict(
                    messages=[
                        {"role": "user", "content": row["prompt"]},
                    ],
                    sampling_params=dict(
                        temperature=0.6,
                        max_tokens=100,
                    ),
                ),
                builder_kwargs=dict(
                    chat_template_kwargs={"enable_thinking": True},
                ),
            )

            ds = ray.data.from_items([{"prompt": "What is 2+2?"}])
            ds = processor(ds)
            for row in ds.take_all():
                print(row)
    """
    from ray.llm._internal.batch.processor import ProcessorBuilder

    ProcessorBuilder.validate_builder_kwargs(builder_kwargs)

    build_kwargs = dict(
        preprocess=preprocess,
        postprocess=postprocess,
        preprocess_map_kwargs=preprocess_map_kwargs,
        postprocess_map_kwargs=postprocess_map_kwargs,
    )

    # Pass through any additional builder kwargs
    if builder_kwargs is not None:
        build_kwargs.update(builder_kwargs)

    return ProcessorBuilder.build(config, **build_kwargs)


__all__ = [
    "ProcessorConfig",
    "Processor",
    "HttpRequestProcessorConfig",
    "vLLMEngineProcessorConfig",
    "SGLangEngineProcessorConfig",
    "ServeDeploymentProcessorConfig",
    "ChatTemplateStageConfig",
    "DetokenizeStageConfig",
    "PrepareMultimodalStageConfig",
    "TokenizerStageConfig",
    "PrepareImageStageConfig",
    "build_llm_processor",
    "build_processor",
]
