import asyncio
import os
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Dict, Optional, Type, Union

# Third-party imports
from ray import serve
from ray._common.utils import import_attr

# Local imports
from ray.llm._internal.serve.configs.constants import (
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    ENGINE_START_TIMEOUT_S,
    MODEL_RESPONSE_BATCH_TIMEOUT_MS,
    RAYLLM_VLLM_ENGINE_CLS_ENV,
)
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionLogProb,
    ChatCompletionLogProbs,
    ChatCompletionLogProbsContent,
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionResponseChoice,
    ChatCompletionResponseStreamChoice,
    ChatCompletionStreamResponse,
    ChatMessage,
    CompletionRequest,
    CompletionResponse,
    CompletionResponseChoice,
    CompletionResponseStreamChoice,
    CompletionStreamResponse,
    DeltaMessage,
    EmbeddingRequest,
    EmbeddingResponse,
    EmbeddingResponseData,
    LLMChatResponse,
    LLMCompletionsResponse,
    LLMEmbeddingsResponse,
    UsageInfo,
)
from ray.llm._internal.serve.configs.prompt_formats import Message, Prompt
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
    LLMRawResponse,
)
from ray.llm._internal.serve.deployments.llm.llm_engine import LLMEngine
from ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader import (
    LoraModelLoader,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import VLLMEngine
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMEmbeddingRequest,
)
from ray.llm._internal.serve.deployments.utils.batcher import OpenAIResponseBatcher
from ray.llm._internal.serve.deployments.utils.error_handling_utils import (
    StreamingErrorHandler,
)
from ray.llm._internal.serve.deployments.utils.server_utils import (
    get_model_request_id,
    get_response_for_error,
    get_serve_request_id,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.usage_telemetry.usage import (
    push_telemetry_report_for_all_models,
)

logger = get_logger(__name__)


class _LLMServerBase(ABC):
    """
    This is the common interface between all the llm deployment. All llm deployments
    need to implement an async constructor, an async predict, and check_health method.
    """

    # TODO (Kourosh): I don't know why this is an async init. Need to fix.
    async def __init__(self, llm_config: LLMConfig):
        """
        Constructor takes in an LLMConfig object and start the underlying engine.
        """
        self._llm_config = llm_config

    @abstractmethod
    async def chat(self, request: ChatCompletionRequest) -> LLMChatResponse:
        """
        Inferencing to the engine for chat, and return the response as LLMChatResponse.
        """
        ...

    @abstractmethod
    async def completions(self, request: CompletionRequest) -> LLMCompletionsResponse:
        """
        Inferencing to the engine for completion api, and return the response as LLMCompletionsResponse.
        """
        ...

    @abstractmethod
    async def check_health(self) -> None:
        """
        Check the health of the replica. Does not return anything. Raise error when
        the engine is dead and needs to be restarted.
        """
        ...

    async def llm_config(self) -> LLMConfig:
        return self._llm_config


class ResponsePostprocessor:
    """Processes raw LLM responses into OpenAI-compatible formats.

    This class handles:
    1. Error handling for the response stream
    2. Converting LLMRawResponse to Chat/Completion API formats
    3. Supporting both streaming and non-streaming responses
    """

    def __init__(self):
        self.metrics_wrapper = StreamingErrorHandler()

    async def handle_failure(
        self, model: str, gen: AsyncGenerator[LLMRawResponse, None]
    ) -> AsyncGenerator[LLMRawResponse, None]:
        async for llm_response in self.metrics_wrapper.handle_failure(model, gen):
            yield llm_response

    @staticmethod
    async def merge_stream(
        response_stream: AsyncGenerator[LLMRawResponse, None]
    ) -> LLMRawResponse:
        responses = [resp async for resp in response_stream]
        return LLMRawResponse.merge_stream(*responses)

    async def process_chat(
        self, model: str, gen: AsyncGenerator[LLMRawResponse, None], stream: bool
    ) -> LLMChatResponse:
        """Process raw LLM responses into chat completion format."""
        gen = self.handle_failure(model=model, gen=gen)
        request_id = get_serve_request_id()
        completion_id = get_model_request_id(model)

        if stream:
            # Stream processing - preserve batching from generator
            yielded_role = False
            all_results = []
            try:
                async for batched_results in gen:

                    for result in batched_results.unpack():
                        all_results.append(result)

                        # Handle errors
                        if result.error:
                            logger.error(f"{result.error}")
                            # Drop finish reason as OpenAI doesn't expect it for errors
                            result.finish_reason = None
                            all_results.pop()
                            yield result.error
                            return

                        finish_reason = result.finish_reason

                        # Send role message first
                        if not yielded_role:
                            yield ChatCompletionStreamResponse(
                                id=completion_id,
                                model=model,
                                choices=[
                                    ChatCompletionResponseStreamChoice(
                                        delta=DeltaMessage(role="assistant"),
                                        index=0,
                                        finish_reason=None,
                                        logprobs=ChatCompletionLogProbs(content=[]),
                                    )
                                ],
                                usage=None,
                            )
                            yielded_role = True

                        # Process logprobs if present
                        logprobs = None
                        if result.logprobs:
                            logprobs = ChatCompletionLogProbs(
                                content=[
                                    ChatCompletionLogProbsContent(
                                        token=logprobs.token,
                                        logprob=logprobs.logprob,
                                        bytes=logprobs.bytes,
                                        top_logprobs=[
                                            ChatCompletionLogProb(
                                                token=logprob.token,
                                                logprob=logprob.logprob,
                                                bytes=logprob.bytes,
                                            )
                                            for logprob in logprobs.top_logprobs
                                        ],
                                    )
                                    for logprobs in result.logprobs
                                ]
                            )

                        yield ChatCompletionStreamResponse(
                            id=completion_id,
                            model=model,
                            choices=[
                                ChatCompletionResponseStreamChoice(
                                    delta=DeltaMessage(
                                        content=result.generated_text or ""
                                    ),
                                    index=0,
                                    finish_reason=None,
                                    logprobs=logprobs,
                                )
                            ],
                            usage=None,
                        )

                # Send final message with finish_reason if there were any results
                # TODO (Kourosh): Doing this much for the last token
                # (usage token) might add extra overhead to ITL of the last token.
                # We should find a better way to do this.
                if all_results:
                    merged_results = LLMRawResponse.merge_stream(*all_results)
                    finish_reason = merged_results.finish_reason
                    usage = UsageInfo(
                        prompt_tokens=merged_results.num_input_tokens or 0,
                        completion_tokens=merged_results.num_generated_tokens or 0,
                        total_tokens=(merged_results.num_input_tokens or 0)
                        + (merged_results.num_generated_tokens or 0),
                    )

                    yield ChatCompletionStreamResponse(
                        id=completion_id,
                        model=model,
                        choices=[
                            ChatCompletionResponseStreamChoice(
                                delta=DeltaMessage(),
                                index=0,
                                finish_reason=finish_reason,
                            )
                        ],
                        usage=usage,
                    )
            except Exception as e:
                logger.error(
                    f"Failed while handling chat-completions for request ({request_id}): {repr(e)}",
                    exc_info=e,
                )
                yield get_response_for_error(e, request_id).error
        else:
            # Non-streaming processing - merge and return a single response
            try:
                results: LLMRawResponse = await self.merge_stream(gen)
                if results.error:
                    yield results.error
                    return

                logprobs = None
                if results.logprobs:
                    logprobs = ChatCompletionLogProbs(
                        content=[
                            ChatCompletionLogProbsContent(
                                token=logprobs.token,
                                logprob=logprobs.logprob,
                                bytes=logprobs.bytes,
                                top_logprobs=[
                                    ChatCompletionLogProb(
                                        token=logprob.token,
                                        logprob=logprob.logprob,
                                        bytes=logprob.bytes,
                                    )
                                    for logprob in logprobs.top_logprobs
                                ],
                            )
                            for logprobs in results.logprobs
                        ]
                    )

                yield ChatCompletionResponse(
                    id=completion_id,
                    model=model,
                    choices=[
                        ChatCompletionResponseChoice(
                            message=ChatMessage(
                                role="assistant",
                                content=results.generated_text or "",
                            ),
                            index=0,
                            finish_reason=results.finish_reason,
                            logprobs=logprobs,
                        )
                    ],
                    usage=UsageInfo(
                        prompt_tokens=results.num_input_tokens or 0,
                        completion_tokens=results.num_generated_tokens or 0,
                        total_tokens=(results.num_input_tokens or 0)
                        + (results.num_generated_tokens or 0),
                    ),
                )
            except Exception as e:
                logger.error(
                    f"Failed while handling chat-completions for request ({request_id}): {repr(e)}",
                    exc_info=e,
                )
                yield get_response_for_error(e, request_id).error

    async def process_completions(
        self, model: str, gen: AsyncGenerator[LLMRawResponse, None], stream: bool
    ) -> LLMCompletionsResponse:
        """Process raw LLM responses into completions format."""
        gen = self.handle_failure(model=model, gen=gen)
        request_id = get_serve_request_id()
        completion_id = get_model_request_id(model)

        if stream:
            # Stream processing - preserve batching from generator
            all_results = []
            try:
                async for batched_results in gen:

                    for result in batched_results.unpack():
                        all_results.append(result)

                        # Handle errors
                        if result.error:
                            # Drop finish reason as OpenAI doesn't expect it for errors
                            result.finish_reason = None
                            logger.error(
                                f"Reporting back an error: {result.error}",
                                extra={
                                    "ray_serve_extra_fields": {"response": str(result)}
                                },
                            )
                            all_results.pop()
                            yield result.error
                            return

                        # Calculate usage if finished
                        usage = None
                        if result.finish_reason:
                            merged_results = LLMRawResponse.merge_stream(*all_results)
                            usage = UsageInfo(
                                prompt_tokens=merged_results.num_input_tokens or 0,
                                completion_tokens=merged_results.num_generated_tokens
                                or 0,
                                total_tokens=(merged_results.num_input_tokens or 0)
                                + (merged_results.num_generated_tokens or 0),
                            )

                        chunk = CompletionStreamResponse(
                            id=completion_id,
                            model=model,
                            choices=[
                                CompletionResponseStreamChoice(
                                    text=result.generated_text or "",
                                    index=0,
                                    logprobs={},
                                    finish_reason=result.finish_reason,
                                )
                            ],
                            usage=usage,
                        )

                        yield chunk

            except Exception as e:
                logger.error(
                    f"Failed while handling completions for request ({request_id}): {repr(e)}",
                    exc_info=e,
                )
                yield get_response_for_error(e, request_id).error
        else:
            # Non-streaming processing - merge and return a single response
            try:
                results: LLMRawResponse = await self.merge_stream(gen)
                if results.error:
                    yield results.error
                    return

                yield CompletionResponse(
                    id=completion_id,
                    model=model,
                    choices=[
                        CompletionResponseChoice(
                            text=results.generated_text or "",
                            index=0,
                            logprobs={},
                            finish_reason=results.finish_reason,
                        )
                    ],
                    usage=UsageInfo(
                        prompt_tokens=results.num_input_tokens or 0,
                        completion_tokens=results.num_generated_tokens or 0,
                        total_tokens=(results.num_input_tokens or 0)
                        + (results.num_generated_tokens or 0),
                    ),
                )
            except Exception as e:
                logger.error(
                    f"Failed while handling completions for request ({request_id}): {repr(e)}",
                    exc_info=e,
                )
                yield get_response_for_error(e, request_id).error


class LLMServer(_LLMServerBase):
    _default_engine_cls = VLLMEngine

    async def __init__(
        self,
        llm_config: LLMConfig,
        *,
        engine_cls: Optional[Type[LLMEngine]] = None,
        model_downloader: Optional[LoraModelLoader] = None,
    ):
        """Constructor of LLMServer.

        Only the llm_config is public api, the other arguments are private
        and used for testing.

        Args:
            llm_config: LLMConfig for the model.
            engine_cls: Dependency injection for the vllm engine class.
                Defaults to `VLLMEngine`.
            model_downloader: Dependency injection for the model downloader
                object. Defaults to be initialized with `LoraModelLoader`.
        """
        await super().__init__(llm_config)

        self._engine_cls = engine_cls or self._get_default_engine_class()
        self.engine: Optional[LLMEngine] = None
        if self._engine_cls is not None:
            self.engine = self._engine_cls(self._llm_config)
            await asyncio.wait_for(self._start_engine(), timeout=ENGINE_START_TIMEOUT_S)

        multiplex_config = self._llm_config.multiplex_config()
        if model_downloader:
            self.model_downloader = model_downloader
        elif multiplex_config:
            self.model_downloader = LoraModelLoader(
                download_timeout_s=multiplex_config.download_timeout_s,
                max_tries=multiplex_config.max_download_tries,
            )
        else:
            self.model_downloader = LoraModelLoader()

        # Hack that lets us set max_num_models_per_replica from the llm_config
        if multiplex_config:
            self.load_model = serve.multiplexed(
                max_num_models_per_replica=multiplex_config.max_num_models_per_replica
            )(lambda lora_model_id: self._load_model(lora_model_id))

        self.response_postprocessor = ResponsePostprocessor()

    def _get_default_engine_class(self) -> Type[LLMEngine]:
        """Helper to load the engine class from the environment variable.
        This is used for testing or escape-hatch for patching purposes.
        If env variable is not set, it will fallback to the default engine class.
        """
        engine_cls_path = os.environ.get(RAYLLM_VLLM_ENGINE_CLS_ENV)
        if engine_cls_path:
            return import_attr(engine_cls_path)
        return self._default_engine_cls

    async def _start_engine(self):
        if self.engine is None:
            raise ValueError("Engine is not set")

        await self.engine.start()

        # Push telemetry reports for the model in the current deployment.
        push_telemetry_report_for_all_models(all_models=[self._llm_config])

    async def _predict(
        self,
        request_id: str,
        prompt: Prompt,
        stream: bool,
    ) -> AsyncGenerator[LLMRawResponse, None]:
        """A thin wrapper around VLLMEngine.generate().

        1. Load the model to disk
        2. Format parameters correctly
        3. Forward request to VLLMEngine.generate()
        """

        logger.info(f"Received streaming request {request_id}")
        multiplexed_model_id = serve.get_multiplexed_model_id()

        if multiplexed_model_id:
            assert (
                self._llm_config.lora_config is not None
            ), "Must setup lora config for multiplexed requests."
            disk_lora_model = await self._disk_lora_model(multiplexed_model_id)
        else:
            disk_lora_model = None

        llm_request = await self.engine.prepare_request(
            request_id=request_id,
            prompt=prompt,
            stream=stream,
            disk_lora_model=disk_lora_model,
        )

        async for llm_response in self.engine.generate(llm_request):
            yield llm_response

    def _get_batch_interval_ms(self, stream: bool = True) -> int:
        """Calculate the batching interval for responses."""
        stream_batching_interval_ms = self._llm_config.experimental_configs.get(
            "stream_batching_interval_ms"
        )
        if stream_batching_interval_ms is None:
            stream_batching_interval_ms = MODEL_RESPONSE_BATCH_TIMEOUT_MS
        return stream_batching_interval_ms if stream else None

    def _process_llm_request(
        self, request: Union[ChatCompletionRequest, CompletionRequest], is_chat: bool
    ) -> Union[LLMChatResponse, LLMCompletionsResponse]:
        """Common processing pipeline for both chat and completions APIs.

        Args:
            request: Either a ChatCompletionRequest or CompletionRequest object
            is_chat: Whether this is a chat request (True) or completions request (False)

        Returns:
            A generator of response objects (either chat completion or text completion)
        """
        request_id = get_serve_request_id()

        # 1. Construct the appropriate prompt based on request type
        if is_chat:
            prompt = Prompt(
                prompt=[
                    Message.model_validate(message) for message in request.messages
                ],
                parameters=request,
            )
        else:
            prompt = Prompt(
                prompt=request.prompt,
                parameters=request,
                use_prompt_format=False,
            )

        # 2. Predict using the engine
        gen = self._predict(request_id=request_id, prompt=prompt, stream=request.stream)

        # 3. Convert raw LLM responses to OpenAI format
        processor_method = (
            self.response_postprocessor.process_chat
            if is_chat
            else self.response_postprocessor.process_completions
        )
        openai_resp_generator = processor_method(
            model=self._llm_config.model_id, gen=gen, stream=request.stream
        )

        if request.stream:
            # 4. Apply batching with appropriate interval in case of streaming
            batched_openai_response_stream = OpenAIResponseBatcher(
                openai_resp_generator,
                interval_ms=self._get_batch_interval_ms(),
            )

            return batched_openai_response_stream.stream()

        return openai_resp_generator

    async def chat(self, request: ChatCompletionRequest) -> LLMChatResponse:
        """Runs a chat request to the LLM engine and returns the response.

        Args:
            request: A ChatCompletionRequest object.

        Returns:
            A LLMChatResponse object.
        """
        return self._process_llm_request(request, is_chat=True)

    async def completions(self, request: CompletionRequest) -> LLMCompletionsResponse:
        """Runs a completion request to the LLM engine and returns the response.

        Args:
            request: A CompletionRequest object.

        Returns:
            A LLMCompletionsResponse object.
        """
        return self._process_llm_request(request, is_chat=False)

    async def check_health(self) -> None:
        """
        Check the health of the replica. Does not return anything. Raise error when
        the engine is dead and needs to be restarted.
        """
        if self.engine is None:
            return
        try:
            return await self.engine.check_health()
        except Exception as e:
            logger.error("Engine health check failed in LLMServer.check_health: %s", e)
            raise e

    async def embeddings(self, request: EmbeddingRequest) -> LLMEmbeddingsResponse:
        """Runs an embeddings request to the vllm engine, and return the response.

        Args:
            request: An EmbeddingRequest object.

        Returns:
            A LLMEmbeddingsResponse object.
        """
        request_id = get_serve_request_id()
        try:
            multiplexed_model_id = serve.get_multiplexed_model_id()

            if multiplexed_model_id:
                assert (
                    self._llm_config.lora_config is not None
                ), "Must setup lora config for multiplexed requests."
                disk_lora_model = await self._disk_lora_model(multiplexed_model_id)
            else:
                disk_lora_model = None

            request_params = {
                "request_id": request_id,
                "prompt": request.input,
                "encoding_format": request.encoding_format,
                "disk_multiplex_config": disk_lora_model,
                "serve_request_context": serve.context._serve_request_context.get(),
            }
            vllm_request = VLLMEmbeddingRequest(**request_params)
            embedding_data, total_tokens = await self.engine.embed(vllm_request)

            data = [
                EmbeddingResponseData(
                    object="embedding", index=index, embedding=embedding
                )
                for index, embedding in enumerate(embedding_data)
            ]

            usage = UsageInfo(prompt_tokens=total_tokens, total_tokens=total_tokens)

            yield EmbeddingResponse(
                model=self._llm_config.model_id, data=data, usage=usage, object="list"
            )
        except Exception as e:
            logger.error(
                f"Failed while handling embeddings for request ({request_id}): {repr(e)}",
                exc_info=e,
            )

    async def _load_model(self, lora_model_id: str) -> DiskMultiplexConfig:
        return await self.model_downloader.load_model(
            lora_model_id=lora_model_id,
            llm_config=self._llm_config,
        )

    async def _disk_lora_model(self, lora_model_id: str) -> DiskMultiplexConfig:
        disk_lora_model: DiskMultiplexConfig = await self.load_model(lora_model_id)
        return disk_lora_model

    @classmethod
    def as_deployment(
        cls, deployment_options: Dict[str, Any] = None
    ) -> serve.Deployment:
        """Convert the LLMServer to a Ray Serve deployment.

        Args:
            deployment_options: A dictionary of deployment options.

        Returns:
            A Ray Serve deployment.
        """
        deployment_options = deployment_options or {}
        return LLMDeployment.options(**deployment_options)


@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "initial_replicas": 1,
        "max_replicas": 10,
        "target_ongoing_requests": int(
            os.environ.get(
                "RAYLLM_ROUTER_TARGET_ONGOING_REQUESTS",
                os.environ.get(
                    "RAYLLM_ROUTER_TARGET_NUM_ONGOING_REQUESTS_PER_REPLICA", 10
                ),
            )
        ),
    },
    max_ongoing_requests=20,  # Maximum backlog for a single replica
    health_check_period_s=DEFAULT_HEALTH_CHECK_PERIOD_S,
    health_check_timeout_s=DEFAULT_HEALTH_CHECK_TIMEOUT_S,
)
class LLMDeployment(LLMServer):
    # Note (genesu): We are separating the LLMServer and LLMDeployment just
    # to give developers an ability to test the implementation outside the Ray Serve.
    # But in practice we should always test the LLMDeployment class as a Serve
    # deployment to ensure all functionalities can be run remotely asynchronously.
    ...
