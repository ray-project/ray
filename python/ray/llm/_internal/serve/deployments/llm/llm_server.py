import asyncio
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type

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
    ChatCompletionRequest,
    CompletionRequest,
    EmbeddingRequest,
    LLMChatResponse,
    LLMCompletionsResponse,
    LLMEmbeddingsResponse,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_engine import LLMEngine
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import VLLMEngine
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMEmbeddingRequest,
)
from ray.llm._internal.serve.deployments.utils.batcher import OpenAIResponseBatcher
from ray.llm._internal.serve.deployments.utils.server_utils import (
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
    async def __init__(self):
        """
        Constructor takes in an LLMConfig object and start the underlying engine.
        """

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
    
    # TODO (Kourosh): This does not belong here. 
    async def llm_config(self) -> Optional[LLMConfig]:
        return None


class LLMServer(_LLMServerBase):
    """This is a shm layer to decouple the LLM engine from the ingress deployment.
    
    It has a very similar API as the engine. Almost all of the abstractions are implemented by the engine. This class just a little bit more logic on top, e.g.:
    1. Logic for serve multiplexing, etc.
    2. Telemetry reporting 
    """
    _default_engine_cls = VLLMEngine

    async def __init__(
        self,
        llm_config: LLMConfig,
        *,
        engine_cls: Optional[Type[LLMEngine]] = None,
    ):
        """Constructor of LLMServer.

        Only the llm_config is public api, the other arguments are private
        and used for testing.

        Args:
            llm_config: LLMConfig for the model.
            engine_cls: Dependency injection for the vllm engine class.
                Defaults to `VLLMEngine`.
        """
        await super().__init__()
        self._llm_config = llm_config

        self._engine_cls = engine_cls or self._get_default_engine_class()
        self.engine: Optional[LLMEngine] = None
        if self._engine_cls is not None:
            self.engine = self._engine_cls(self._llm_config)
            await asyncio.wait_for(self._start_engine(), timeout=ENGINE_START_TIMEOUT_S)


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
        # Note: the model architecture is only available after node initialized and the
        # engine is started.
        if self._llm_config.model_architecture:
            push_telemetry_report_for_all_models(all_models=[self._llm_config])


    def _get_batch_interval_ms(self, stream: bool = True) -> int:
        """Calculate the batching interval for responses."""
        stream_batching_interval_ms = self._llm_config.experimental_configs.get(
            "stream_batching_interval_ms"
        )
        if stream_batching_interval_ms is None:
            stream_batching_interval_ms = MODEL_RESPONSE_BATCH_TIMEOUT_MS
        return stream_batching_interval_ms if stream else None
    
    
    async def _maybe_resolve_lora_from_multiplex(self) -> None:
        """Handle the lora model for the request."""
        multiplexed_model_id = serve.get_multiplexed_model_id()
        if multiplexed_model_id:
            assert (
                self._llm_config.lora_config is not None
            ), "Must setup lora config for multiplexed requests."
            disk_lora_model = await self._disk_lora_model(multiplexed_model_id)
            await self.engine.resolve_lora(disk_lora_model)
            
    def _batch_output_stream(self, generator):
        return OpenAIResponseBatcher(
            generator,
            interval_ms=self._get_batch_interval_ms(),
        ).stream()
        

    async def chat(self, request: ChatCompletionRequest):
        """Runs a chat request to the LLM engine and returns the response.

        Args:
            request: A ChatCompletionRequest object.

        Returns:
            A LLMChatResponse object.
        """
        await self._maybe_resolve_lora_from_multiplex()
        stream = self._batch_output_stream(
            self.engine.chat(request)
        )

        async for chunk in stream:
            yield chunk

    async def completions(self, request: CompletionRequest) -> LLMCompletionsResponse:
        """Runs a completion request to the LLM engine and returns the response.

        Args:
            request: A CompletionRequest object.

        Returns:
            A LLMCompletionsResponse object.
        """
        await self._maybe_resolve_lora_from_multiplex()
        response_generator = self._batch_output_stream(
            request, 
            self.engine.completions(request)
        )

        async for response in response_generator:
            yield response
            

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

    async def llm_config(self) -> Optional[LLMConfig]:
        return self._llm_config
    
    @classmethod
    def as_deployment(
        cls, deployment_options: Optional[Dict[str, Any]] = None
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
