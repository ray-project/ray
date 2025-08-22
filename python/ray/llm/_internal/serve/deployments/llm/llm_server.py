import asyncio
import os
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

from ray import serve
from ray._common.utils import import_attr
from ray.llm._internal.serve.configs.constants import (
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_MAX_ONGOING_REQUESTS,
    DEFAULT_MAX_REPLICAS,
    DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    ENGINE_START_TIMEOUT_S,
    MODEL_RESPONSE_BATCH_TIMEOUT_MS,
    RAYLLM_VLLM_ENGINE_CLS_ENV,
)
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_engine import LLMEngine
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import VLLMEngine
from ray.llm._internal.serve.deployments.utils.batcher import Batcher
from ray.llm._internal.serve.deployments.utils.server_utils import (
    get_serve_request_id,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.usage_telemetry.usage import (
    push_telemetry_report_for_all_models,
)
from ray.llm._internal.serve.utils.lora_serve_utils import (
    LoraModelLoader,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.configs.openai_api_models import (
        ChatCompletionRequest,
        ChatCompletionResponse,
        CompletionRequest,
        CompletionResponse,
        EmbeddingRequest,
        EmbeddingResponse,
        ErrorResponse,
    )

logger = get_logger(__name__)
T = TypeVar("T")


class _LLMServerBase(ABC):
    """
    This is the common interface between all the llm deployment. All llm deployments
    need to implement a sync constructor, an async start method, and check_health method.
    """

    def __init__(self):
        """
        Constructor takes basic setup that doesn't require async operations.
        """

    @abstractmethod
    async def start(self):
        """
        Start the underlying engine. This handles async initialization.
        """
        ...

    @abstractmethod
    async def chat(
        self, request: "ChatCompletionRequest"
    ) -> AsyncGenerator[Union[str, "ChatCompletionResponse", "ErrorResponse"], None]:
        """
        Inferencing to the engine for chat, and return the response.
        """
        ...

    @abstractmethod
    async def completions(
        self, request: "CompletionRequest"
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "CompletionResponse"], None
    ]:
        """
        Inferencing to the engine for completion api, and return the response.
        """
        ...

    @abstractmethod
    async def check_health(self) -> None:
        """
        Check the health of the replica. Does not return anything.
        Raise error when the engine is dead and needs to be restarted.
        """
        ...

    @abstractmethod
    async def reset_prefix_cache(self) -> None:
        """Reset the prefix cache of the underlying engine"""

    # TODO (Kourosh): This does not belong here.
    async def llm_config(self) -> Optional[LLMConfig]:
        return None


class LLMServer(_LLMServerBase):
    """This is a shim layer to decouple the LLM engine from the ingress
    deployment.

    It has a very similar API as the engine. Almost all of the abstractions are
    implemented by the engine. This class just a little bit more logic on top:

    1. Logic for serve multiplexing (e.g. LoRA loading).
    2. Request id handing from serve context.
    3. Batching in case of streaming (only for chat and completions).
    4. Telemetry reporting.

    Usage Patterns:

    1. Basic pattern (for testing):
        server = LLMServer.sync_init(llm_config)  # Sync constructor, unstarted
        await server.start()  # Must explicitly start

    2. Async context (default, used by Ray Serve):
        server = await LLMServer(llm_config)  # Async constructor, fully started

    3. Ray Serve deployment:
        # Ray Serve calls the async constructor directly
        deployment = serve.deployment(LLMServer).bind(llm_config)
    """

    _default_engine_cls = VLLMEngine

    async def __init__(
        self,
        llm_config: LLMConfig,
        *,
        engine_cls: Optional[Type[LLMEngine]] = None,
        model_downloader: Optional[Type[LoraModelLoader]] = None,
    ):
        """Asynchronous constructor that returns a fully started instance.

        This is the default constructor used by Ray Serve deployments.

        Args:
            llm_config: LLMConfig for the model.
            engine_cls: Dependency injection for the vllm engine class.
                Defaults to `VLLMEngine`.
            model_downloader: Dependency injection for the model downloader.
                Defaults to `LoraModelLoader`.
        """
        super().__init__()
        self._init_shared(llm_config, engine_cls, model_downloader)
        await self.start()

    def _init_shared(
        self,
        llm_config: LLMConfig,
        engine_cls: Optional[Type[LLMEngine]] = None,
        model_downloader: Optional[Type[LoraModelLoader]] = None,
    ):
        """Shared initialization logic between constructors."""
        self._llm_config = llm_config
        self._engine_cls = engine_cls or self._get_default_engine_class()
        self.engine: Optional[LLMEngine] = None
        self._init_multiplex_loader(model_downloader)

    @classmethod
    def sync_init(
        cls,
        llm_config: LLMConfig,
        *,
        engine_cls: Optional[Type[LLMEngine]] = None,
        model_downloader: Optional[Type[LoraModelLoader]] = None,
    ) -> "LLMServer":
        """Synchronous constructor that returns an unstarted instance.

        This is used for testing the new pattern where initialization
        and starting are explicitly separated.

        Args:
            llm_config: LLMConfig for the model.
            engine_cls: Dependency injection for the vllm engine class.
                Defaults to `VLLMEngine`.
            model_downloader: Dependency injection for the model downloader.
                Defaults to `LoraModelLoader`.

        Returns:
            An unstarted LLMServer instance. Caller must call await start().
        """
        instance = cls.__new__(cls)
        _LLMServerBase.__init__(instance)
        instance._init_shared(llm_config, engine_cls, model_downloader)
        return instance

    async def start(self):
        """Start the underlying engine. This handles async initialization."""
        if self._engine_cls is not None:
            self.engine = self._engine_cls(self._llm_config)
            await asyncio.wait_for(self._start_engine(), timeout=ENGINE_START_TIMEOUT_S)

    def _init_multiplex_loader(
        self, model_downloader_cls: Optional[Type[LoraModelLoader]] = None
    ):
        """Initialize the multiplex loader."""

        model_downloader_cls = model_downloader_cls or LoraModelLoader
        mx_config = self._llm_config.multiplex_config()

        if mx_config is not None:
            model_downloader = model_downloader_cls(
                download_timeout_s=mx_config.download_timeout_s,
                max_tries=mx_config.max_download_tries,
            )

            async def _load_model(lora_model_id: str) -> DiskMultiplexConfig:
                return await model_downloader.load_model_from_config(
                    lora_model_id=lora_model_id,
                    llm_config=self._llm_config,
                )

            self._load_model = serve.multiplexed(
                max_num_models_per_replica=mx_config.max_num_models_per_replica
            )(_load_model)
        else:

            async def _load_model(lora_model_id: str) -> DiskMultiplexConfig:
                raise ValueError("LoRA config is not set in the LLMConfig")

            self._load_model = _load_model

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

    def _get_batch_interval_ms(self, stream: bool = True) -> int:
        """Calculate the batching interval for responses."""
        stream_batching_interval_ms = self._llm_config.experimental_configs.get(
            "stream_batching_interval_ms"
        )
        if stream_batching_interval_ms is None:
            stream_batching_interval_ms = MODEL_RESPONSE_BATCH_TIMEOUT_MS
        return stream_batching_interval_ms if stream else None

    async def _maybe_add_request_id_to_request(
        self,
        request: Union[
            "ChatCompletionRequest", "CompletionRequest", "EmbeddingRequest"
        ],
    ):
        """Add the request id to the request."""
        request_id = get_serve_request_id()
        if request_id:
            request.request_id = request_id

    async def _maybe_resolve_lora_from_multiplex(self) -> None:
        """Handle the lora model for the request."""
        multiplexed_model_id = serve.get_multiplexed_model_id()
        if multiplexed_model_id:
            if self._llm_config.lora_config is None:
                raise ValueError("Must setup lora config for multiplexed requests.")
            disk_lora_model = await self._load_model(multiplexed_model_id)
            await self.engine.resolve_lora(disk_lora_model)

    def _batch_output_stream(
        self, generator: AsyncGenerator[T, None]
    ) -> AsyncGenerator[List[T], None]:
        return Batcher(
            generator,
            interval_ms=self._get_batch_interval_ms(),
        ).stream()

    async def _run_request(
        self,
        request: Union[
            "ChatCompletionRequest", "CompletionRequest", "EmbeddingRequest"
        ],
        *,
        engine_method: str,
        batch_output_stream: bool = False,
    ) -> AsyncGenerator[Any, None]:
        """Run the engine method on the request + perform batching when stream=True.

        Args:
            request: The request to run.
            engine_method: The method to call on the engine.
            batch_output_stream: Whether to batch the output stream.

        Returns:
            An AsyncGenerator of the response. If stream is True and batching is enabled, then the generator will yield a list of streaming responses (strings of the format data: {response_json}\n\n). Otherwise, it will yield the non-streaming response from engine directly.
        """

        await self._maybe_add_request_id_to_request(request)
        await self._maybe_resolve_lora_from_multiplex()

        is_stream = hasattr(request, "stream") and request.stream
        if is_stream and batch_output_stream:
            stream = self._batch_output_stream(
                getattr(self.engine, engine_method)(request)
            )
        else:
            stream = getattr(self.engine, engine_method)(request)

        return stream

    async def chat(
        self, request: "ChatCompletionRequest"
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "ChatCompletionResponse"], None
    ]:
        """Runs a chat request to the LLM engine and returns the response.

        Args:
            request: A ChatCompletionRequest object.

        Returns:
            An AsyncGenerator of the response. If stream is True and batching is enabled, then the generator will yield a list of chat streaming responses (strings of the format data: {response_json}\n\n). Otherwise, it will yield the ChatCompletionResponse object directly.
        """
        return await self._run_request(
            request,
            engine_method="chat",
            batch_output_stream=True,
        )

    async def completions(
        self, request: "CompletionRequest"
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "CompletionResponse"], None
    ]:
        """Runs a completion request to the LLM engine and returns the response.

        Args:
            request: A CompletionRequest object.

        Returns:
            An AsyncGenerator of the response. If stream is True and batching is enabled, then the generator will yield a list of completion streaming responses (strings of the format data: {response_json}\n\n). Otherwise, it will yield the CompletionResponse object directly.
        """
        return await self._run_request(
            request,
            engine_method="completions",
            batch_output_stream=True,
        )

    async def embeddings(
        self, request: "EmbeddingRequest"
    ) -> AsyncGenerator[Union[List["ErrorResponse"], "EmbeddingResponse"], None]:
        """Runs an embeddings request to the engine and returns the response.

        Returns an AsyncGenerator over the EmbeddingResponse object. This is so that the caller can have a consistent interface across all the methods of chat, completions, and embeddings.

        Args:
            request: An EmbeddingRequest object.

        Returns:
            An AsyncGenerator over the EmbeddingResponse object.
        """
        # NOTE: Embeddings does not need batching.
        return await self._run_request(
            request, engine_method="embeddings", batch_output_stream=False
        )

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

    async def reset_prefix_cache(self) -> None:
        """Reset the prefix cache of the underlying engine"""
        if self.engine is None:
            return
        try:
            await self.engine.reset_prefix_cache()
        except Exception as e:
            logger.error(
                "Engine reset prefix cache failed in LLMServer.reset_prefix_cache: %s",
                e,
            )
            raise e

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
        "max_replicas": DEFAULT_MAX_REPLICAS,
        "target_ongoing_requests": DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    },
    max_ongoing_requests=DEFAULT_MAX_ONGOING_REQUESTS,
    health_check_period_s=DEFAULT_HEALTH_CHECK_PERIOD_S,
    health_check_timeout_s=DEFAULT_HEALTH_CHECK_TIMEOUT_S,
)
class LLMDeployment(LLMServer):
    # Note (genesu): We are separating the LLMServer and LLMDeployment just
    # to give developers an ability to test the implementation outside the Ray Serve.
    # But in practice we should always test the LLMDeployment class as a Serve
    # deployment to ensure all functionalities can be run remotely asynchronously.
    pass
