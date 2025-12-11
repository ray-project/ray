import asyncio
import copy
import os
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

import ray
from ray import serve
from ray._common.utils import import_attr
from ray.llm._internal.serve.constants import (
    ENABLE_WORKER_PROCESS_SETUP_HOOK,
    ENGINE_START_TIMEOUT_S,
    MODEL_RESPONSE_BATCH_TIMEOUT_MS,
    RAYLLM_VLLM_ENGINE_CLS_ENV,
)
from ray.llm._internal.serve.core.configs.llm_config import (
    DiskMultiplexConfig,
    LLMConfig,
)
from ray.llm._internal.serve.core.engine.protocol import LLMEngine
from ray.llm._internal.serve.core.protocol import LLMServerProtocol, RawRequestInfo
from ray.llm._internal.serve.engines.vllm.vllm_engine import VLLMEngine
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.usage_telemetry.usage import (
    push_telemetry_report_for_all_models,
)
from ray.llm._internal.serve.utils.batcher import Batcher
from ray.llm._internal.serve.utils.lora_serve_utils import (
    LoraModelLoader,
)
from ray.llm._internal.serve.utils.server_utils import (
    get_serve_request_id,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.openai_api_models import (
        ChatCompletionRequest,
        ChatCompletionResponse,
        CompletionRequest,
        CompletionResponse,
        EmbeddingRequest,
        EmbeddingResponse,
        ErrorResponse,
        ScoreRequest,
        ScoreResponse,
        TranscriptionRequest,
        TranscriptionResponse,
    )

logger = get_logger(__name__)
T = TypeVar("T")


def _merge_replica_actor_and_child_actor_bundles(
    child_actor_bundles: List[Dict[str, float]],
    replica_actor_bundle: Dict[str, float],
) -> List[Dict[str, float]]:
    """Sum up the bundles from replica actor bundles with the first bundle from child actor bundles.

    This is because the replica actor will use the first bundle in the list, and we want to collocate the replica actor with the child actor.
    So we need to group them together.

    So for example:
    child_actor_bundles = [{"GPU": 1, "CPU": 1}, {"GPU": 1, "CPU": 1}]
    replica_actor_bundle = {"GPU": 0, "CPU": 1, "memory": 100}
    return [{"GPU": 1, "CPU": 2, "memory": 100}, {"GPU": 1, "CPU": 1}]
    """

    if not child_actor_bundles:
        return [copy.copy(replica_actor_bundle)]

    if not replica_actor_bundle:
        return [copy.copy(bundle) for bundle in child_actor_bundles]

    original_first_bundle = child_actor_bundles[0]
    bundle_key_set = set(original_first_bundle.keys()) | set(
        replica_actor_bundle.keys()
    )

    merged_first_bundle = {
        key: original_first_bundle.get(key, 0) + replica_actor_bundle.get(key, 0)
        for key in bundle_key_set
    }

    return [merged_first_bundle] + [
        copy.copy(bundle) for bundle in child_actor_bundles[1:]
    ]


class LLMServer(LLMServerProtocol):
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
        LLMServerProtocol.__init__(instance)
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
            "ChatCompletionRequest",
            "CompletionRequest",
            "EmbeddingRequest",
            "TranscriptionRequest",
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
            "ChatCompletionRequest",
            "CompletionRequest",
            "EmbeddingRequest",
            "TranscriptionRequest",
            "ScoreRequest",
        ],
        *,
        engine_method: str,
        batch_output_stream: bool = False,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Any, None]:
        """Run the engine method on the request + perform batching when stream=True.

        Args:
            request: The request to run.
            engine_method: The method to call on the engine.
            batch_output_stream: Whether to batch the output stream.
            raw_request_info: Optional RawRequestInfo containing data from the original
                HTTP request.

        Returns:
            An AsyncGenerator of the response. If stream is True and batching is enabled, then the generator will yield a list of streaming responses (strings of the format data: {response_json}\n\n). Otherwise, it will yield the non-streaming response from engine directly.
        """

        await self._maybe_add_request_id_to_request(request)
        await self._maybe_resolve_lora_from_multiplex()

        is_stream = hasattr(request, "stream") and request.stream
        engine_stream = getattr(self.engine, engine_method)(request, raw_request_info)

        if is_stream and batch_output_stream:
            stream = self._batch_output_stream(engine_stream)
        else:
            stream = engine_stream

        return stream

    async def chat(
        self,
        request: "ChatCompletionRequest",
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "ChatCompletionResponse"], None
    ]:
        """Runs a chat request to the LLM engine and returns the response.

        Args:
            request: A ChatCompletionRequest object.
            raw_request_info: Optional RawRequestInfo containing data from the original
                HTTP request.

        Returns:
            An AsyncGenerator of the response. If stream is True and batching
            is enabled, then the generator will yield a list of chat streaming
            responses (strings of the format data: {response_json}\\n\\n).
            Otherwise, it will yield the ChatCompletionResponse object directly.
        """
        return await self._run_request(
            request,
            engine_method="chat",
            batch_output_stream=True,
            raw_request_info=raw_request_info,
        )

    async def completions(
        self,
        request: "CompletionRequest",
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "CompletionResponse"], None
    ]:
        """Runs a completion request to the LLM engine and returns the response.

        Args:
            request: A CompletionRequest object.
            raw_request_info: Optional RawRequestInfo containing data from the original
                HTTP request.

        Returns:
            An AsyncGenerator of the response. If stream is True and batching
            is enabled, then the generator will yield a list of completion
            streaming responses (strings of the format data: {response_json}\\n\\n).
            Otherwise, it will yield the CompletionResponse object directly.
        """
        return await self._run_request(
            request,
            engine_method="completions",
            batch_output_stream=True,
            raw_request_info=raw_request_info,
        )

    async def embeddings(
        self,
        request: "EmbeddingRequest",
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[List["ErrorResponse"], "EmbeddingResponse"], None]:
        """Runs an embeddings request to the engine and returns the response.

        Returns an AsyncGenerator over the EmbeddingResponse object. This is so that the caller can have a consistent interface across all the methods of chat, completions, embeddings and transcriptions.

        Args:
            request: An EmbeddingRequest object.
            raw_request_info: Optional RawRequestInfo containing data from the original
                HTTP request.

        Returns:
            An AsyncGenerator over the EmbeddingResponse object.
        """
        # NOTE: Embeddings does not need batching.
        return await self._run_request(
            request,
            engine_method="embeddings",
            batch_output_stream=False,
            raw_request_info=raw_request_info,
        )

    async def transcriptions(
        self,
        request: "TranscriptionRequest",
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "TranscriptionResponse"], None
    ]:
        """Runs an transcriptions request to the engine and returns the response.

        Returns an AsyncGenerator over the TranscriptionResponse object. This is so that the caller can have a consistent interface across all the methods of chat, completions, embeddings and transcriptions.

        Args:
            request: A TranscriptionRequest object.
            raw_request_info: Optional RawRequestInfo containing data from the original
                HTTP request.

        Returns:
            An AsyncGenerator over the TranscriptionResponse object.
        """
        return await self._run_request(
            request,
            engine_method="transcriptions",
            batch_output_stream=True,
            raw_request_info=raw_request_info,
        )

    async def score(
        self,
        request: "ScoreRequest",
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union["ScoreResponse", "ErrorResponse"], None]:
        """Runs a score request to the engine and returns the response.

        Returns an AsyncGenerator over the ScoreResponse object. This is so that the caller can have a consistent interface across all the methods of chat, completions, embeddings, and score.

        Args:
            request: A ScoreRequest object.
            raw_request_info: Optional RawRequestInfo containing data from the original
                HTTP request.

        Returns:
            An AsyncGenerator over the ScoreResponse object.
        """
        # NOTE: Score does not need batching, similar to embeddings.
        return await self._run_request(
            request,
            engine_method="score",
            batch_output_stream=False,
            raw_request_info=raw_request_info,
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

    async def start_profile(self) -> None:
        """Start profiling"""
        if self.engine is None:
            return
        try:
            await self.engine.start_profile()
        except Exception as e:
            logger.error(
                "Engine start profile failed in LLMServer.start_profile: %s", e
            )
            raise e

    async def stop_profile(self) -> None:
        """Stop profiling"""
        if self.engine is None:
            return
        try:
            await self.engine.stop_profile()
        except Exception as e:
            logger.error("Engine stop profile failed in LLMServer.stop_profile: %s", e)
            raise e

    async def llm_config(self) -> Optional[LLMConfig]:
        return self._llm_config

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        engine_config = llm_config.get_engine_config()
        deployment_options = copy.deepcopy(llm_config.deployment_config)

        # Handle the ray_actor_options that could be passed in to
        # deployment_options
        ray_actor_options = deployment_options.get("ray_actor_options", {})

        replica_actor_resources = {
            "CPU": ray_actor_options.get("num_cpus", 1),
            "GPU": ray_actor_options.get("num_gpus", 0),
            **ray_actor_options.get("resources", {}),
        }
        if "memory" in ray_actor_options:
            replica_actor_resources["memory"] = ray_actor_options["memory"]

        if (
            "placement_group_bundles" in llm_config.deployment_config
            or "placement_group_strategy" in llm_config.deployment_config
        ):
            raise ValueError(
                "placement_group_bundles and placement_group_strategy must not be specified in deployment_config. You can override the default values by setting the `placement_group_config` in the LLMConfig."
            )

        # TODO: Move this _merge_replica_actor_and_child_actor_bundles to a
        # more generic place.
        pg_bundles = _merge_replica_actor_and_child_actor_bundles(
            engine_config.placement_bundles, replica_actor_resources
        )

        deployment_options.update(
            {
                "placement_group_bundles": pg_bundles,
                "placement_group_strategy": engine_config.placement_strategy,
            }
        )

        # Handle env vars from runtime_env
        default_runtime_env = ray.get_runtime_context().runtime_env
        if ENABLE_WORKER_PROCESS_SETUP_HOOK:
            default_runtime_env[
                "worker_process_setup_hook"
            ] = "ray.llm._internal.serve._worker_process_setup_hook"

        ray_actor_options = deployment_options.get("ray_actor_options", {})
        ray_actor_options["runtime_env"] = {
            **default_runtime_env,
            # Existing runtime_env should take precedence over the default.
            **ray_actor_options.get("runtime_env", {}),
            **(llm_config.runtime_env if llm_config.runtime_env else {}),
        }
        deployment_options["ray_actor_options"] = ray_actor_options

        return deployment_options
