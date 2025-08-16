import argparse
import os
from typing import TYPE_CHECKING, AsyncGenerator, Optional, Tuple, Union

from starlette.datastructures import State
from starlette.requests import Request
from transformers.dynamic_module_utils import init_hf_modules
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.entrypoints.openai.cli_args import FrontendArgs
from vllm.entrypoints.openai.protocol import ErrorResponse as VLLMErrorResponse

import ray
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ErrorResponse,
)
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_engine import LLMEngine
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMEngineConfig,
)
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    InitializeNodeOutput,
    initialize_node,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

if TYPE_CHECKING:
    from vllm.config import VllmConfig
    from vllm.engine.protocol import EngineClient
    from vllm.entrypoints.openai.serving_chat import OpenAIServingChat
    from vllm.entrypoints.openai.serving_completion import OpenAIServingCompletion
    from vllm.entrypoints.openai.serving_embedding import OpenAIServingEmbedding
    from vllm.entrypoints.openai.serving_models import OpenAIServingModels

vllm = try_import("vllm")
logger = get_logger(__name__)


def _get_vllm_engine_config(
    llm_config: LLMConfig,
) -> Tuple["AsyncEngineArgs", "VllmConfig"]:
    engine_config = llm_config.get_engine_config()
    async_engine_args = vllm.engine.arg_utils.AsyncEngineArgs(
        **engine_config.get_initialization_kwargs()
    )
    from vllm.usage.usage_lib import UsageContext

    vllm_engine_config = async_engine_args.create_engine_config(
        usage_context=UsageContext.OPENAI_API_SERVER
    )
    return async_engine_args, vllm_engine_config


def _clear_current_platform_cache():
    """Clear the cache of the current platform.

    vllm current has an lru cache for getting device compatibility
    that will not have the correct returned value if
    CUDA_VISIBLE_DEVICES is not set properly. In RayLLM eventually
    when we want to create the engine the env will be set properly,
    but till then, upon the import of vllm somewhere
    (which is a mystery) the lru cache will have the wrong value.
    This function will clear the cache so that the next time the
    cache is accessed, it will be re-evaluated.

    Related issues:
    https://github.com/vllm-project/vllm/issues/8402
    https://github.com/vllm-project/vllm/issues/7890
    """
    from vllm.platforms import current_platform

    # TODO(seiji): remove this once https://github.com/vllm-project/vllm/pull/18979 is merged
    if (
        "CUDA_VISIBLE_DEVICES" in os.environ
        and os.environ["CUDA_VISIBLE_DEVICES"] == ""
    ):
        del os.environ["CUDA_VISIBLE_DEVICES"]

    # This check is just to future proof this implementation
    # in case vllm removes their lru_cache decorator
    if hasattr(current_platform.get_device_capability, "cache_clear"):
        logger.info("Clearing the current platform cache ...")
        current_platform.get_device_capability.cache_clear()


class VLLMEngine(LLMEngine):
    def __init__(
        self,
        llm_config: LLMConfig,
    ):
        """Create a vLLM Engine class

        Args:
            llm_config: The llm configuration for this engine
        """
        super().__init__(llm_config)

        # Ensure transformers_modules is initialized early in worker processes.
        # This is critical for models with trust_remote_code=True to avoid pickle errors.
        init_hf_modules()

        self.llm_config = llm_config

        if vllm is None:
            raise ImportError(
                "vLLM is not installed. Please install it with `pip install ray[llm]`."
            )
        from vllm import envs as vllm_envs

        if not vllm_envs.VLLM_USE_V1:
            logger.warning(
                "vLLM v0 is getting fully deprecated. As a result in Ray Serve LLM only v1 is supported. Only when you know what you are doing, you can set VLLM_USE_V1=0"
            )

        self.llm_config.setup_engine_backend()

        self._running = False

        # vLLM Integration points. Will be set through .start()
        self._engine_client = None
        self._oai_models: Optional["OpenAIServingModels"] = None
        self._oai_serving_chat: Optional["OpenAIServingChat"] = None
        self._oai_serving_completion: Optional["OpenAIServingCompletion"] = None
        self._oai_serving_embedding: Optional["OpenAIServingEmbedding"] = None

    async def start(self) -> None:
        """Start the vLLM engine.

        If the engine is already running, do nothing.
        """

        if self._running:
            # The engine is already running!
            logger.info("Skipping engine restart because the engine is already running")
            return

        from vllm.entrypoints.openai.api_server import init_app_state

        node_initialization = await initialize_node(self.llm_config)

        (
            vllm_engine_args,
            vllm_frontend_args,
            vllm_engine_config,
        ) = self._prepare_engine_config(node_initialization)

        # Apply checkpoint info to the llm_config.
        # This is needed for capturing model capabilities
        # (e.g. supports vision, etc.) on the llm_config.
        config = self.llm_config.get_engine_config()
        self.llm_config.apply_checkpoint_info(
            config.actual_hf_model_id,
            trust_remote_code=config.trust_remote_code,
        )

        self._engine_client = self._start_async_llm_engine(
            vllm_engine_args,
            vllm_engine_config,
            node_initialization.placement_group,
        )

        state = State()
        # TODO (Kourosh): There might be some variables that needs protection?
        args = argparse.Namespace(
            **vllm_frontend_args.__dict__,
            **vllm_engine_args.__dict__,
        )

        await init_app_state(
            engine_client=self._engine_client,
            vllm_config=vllm_engine_config,
            state=state,
            args=args,
        )

        self._oai_models = state.openai_serving_models
        self._oai_serving_chat = state.openai_serving_chat
        self._oai_serving_completion = state.openai_serving_completion
        self._oai_serving_embedding = state.openai_serving_embedding

        self._validate_openai_serving_models()
        self._validate_engine_client()

        self._running = True

        logger.info("Started vLLM engine.")

    def _validate_openai_serving_models(self):
        assert self._oai_models is not None, "oai_models is not initialized"
        assert hasattr(
            self._oai_models, "lora_requests"
        ), "oai_models must have a lora_requests attribute"
        assert hasattr(
            self._oai_models, "load_lora_adapter"
        ), "oai_models must have a load_lora_adapter attribute"

    def _validate_openai_serving_chat(self):
        assert hasattr(
            self._oai_serving_chat, "create_chat_completion"
        ), "oai_serving_chat must have a create_chat_completion attribute"

    def _validate_openai_serving_completion(self):
        assert hasattr(
            self._oai_serving_completion, "create_completion"
        ), "oai_serving_completion must have a create_completion attribute"

    def _validate_openai_serving_embedding(self):
        assert hasattr(
            self._oai_serving_embedding, "create_embedding"
        ), "oai_serving_embedding must have a create_embedding attribute"

    def _validate_engine_client(self):
        assert hasattr(
            self._engine_client, "check_health"
        ), "engine_client must have a check_health attribute"

    def _prepare_engine_config(
        self, node_initialization: InitializeNodeOutput
    ) -> Tuple["AsyncEngineArgs", "FrontendArgs", "VllmConfig"]:
        """Prepare the engine config to start the engine.

        Args:
            node_initialization: The node initialization output.

        Returns:
            A tuple of:
                engine_args: The vLLM's internal engine arguments that is flattened.
                frontend_args: The vLLM's internal frontend arguments that is flattened.
                engine_config: The vLLM's internal engine config that is nested.
        """

        engine_config: VLLMEngineConfig = self.llm_config.get_engine_config()

        if engine_config.use_gpu:
            # Create engine config on a task with access to GPU,
            # as GPU capability may be queried.
            ref = (
                ray.remote(
                    num_cpus=0,
                    num_gpus=0.001,
                    accelerator_type=self.llm_config.accelerator_type,
                )(_get_vllm_engine_config)
                .options(
                    runtime_env=node_initialization.runtime_env,
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=node_initialization.placement_group,
                    ),
                )
                .remote(self.llm_config)
            )
            vllm_engine_args, vllm_engine_config = ray.get(ref)
        else:
            vllm_engine_args, vllm_engine_config = _get_vllm_engine_config(
                self.llm_config
            )

        vllm_frontend_args = FrontendArgs(**engine_config.frontend_kwargs)
        return vllm_engine_args, vllm_frontend_args, vllm_engine_config

    def _start_async_llm_engine_v0(
        self,
        engine_args: "AsyncEngineArgs",
        vllm_config: "VllmConfig",
        placement_group: PlacementGroup,
    ) -> "EngineClient":

        from vllm.engine.async_llm_engine import AsyncLLMEngine
        from vllm.executor.ray_distributed_executor import RayDistributedExecutor

        vllm_config.parallel_config.placement_group = placement_group

        _clear_current_platform_cache()

        engine_client = AsyncLLMEngine(
            vllm_config=vllm_config,
            executor_class=RayDistributedExecutor,
            log_stats=not engine_args.disable_log_stats,
        )

        return engine_client

    def _start_async_llm_engine(
        self,
        vllm_engine_args: "AsyncEngineArgs",
        vllm_engine_config: "VllmConfig",
        placement_group: PlacementGroup,
    ) -> "EngineClient":
        """Creates an async LLM engine from the engine arguments."""
        from vllm import envs as vllm_envs

        # NOTE: This is a temporary solution until vLLM v1 supports embeddings.
        if not vllm_envs.VLLM_USE_V1:
            return self._start_async_llm_engine_v0(
                vllm_engine_args, vllm_engine_config, placement_group
            )

        from vllm.v1.engine.async_llm import AsyncLLM
        from vllm.v1.executor.abstract import Executor

        vllm_engine_config.parallel_config.placement_group = placement_group

        _clear_current_platform_cache()

        custom_stat_loggers = None
        if self.llm_config.log_engine_metrics:
            from vllm.v1.metrics.ray_wrappers import RayPrometheusStatLogger

            # V1 AsyncLLM does not yet support add_logger: https://github.com/vllm-project/vllm/issues/17702
            # Use `disable_log_stats: False` and `log_engine_metrics: False` as
            # a workaround to enable PrometheusStatLogger instead.
            custom_stat_loggers = [RayPrometheusStatLogger]

        executor_class = Executor.get_class(vllm_engine_config)
        logger.info(f"Using executor class: {executor_class}")
        engine_client = AsyncLLM(
            vllm_config=vllm_engine_config,
            executor_class=executor_class,
            log_stats=not vllm_engine_args.disable_log_stats,
            stat_loggers=custom_stat_loggers,
        )

        return engine_client

    async def resolve_lora(self, disk_lora_model: DiskMultiplexConfig):
        from vllm.entrypoints.openai.protocol import LoadLoRAAdapterRequest

        self._validate_openai_serving_models()

        if disk_lora_model.model_id in self._oai_models.lora_requests:
            # Lora is already loaded, return
            return

        lora_request = await self._oai_models.load_lora_adapter(  # type: ignore[attr-defined]
            request=LoadLoRAAdapterRequest(
                lora_name=disk_lora_model.model_id,
                lora_path=disk_lora_model.local_path,
            )
        )

        if isinstance(lora_request, VLLMErrorResponse):
            raise ValueError(f"Failed to load lora model: {lora_request.message}")

    def _create_raw_request(
        self,
        request: Union[CompletionRequest, ChatCompletionRequest, EmbeddingRequest],
        path: str,
    ) -> Request:
        scope = {
            "type": "http",
            "method": "POST",
            "path": path,
            "headers": [(b"x-request-id", getattr(request, "request_id", "").encode())],
            "query_string": b"",
        }
        return Request(scope)

    async def chat(
        self, request: ChatCompletionRequest
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        self._validate_openai_serving_chat()

        # TODO (Kourosh): Remove when we upstream request_id attribute to vLLM.
        # PR: https://github.com/vllm-project/vllm/pull/21009
        # Create a fake starlette.Request object with the x-request-id header
        # so that the create_chat_completion API can assign the request_id properly.
        raw_request = self._create_raw_request(request, "/chat/completions")

        chat_response = await self._oai_serving_chat.create_chat_completion(  # type: ignore[attr-defined]
            request, raw_request=raw_request
        )

        if isinstance(chat_response, AsyncGenerator):
            async for response in chat_response:
                if not isinstance(response, str):
                    raise ValueError(
                        f"Expected create_chat_completion to return a stream of strings, got and item with type {type(response)}"
                    )
                yield response
        else:
            if isinstance(chat_response, VLLMErrorResponse):
                yield ErrorResponse(**chat_response.model_dump())
            else:
                yield ChatCompletionResponse(**chat_response.model_dump())

    async def completions(
        self, request: CompletionRequest
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        self._validate_openai_serving_completion()

        # TODO (Kourosh): Remove when we upstream request_id attribute to vLLM.
        # PR: https://github.com/vllm-project/vllm/pull/21009
        # Create a fake starlette.Request object with the x-request-id header
        # so that the create_completion API can assign the request_id properly.
        raw_request = self._create_raw_request(request, "/completions")

        completion_response = await self._oai_serving_completion.create_completion(  # type: ignore[attr-defined]
            request,
            raw_request=raw_request,
        )

        if isinstance(completion_response, AsyncGenerator):
            async for response in completion_response:
                if not isinstance(response, str):
                    raise ValueError(
                        f"Expected create_completion to return a stream of strings, got and item with type {type(response)}"
                    )
                yield response
        else:
            if isinstance(completion_response, VLLMErrorResponse):
                yield ErrorResponse(**completion_response.model_dump())
            else:
                yield CompletionResponse(**completion_response.model_dump())

    async def embeddings(
        self, request: EmbeddingRequest
    ) -> AsyncGenerator[Union[EmbeddingResponse, ErrorResponse], None]:
        self._validate_openai_serving_embedding()

        # TODO (Kourosh): Remove when upstream is fixed to accept req_id.
        # Create a fake starlette.Request object with the x-request-id header
        # so that the create_embedding API can assign the request_id properly.
        raw_request = self._create_raw_request(request, "/embeddings")

        embedding_response = await self._oai_serving_embedding.create_embedding(  # type: ignore[attr-defined]
            request, raw_request=raw_request
        )

        if isinstance(embedding_response, VLLMErrorResponse):
            yield ErrorResponse(**embedding_response.model_dump())
        else:
            yield EmbeddingResponse(**embedding_response.model_dump())

    async def check_health(self) -> None:
        assert self._engine_client is not None, "engine_client is not initialized"

        try:
            await self._engine_client.check_health()
        except BaseException as e:
            logger.error("Healthcheck failed. The replica will be restarted")
            raise e from None

    async def reset_prefix_cache(self) -> None:
        assert self._engine_client is not None, "engine_client is not initialized"
        await self._engine_client.reset_prefix_cache()
