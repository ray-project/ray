import os
import uuid
import argparse
from starlette.datastructures import State

from typing import TYPE_CHECKING, AsyncGenerator, List, Tuple

import ray
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.configs.constants import (
    RAYLLM_ENABLE_REQUEST_PROMPT_LOGS,
)
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    GenerationRequest,
    LLMConfig,
)
from transformers.dynamic_module_utils import init_hf_modules

from ray.llm._internal.serve.deployments.llm.llm_engine import LLMEngine
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine_stats import (
    VLLMEngineStatTracker,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMEmbeddingRequest,
    VLLMEngineConfig,
)
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    InitializeNodeOutput,
    initialize_node,
)
from ray.llm._internal.serve.deployments.utils.server_utils import floats_to_base64
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from vllm.entrypoints.openai.cli_args import FrontendArgs
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.entrypoints.openai.protocol import ErrorResponse as VLLMErrorResponse


if TYPE_CHECKING:
    from vllm.config import VllmConfig
    from vllm.engine.protocol import EngineClient
    from vllm.outputs import PoolingRequestOutput

vllm = try_import("vllm")
logger = get_logger(__name__)


def _get_vllm_engine_config(
    llm_config: LLMConfig,
) -> Tuple["AsyncEngineArgs", "VllmConfig"]:
    engine_config = llm_config.get_engine_config()
    async_engine_args = vllm.engine.arg_utils.AsyncEngineArgs(**engine_config.get_initialization_kwargs())
    vllm_engine_config = async_engine_args.create_engine_config()
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
            
        if not vllm.envs.VLLM_USE_V1:
            raise ValueError("vLLM v0 is getting fully deprecated. As a result in Ray Serve LLM only v1 is supported.")

        # TODO (Kourosh): This validation logic belongs to the PDProxy module.
        # Pick a random port in P/D case.
        kv_transfer_config = llm_config.engine_kwargs.get("kv_transfer_config", None)
        if kv_transfer_config is not None:
            connector_type = getattr(kv_transfer_config, "kv_connector", "")
            if connector_type != "NixlConnector":
                raise ValueError("Only NixlConnector is supported for kv transfer.")
            if (
                "VLLM_NIXL_SIDE_CHANNEL_PORT" not in vllm.envs.environment_variables
                or "VLLM_NIXL_SIDE_CHANNEL_HOST" not in vllm.envs.environment_variables
            ):
                raise ValueError(
                    "This vLLM version does not support VLLM_NIXL_SIDE_CHANNEL_PORT"
                    "or VLLM_NIXL_SIDE_CHANNEL_HOST environment variable. It's likely"
                    "that you are using an older version of vLLM."
                )

            if not vllm.envs.is_set("VLLM_NIXL_SIDE_CHANNEL_PORT"):
                port: int = vllm.utils.get_open_port()
                os.environ["VLLM_NIXL_SIDE_CHANNEL_PORT"] = str(port)
            if not vllm.envs.is_set("VLLM_NIXL_SIDE_CHANNEL_HOST"):
                os.environ["VLLM_NIXL_SIDE_CHANNEL_HOST"] = vllm.utils.get_ip()

            # We need to overwrite the engine_id to make it unique across replicas.
            engine_id = getattr(kv_transfer_config, "engine_id", str(uuid.uuid4()))
            host = vllm.envs.VLLM_NIXL_SIDE_CHANNEL_HOST
            port = vllm.envs.VLLM_NIXL_SIDE_CHANNEL_PORT
            kv_transfer_config.engine_id = "-".join([engine_id, host, str(port)])


        # TODO (Kourosh): What do we do with this stats tracker? 
        self._stats = VLLMEngineStatTracker()
        self._running = False

        # vLLM Integration points. Will be set through .start()
        self._engine_client = None
        self._oai_models = None
        self._oai_serving_chat = None
        self._oai_serving_completion = None
        self._oai_serving_embedding = None


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

        self._engine_client = self._start_async_llm_engine(
            vllm_engine_args,
            vllm_engine_config,
            node_initialization.placement_group,
        )

        
        state = State()
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
        self._validate_openai_serving_chat()
        

        self._running = True

        logger.info("Started vLLM engine.")

    def _validate_openai_serving_models(self):
        if not hasattr(self._oai_models, "lora_requests"):
            raise ValueError("oai_models must have a lora_requests attribute")
        
        if not hasattr(self._oai_models, "load_lora_adapter"):
            raise ValueError("oai_models must have a load_lora_adapter attribute")
        
    def _validate_openai_serving_chat(self):
        if not hasattr(self._oai_serving_chat, "create_chat_completion"):
            raise ValueError("oai_serving_chat must have a create_chat_completion attribute")
        

    def _prepare_engine_config(self, node_initialization: InitializeNodeOutput):
        """Prepare the engine config to start the engine.

        Returns:
            engine_args: The vLLM's internal engine arguments that is flattened.
            frontend_args: The vLLM's internal frontend arguments that is 
                flattened.
            engine_config: The vLLM's internal engine config that is nested.
        """
        
        engine_config: VLLMEngineConfig = self.llm_config.get_engine_config()

        if engine_config.use_gpu:
            # Create engine config on a task with access to GPU,
            # as GPU capability may be queried.
            ref = (
                ray.remote(
                    num_cpus=0,
                    num_gpus=1,
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
            vllm_engine_args, vllm_engine_config = _get_vllm_engine_config(self.llm_config)

        vllm_frontend_args = FrontendArgs(**engine_config.frontend_kwargs)
        return vllm_engine_args, vllm_frontend_args, vllm_engine_config


    def _start_async_llm_engine(
        self,
        engine_args: "AsyncEngineArgs",
        vllm_config: "VllmConfig",
        placement_group: PlacementGroup,
    ) -> "EngineClient":
        """Creates an async LLM engine from the engine arguments."""
        from vllm.v1.executor.abstract import Executor
        from vllm.v1.engine.async_llm import AsyncLLM

        vllm_config.parallel_config.placement_group = placement_group
        _clear_current_platform_cache()

        custom_stat_loggers = None
        if self.llm_config.log_engine_metrics:
            from ray.llm._internal.serve.deployments.llm.vllm.vllm_loggers import (
                RayPrometheusStatLogger,
            )

            # V1 AsyncLLM does not yet support add_logger
            # For now, assume folks enabling log_engine_metrics do not require LoggingStatLogger, PrometheusStatLogger
            custom_stat_loggers = [RayPrometheusStatLogger]

        executor_class = Executor.get_class(vllm_config)
        logger.info(f"Using executor class: {executor_class}")
        engine = AsyncLLM(
            vllm_config=vllm_config,
            executor_class=executor_class,
            log_stats=not engine_args.disable_log_stats,
            stat_loggers=custom_stat_loggers,
        )

        return engine

    async def resolve_lora(self, disk_lora_model: DiskMultiplexConfig):
        from vllm.entrypoints.openai.protocol import LoadLoRAAdapterRequest

        if disk_lora_model.model_id in self.oai_models.lora_requests:
            # Lora is already loaded, return
            return
        
        lora_request = await self.oai_models.load_lora_adapter(
            request=LoadLoRAAdapterRequest(
                lora_name=disk_lora_model.model_id,
                lora_path=disk_lora_model.local_path,
            )
        )

        if isinstance(lora_request, VLLMErrorResponse):
            raise ValueError(f"Failed to load lora model: {lora_request.message}")

    async def chat(
        self, request: GenerationRequest
    ) -> AsyncGenerator[str, None]:
        """
        
        input: Take a genric free form input type and cast it to the target engine request type inside the engine.
        
        output: 
        - stream: True --> for each chunk, yield astring representing data: <json_str>\n\n
        - stream: False --> yield only one string representing the response <json_str>

        Error:
        option A:
        when request hits an error, raise an HTTPException(msg, code, type)
        option B:
        yield a HTTPException object
        """


        chat_response = await self._oai_serving_chat.create_chat_completion(request)

        if isinstance(chat_response, AsyncGenerator):
            async for response in chat_response:
                if not isinstance(response, str):
                    raise ValueError(f"Expected create_chat_completion to return a stream of strings, got and item with type {type(response)}")
                yield response
        else:
            logger.info(
                f"[Kourosh] non streaming response received, type: {type(chat_response)}, chat_response: {chat_response}"
            )
            if isinstance(chat_response, VLLMErrorResponse):
                yield ErrorResponse(**chat_response.model_dump())
            yield ChatCompletionResponse(**chat_response.model_dump())


    async def completions(
        self, request
    ):
        raise NotImplementedError("Completions are not supported yet")
    
    async def embeddings(
        self, vllm_embedding_request: VLLMEmbeddingRequest
    ) -> Tuple[List[List[float]], int]:
        """Return (embeddings, num_prompt_tokens)"""

        num_prompts = len(vllm_embedding_request.prompt)
        if RAYLLM_ENABLE_REQUEST_PROMPT_LOGS:
            logger.info(
                f"Encoding request {vllm_embedding_request.request_id} started. "
                f"Num prompts: {num_prompts}"
            )

        generators: List[AsyncGenerator["PoolingRequestOutput", None]] = []

        prompts = vllm_embedding_request.prompt
        if isinstance(prompts, str):
            prompts = [prompts]

        for i, prompt in enumerate(prompts):
            request_id = f"{vllm_embedding_request.request_id}-{i}"
            gen: AsyncGenerator["PoolingRequestOutput", None] = self._engine_client.encode(
                prompt=vllm.inputs.TextPrompt(
                    prompt=prompt,
                ),
                pooling_params=vllm.pooling_params.PoolingParams(),
                request_id=request_id,
                lora_request=vllm_embedding_request.lora_request,  # type: ignore
            )
            generators.append(gen)

        embedding_data = []
        total_prompt_tokens = 0

        for gen in generators:
            async for result in gen:
                embedding = result.outputs.embedding
                if vllm_embedding_request.encoding_format == "base64":
                    embedding = floats_to_base64(embedding)

                embedding_data.append(embedding)
                total_prompt_tokens += len(result.prompt_token_ids)

        return embedding_data, total_prompt_tokens

    async def check_health(self) -> None:
        if not hasattr(self._engine_client, "check_health"):
            raise RuntimeError(f"{type(self._engine_client)} does not support health check.")

        try:
            await self._engine_client.check_health()
        except BaseException as e:
            logger.error("Healthcheck failed. The replica will be restarted")
            raise e from None
